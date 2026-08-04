//! Iceberg ↔ TeoDB type conversions.
//!
//! Converts `iceberg::spec` types into TeoDB's internal domain types
//! (`SchemaDefinition`, `DataFile`, `Snapshot`, `TableMetadata`, etc.).
//! These conversions are pure functions with no I/O; manifest entries
//! and data files must be pre-loaded before calling them.

mod data_file;
mod metadata;
pub(crate) mod partition;
mod scalar;
pub(crate) mod schema;
mod sort;

use std::collections::HashMap;

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::FieldId;
use teodb_core::location::ObjectLocation;
use teodb_core::schema::TeoDataType;

#[derive(Debug, Clone)]
pub(crate) struct LoadedIcebergDataFile {
    pub(crate) file: iceberg::spec::DataFile,
    pub(crate) schema_id: i32,
    pub(crate) schema: iceberg::spec::SchemaRef,
    pub(crate) partition_spec_id: i32,
    pub(crate) partition_spec: iceberg::spec::PartitionSpec,
}

/// Lookup table: field_id → TeoDataType, used by data file conversion.
pub type TypeLookup = HashMap<FieldId, TeoDataType>;

/// Normalize an Iceberg location into TeoDB's canonical URI-backed shape.
/// Iceberg's in-memory and local catalogs may return a bare filesystem path.
pub(crate) fn iceberg_location_to_teodb(location: &str) -> TeoDBResult<ObjectLocation> {
    let canonical = if location.contains("://") {
        location.to_owned()
    } else {
        format!("file://{location}")
    };
    ObjectLocation::parse(&canonical).map_err(|error| TeoDBError::Catalog(error.to_string()))
}

// Keep the adapter-facing conversion surface small. Other helpers stay in
// their owning modules and are tested there or through the round-trip tests.
pub use data_file::teodb_data_file_to_iceberg;
pub use metadata::{iceberg_data_files_to_teodb, iceberg_to_teo_metadata};
pub use partition::{
    apply_partition_transform_to_scalar, iceberg_partition_path, teodb_unbound_partition_spec_to_iceberg,
};
pub use schema::teodb_schema_to_iceberg;
pub use sort::teodb_sort_order_to_iceberg;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::spec::{Literal, PrimitiveType, Struct, Transform};
    use teodb_core::file::{DataContent, DataFile, FileFormat};
    use teodb_core::location::{ObjectLocation, StorageScheme};
    use teodb_core::scalar::TeoScalar;
    use teodb_core::schema::{ColumnMeta, SchemaDefinition, TeoDataType};

    use super::data_file::{iceberg_data_file_to_teodb, teodb_data_file_to_iceberg};
    use super::schema::{iceberg_primitive_to_teodb, teodb_schema_to_iceberg};

    #[test]
    fn primitive_roundtrip() {
        let cases = vec![
            TeoDataType::Boolean,
            TeoDataType::Int32,
            TeoDataType::Int64,
            TeoDataType::Float32,
            TeoDataType::Float64,
            TeoDataType::Date32,
            TeoDataType::Time64Micros,
            TeoDataType::TimestampMicros { tz: None },
            TeoDataType::TimestampMicros { tz: Some("UTC".into()) },
            TeoDataType::Utf8,
            TeoDataType::Binary,
            TeoDataType::FixedSizeBinary(16),
            TeoDataType::Decimal128 {
                precision: 10,
                scale: 3,
            },
        ];
        for dt in cases {
            let prim = iceberg_primitive_to_teodb(&match &dt {
                TeoDataType::Boolean => PrimitiveType::Boolean,
                TeoDataType::Int32 => PrimitiveType::Int,
                TeoDataType::Int64 => PrimitiveType::Long,
                TeoDataType::Float32 => PrimitiveType::Float,
                TeoDataType::Float64 => PrimitiveType::Double,
                TeoDataType::Date32 => PrimitiveType::Date,
                TeoDataType::Time64Micros => PrimitiveType::Time,
                TeoDataType::TimestampMicros { tz: None } => PrimitiveType::Timestamp,
                TeoDataType::TimestampMicros { tz: Some(_) } => PrimitiveType::Timestamptz,
                TeoDataType::Utf8 => PrimitiveType::String,
                TeoDataType::Binary => PrimitiveType::Binary,
                TeoDataType::FixedSizeBinary(16) => PrimitiveType::Uuid,
                TeoDataType::Decimal128 { precision, scale } => PrimitiveType::Decimal {
                    precision: *precision as u32,
                    scale: *scale as u32,
                },
                _ => unreachable!(),
            })
            .unwrap();
            assert_eq!(prim, dt, "failed for {dt:?}");
        }
    }

    #[test]
    fn data_file_partition_values_roundtrip() {
        let schema = SchemaDefinition {
            schema_id: 7,
            columns: vec![ColumnMeta {
                id: 1,
                name: "region".into(),
                data_type: TeoDataType::Utf8,
                nullable: false,
                doc: None,
            }],
            identifier_field_ids: vec![],
        };
        let iceberg_schema = teodb_schema_to_iceberg(&schema).unwrap();
        let iceberg_spec = iceberg::spec::PartitionSpec::builder(Arc::new(iceberg_schema.clone()))
            .with_spec_id(3)
            .add_partition_field("region", "region", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let mut partition_values = HashMap::new();
        partition_values.insert(iceberg_spec.fields()[0].field_id, TeoScalar::Utf8("eu".into()));
        let data_file = DataFile {
            content: DataContent::Data,
            path: ObjectLocation {
                scheme: StorageScheme::Local,
                bucket: None,
                key: "warehouse/events/data/a.parquet".into(),
            },
            format: FileFormat::Parquet,
            partition_spec_id: 3,
            sort_order_id: Some(0),
            schema_id: 7,
            partition_values: partition_values.clone(),
            record_count: 10,
            file_size_bytes: 100,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            split_offsets: vec![],
            equality_ids: vec![],
            key_metadata: None,
        };

        let iceberg_file = teodb_data_file_to_iceberg(&data_file, &iceberg_spec).unwrap();
        assert_eq!(
            iceberg_file.partition(),
            &Struct::from_iter([Some(Literal::string("eu"))])
        );

        let mut lookup = HashMap::new();
        lookup.insert(1, TeoDataType::Utf8);
        let converted =
            iceberg_data_file_to_teodb(&iceberg_file, 7, &lookup, 3, &iceberg_spec, &iceberg_schema).unwrap();

        assert_eq!(converted.partition_spec_id, 3);
        assert_eq!(converted.partition_values, partition_values);
    }
}
