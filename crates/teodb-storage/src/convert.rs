//! Bidirectional conversions between TeoDB domain types and Arrow/Parquet types.
//!
//! These conversions are the **only** place Arrow types enter TeoDB's domain model.
//! `teodb-core` defines `TeoDataType` and `TeoScalar` without any Arrow dependency;
//! this module bridges the gap.

use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit};
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::FieldId;
use teodb_core::scalar::TeoScalar;
use teodb_core::schema::{ColumnMeta, SchemaDefinition, TeoDataType};

/// Convert a `TeoDataType` to its Arrow equivalent.
pub fn teo_data_type_to_arrow(dt: &TeoDataType) -> ArrowDataType {
    match dt {
        TeoDataType::Boolean => ArrowDataType::Boolean,
        TeoDataType::Int8 => ArrowDataType::Int8,
        TeoDataType::Int16 => ArrowDataType::Int16,
        TeoDataType::Int32 => ArrowDataType::Int32,
        TeoDataType::Int64 => ArrowDataType::Int64,
        TeoDataType::UInt8 => ArrowDataType::UInt8,
        TeoDataType::UInt16 => ArrowDataType::UInt16,
        TeoDataType::UInt32 => ArrowDataType::UInt32,
        TeoDataType::UInt64 => ArrowDataType::UInt64,
        TeoDataType::Float32 => ArrowDataType::Float32,
        TeoDataType::Float64 => ArrowDataType::Float64,
        TeoDataType::Decimal128 { precision, scale } => ArrowDataType::Decimal128(*precision, *scale),
        TeoDataType::Date32 => ArrowDataType::Date32,
        TeoDataType::TimestampMicros { tz } => {
            ArrowDataType::Timestamp(TimeUnit::Microsecond, tz.as_ref().map(|s| Arc::from(s.as_str())))
        }
        TeoDataType::Time64Micros => ArrowDataType::Time64(TimeUnit::Microsecond),
        TeoDataType::Utf8 => ArrowDataType::Utf8,
        TeoDataType::Binary => ArrowDataType::Binary,
        TeoDataType::FixedSizeBinary(n) => ArrowDataType::FixedSizeBinary(*n),
    }
}

/// Convert an Arrow `DataType` back to `TeoDataType`.
///
/// Returns an error for Arrow types TeoDB does not support (e.g., `List`,
/// `Struct`, `Duration`, non-microsecond timestamps).
pub fn arrow_to_teo_data_type(dt: &ArrowDataType) -> TeoDBResult<TeoDataType> {
    match dt {
        ArrowDataType::Boolean => Ok(TeoDataType::Boolean),
        ArrowDataType::Int8 => Ok(TeoDataType::Int8),
        ArrowDataType::Int16 => Ok(TeoDataType::Int16),
        ArrowDataType::Int32 => Ok(TeoDataType::Int32),
        ArrowDataType::Int64 => Ok(TeoDataType::Int64),
        ArrowDataType::UInt8 => Ok(TeoDataType::UInt8),
        ArrowDataType::UInt16 => Ok(TeoDataType::UInt16),
        ArrowDataType::UInt32 => Ok(TeoDataType::UInt32),
        ArrowDataType::UInt64 => Ok(TeoDataType::UInt64),
        ArrowDataType::Float32 => Ok(TeoDataType::Float32),
        ArrowDataType::Float64 => Ok(TeoDataType::Float64),
        ArrowDataType::Decimal128(precision, scale) => Ok(TeoDataType::Decimal128 {
            precision: *precision,
            scale: *scale,
        }),
        ArrowDataType::Date32 => Ok(TeoDataType::Date32),
        ArrowDataType::Timestamp(TimeUnit::Microsecond, tz) => Ok(TeoDataType::TimestampMicros {
            tz: tz.as_ref().map(|s| s.to_string()),
        }),
        ArrowDataType::Time64(TimeUnit::Microsecond) => Ok(TeoDataType::Time64Micros),
        ArrowDataType::Utf8 => Ok(TeoDataType::Utf8),
        ArrowDataType::Binary => Ok(TeoDataType::Binary),
        ArrowDataType::FixedSizeBinary(n) => Ok(TeoDataType::FixedSizeBinary(*n)),
        other => Err(TeoDBError::InvalidArgument {
            field: "data_type".into(),
            message: format!("unsupported Arrow type: {other:?}"),
        }),
    }
}

/// Build an Arrow `Field` from a `ColumnMeta`, embedding the stable field ID
/// in the field's metadata under the key `PARQUET:field_id`.
pub fn column_meta_to_arrow_field(col: &ColumnMeta) -> Field {
    let mut metadata = std::collections::HashMap::with_capacity(1);
    metadata.insert("PARQUET:field_id".to_owned(), col.id.to_string());
    Field::new(&col.name, teo_data_type_to_arrow(&col.data_type), col.nullable).with_metadata(metadata)
}

/// Build a full Arrow `Schema` from a `SchemaDefinition`.
pub fn schema_to_arrow(schema: &SchemaDefinition) -> arrow::datatypes::Schema {
    let fields: Vec<Field> = schema
        .columns
        .iter()
        .map(column_meta_to_arrow_field)
        .collect();
    arrow::datatypes::Schema::new(fields)
}

/// Extract the `PARQUET:field_id` from an Arrow `Field`'s metadata.
pub fn field_id_from_arrow_field(field: &Field) -> Option<FieldId> {
    field
        .metadata()
        .get("PARQUET:field_id")
        .and_then(|v| v.parse::<FieldId>().ok())
}

/// Convert a `TeoScalar` to the equivalent Arrow scalar array element.
/// Returns `(DataType, Option<ScalarBytes>)` — `None` for `TeoScalar::Null`.
///
/// This is primarily used by the Parquet writer for partition value encoding
/// and by the stats extraction module.
pub fn teo_scalar_to_arrow_scalar(scalar: &TeoScalar) -> TeoDBResult<ArrowScalar> {
    match scalar {
        TeoScalar::Null => Ok(ArrowScalar::Null),
        TeoScalar::Boolean(v) => Ok(ArrowScalar::Boolean(*v)),
        TeoScalar::Int8(v) => Ok(ArrowScalar::Int8(*v)),
        TeoScalar::Int16(v) => Ok(ArrowScalar::Int16(*v)),
        TeoScalar::Int32(v) => Ok(ArrowScalar::Int32(*v)),
        TeoScalar::Int64(v) => Ok(ArrowScalar::Int64(*v)),
        TeoScalar::UInt8(v) => Ok(ArrowScalar::UInt8(*v)),
        TeoScalar::UInt16(v) => Ok(ArrowScalar::UInt16(*v)),
        TeoScalar::UInt32(v) => Ok(ArrowScalar::UInt32(*v)),
        TeoScalar::UInt64(v) => Ok(ArrowScalar::UInt64(*v)),
        TeoScalar::Float32(v) => Ok(ArrowScalar::Float32(*v)),
        TeoScalar::Float64(v) => Ok(ArrowScalar::Float64(*v)),
        TeoScalar::Decimal128 {
            value,
            precision,
            scale,
        } => Ok(ArrowScalar::Decimal128 {
            value: *value,
            precision: *precision,
            scale: *scale,
        }),
        TeoScalar::Date32(v) => Ok(ArrowScalar::Date32(*v)),
        TeoScalar::TimestampMicros { value, tz } => Ok(ArrowScalar::TimestampMicros {
            value: *value,
            tz: tz.clone(),
        }),
        TeoScalar::Time64Micros(v) => Ok(ArrowScalar::Time64Micros(*v)),
        TeoScalar::Utf8(v) => Ok(ArrowScalar::Utf8(v.clone())),
        TeoScalar::Binary(v) => Ok(ArrowScalar::Binary(v.clone())),
        TeoScalar::FixedSizeBinary(v) => Ok(ArrowScalar::FixedSizeBinary(v.clone())),
    }
}

/// Intermediate representation for Arrow scalar values, used during
/// Parquet statistics extraction.
#[derive(Debug, Clone, PartialEq)]
pub enum ArrowScalar {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Decimal128 { value: i128, precision: u8, scale: i8 },
    Date32(i32),
    TimestampMicros { value: i64, tz: Option<String> },
    Time64Micros(i64),
    Utf8(String),
    Binary(Vec<u8>),
    FixedSizeBinary(Vec<u8>),
}

/// Convert an `ArrowScalar` back to `TeoScalar`.
pub fn arrow_to_teo_scalar(scalar: &ArrowScalar) -> TeoScalar {
    match scalar {
        ArrowScalar::Null => TeoScalar::Null,
        ArrowScalar::Boolean(v) => TeoScalar::Boolean(*v),
        ArrowScalar::Int8(v) => TeoScalar::Int8(*v),
        ArrowScalar::Int16(v) => TeoScalar::Int16(*v),
        ArrowScalar::Int32(v) => TeoScalar::Int32(*v),
        ArrowScalar::Int64(v) => TeoScalar::Int64(*v),
        ArrowScalar::UInt8(v) => TeoScalar::UInt8(*v),
        ArrowScalar::UInt16(v) => TeoScalar::UInt16(*v),
        ArrowScalar::UInt32(v) => TeoScalar::UInt32(*v),
        ArrowScalar::UInt64(v) => TeoScalar::UInt64(*v),
        ArrowScalar::Float32(v) => TeoScalar::Float32(*v),
        ArrowScalar::Float64(v) => TeoScalar::Float64(*v),
        ArrowScalar::Decimal128 {
            value,
            precision,
            scale,
        } => TeoScalar::Decimal128 {
            value: *value,
            precision: *precision,
            scale: *scale,
        },
        ArrowScalar::Date32(v) => TeoScalar::Date32(*v),
        ArrowScalar::TimestampMicros { value, tz } => TeoScalar::TimestampMicros {
            value: *value,
            tz: tz.clone(),
        },
        ArrowScalar::Time64Micros(v) => TeoScalar::Time64Micros(*v),
        ArrowScalar::Utf8(v) => TeoScalar::Utf8(v.clone()),
        ArrowScalar::Binary(v) => TeoScalar::Binary(v.clone()),
        ArrowScalar::FixedSizeBinary(v) => TeoScalar::FixedSizeBinary(v.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_all_data_types() {
        let types = vec![
            TeoDataType::Boolean,
            TeoDataType::Int8,
            TeoDataType::Int16,
            TeoDataType::Int32,
            TeoDataType::Int64,
            TeoDataType::UInt8,
            TeoDataType::UInt16,
            TeoDataType::UInt32,
            TeoDataType::UInt64,
            TeoDataType::Float32,
            TeoDataType::Float64,
            TeoDataType::Decimal128 {
                precision: 18,
                scale: 2,
            },
            TeoDataType::Date32,
            TeoDataType::TimestampMicros { tz: None },
            TeoDataType::TimestampMicros { tz: Some("UTC".into()) },
            TeoDataType::Time64Micros,
            TeoDataType::Utf8,
            TeoDataType::Binary,
            TeoDataType::FixedSizeBinary(16),
        ];

        for dt in &types {
            let arrow = teo_data_type_to_arrow(dt);
            let back = arrow_to_teo_data_type(&arrow).unwrap();
            assert_eq!(dt, &back, "roundtrip failed for {dt:?}");
        }
    }

    #[test]
    fn unsupported_arrow_type_returns_error() {
        let list_type = ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int32, true)));
        let result = arrow_to_teo_data_type(&list_type);
        assert!(result.is_err());
    }

    #[test]
    fn column_meta_to_field_preserves_field_id() {
        let col = ColumnMeta {
            id: 42,
            name: "amount".into(),
            data_type: TeoDataType::Decimal128 {
                precision: 18,
                scale: 2,
            },
            nullable: false,
            doc: None,
        };
        let field = column_meta_to_arrow_field(&col);
        assert_eq!(field.name(), "amount");
        assert!(!field.is_nullable());
        assert_eq!(field_id_from_arrow_field(&field), Some(42));
    }

    #[test]
    fn schema_to_arrow_preserves_all_columns() {
        let schema = SchemaDefinition {
            schema_id: 0,
            columns: vec![
                ColumnMeta {
                    id: 1,
                    name: "id".into(),
                    data_type: TeoDataType::Int64,
                    nullable: false,
                    doc: None,
                },
                ColumnMeta {
                    id: 2,
                    name: "name".into(),
                    data_type: TeoDataType::Utf8,
                    nullable: true,
                    doc: None,
                },
            ],
            identifier_field_ids: vec![1],
        };
        let arrow = schema_to_arrow(&schema);
        assert_eq!(arrow.fields().len(), 2);
        assert_eq!(arrow.field(0).name(), "id");
        assert_eq!(arrow.field(1).name(), "name");
    }

    #[test]
    fn scalar_roundtrip() {
        let scalars = vec![
            TeoScalar::Null,
            TeoScalar::Boolean(true),
            TeoScalar::Int64(42),
            TeoScalar::Float64(1.23),
            TeoScalar::Utf8("hello".into()),
            TeoScalar::Binary(vec![0xDE, 0xAD]),
            TeoScalar::Decimal128 {
                value: 12345,
                precision: 18,
                scale: 2,
            },
            TeoScalar::TimestampMicros {
                value: 1_000_000,
                tz: Some("UTC".into()),
            },
        ];

        for s in &scalars {
            let arrow = teo_scalar_to_arrow_scalar(s).unwrap();
            let back = arrow_to_teo_scalar(&arrow);
            assert_eq!(s, &back, "scalar roundtrip failed for {s:?}");
        }
    }
}
