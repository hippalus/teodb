//! Iceberg ↔ TeoDB schema conversions.

use std::sync::Arc;

use iceberg::spec::{PrimitiveType, Type};

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::schema::{ColumnMeta, SchemaDefinition, TeoDataType};

pub fn iceberg_schema_to_teodb(schema: &iceberg::spec::Schema) -> TeoDBResult<SchemaDefinition> {
    let columns = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| nested_field_to_column(f))
        .collect::<TeoDBResult<Vec<_>>>()?;

    Ok(SchemaDefinition {
        schema_id: schema.schema_id(),
        columns,
        identifier_field_ids: schema.identifier_field_ids().collect(),
    })
}

fn nested_field_to_column(f: &iceberg::spec::NestedField) -> TeoDBResult<ColumnMeta> {
    let data_type = match &*f.field_type {
        Type::Primitive(p) => iceberg_primitive_to_teodb(p)?,
        other => {
            return Err(TeoDBError::Catalog(format!(
                "unsupported non-primitive type for field '{}': {other:?}",
                f.name
            )));
        }
    };

    Ok(ColumnMeta {
        id: f.id,
        name: f.name.clone(),
        data_type,
        nullable: !f.required,
        doc: f.doc.clone(),
    })
}

pub fn iceberg_primitive_to_teodb(p: &PrimitiveType) -> TeoDBResult<TeoDataType> {
    Ok(match p {
        PrimitiveType::Boolean => TeoDataType::Boolean,
        PrimitiveType::Int => TeoDataType::Int32,
        PrimitiveType::Long => TeoDataType::Int64,
        PrimitiveType::Float => TeoDataType::Float32,
        PrimitiveType::Double => TeoDataType::Float64,
        PrimitiveType::Date => TeoDataType::Date32,
        PrimitiveType::Time => TeoDataType::Time64Micros,
        PrimitiveType::Timestamp => TeoDataType::TimestampMicros { tz: None },
        PrimitiveType::Timestamptz => TeoDataType::TimestampMicros { tz: Some("UTC".into()) },
        PrimitiveType::String => TeoDataType::Utf8,
        PrimitiveType::Binary => TeoDataType::Binary,
        PrimitiveType::Uuid => TeoDataType::FixedSizeBinary(16),
        PrimitiveType::Fixed(n) => TeoDataType::FixedSizeBinary(*n as i32),
        PrimitiveType::Decimal { precision, scale } => TeoDataType::Decimal128 {
            precision: *precision as u8,
            scale: *scale as i8,
        },
        other => {
            return Err(TeoDBError::InvalidArgument {
                field: "type".into(),
                message: format!("unsupported Iceberg primitive type: {other:?}"),
            });
        }
    })
}

pub fn teodb_to_iceberg_primitive(dt: &TeoDataType) -> TeoDBResult<PrimitiveType> {
    Ok(match dt {
        TeoDataType::Boolean => PrimitiveType::Boolean,
        TeoDataType::Int8 | TeoDataType::Int16 | TeoDataType::Int32 | TeoDataType::UInt8 | TeoDataType::UInt16 => {
            PrimitiveType::Int
        }
        TeoDataType::Int64 | TeoDataType::UInt32 | TeoDataType::UInt64 => PrimitiveType::Long,
        TeoDataType::Float32 => PrimitiveType::Float,
        TeoDataType::Float64 => PrimitiveType::Double,
        TeoDataType::Date32 => PrimitiveType::Date,
        TeoDataType::Time64Micros => PrimitiveType::Time,
        TeoDataType::TimestampMicros { tz: None } => PrimitiveType::Timestamp,
        TeoDataType::TimestampMicros { tz: Some(_) } => PrimitiveType::Timestamptz,
        TeoDataType::Utf8 => PrimitiveType::String,
        TeoDataType::Binary => PrimitiveType::Binary,
        TeoDataType::FixedSizeBinary(16) => PrimitiveType::Uuid,
        TeoDataType::FixedSizeBinary(n) => PrimitiveType::Fixed(*n as u64),
        TeoDataType::Decimal128 { precision, scale } => PrimitiveType::Decimal {
            precision: *precision as u32,
            scale: *scale as u32,
        },
    })
}

/// Convert a TeoDB `SchemaDefinition` to an iceberg `Schema`.
pub fn teodb_schema_to_iceberg(schema: &SchemaDefinition) -> TeoDBResult<iceberg::spec::Schema> {
    let fields: Vec<iceberg::spec::NestedFieldRef> = schema
        .columns
        .iter()
        .map(|col| {
            let prim = teodb_to_iceberg_primitive(&col.data_type)?;
            let field = if col.nullable {
                iceberg::spec::NestedField::optional(col.id, &col.name, Type::Primitive(prim))
            } else {
                iceberg::spec::NestedField::required(col.id, &col.name, Type::Primitive(prim))
            };
            Ok(Arc::new(field))
        })
        .collect::<TeoDBResult<Vec<_>>>()?;

    iceberg::spec::Schema::builder()
        .with_fields(fields)
        .with_schema_id(schema.schema_id)
        .with_identifier_field_ids(schema.identifier_field_ids.iter().copied())
        .build()
        .map_err(|e| TeoDBError::Catalog(format!("failed to build iceberg Schema: {e}")))
}
