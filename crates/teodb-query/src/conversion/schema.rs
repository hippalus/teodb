use std::sync::Arc;

use arrow::datatypes::Field;
use teodb_core::ident::FieldId;
use teodb_core::schema::{ColumnMeta, SchemaDefinition};

use super::teo_to_arrow_type;

/// Build an Arrow field from a TeoDB column definition.
pub fn column_meta_to_arrow_field(column: &ColumnMeta) -> Field {
    let mut metadata = std::collections::HashMap::with_capacity(1);
    metadata.insert("PARQUET:field_id".to_owned(), column.id.to_string());
    Field::new(&column.name, teo_to_arrow_type(&column.data_type), column.nullable).with_metadata(metadata)
}

/// Build an Arrow schema from a TeoDB schema definition.
pub fn schema_to_arrow(schema: &SchemaDefinition) -> arrow::datatypes::SchemaRef {
    let fields: Vec<Field> = schema
        .columns
        .iter()
        .map(column_meta_to_arrow_field)
        .collect();
    Arc::new(arrow::datatypes::Schema::new(fields))
}

/// Extract the Parquet field ID from an Arrow field.
pub fn field_id_from_arrow_field(field: &Field) -> Option<FieldId> {
    field
        .metadata()
        .get("PARQUET:field_id")
        .and_then(|value| value.parse::<FieldId>().ok())
}
