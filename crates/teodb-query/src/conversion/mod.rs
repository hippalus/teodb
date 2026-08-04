//! Conversions between TeoDB domain values and Arrow/DataFusion values.

mod data_type;
mod scalar;
mod schema;

pub use data_type::teo_to_arrow_type;
pub use scalar::{scalar_value_to_teo_scalar, teo_scalar_to_scalar_value};
pub use schema::{column_meta_to_arrow_field, field_id_from_arrow_field, schema_to_arrow};

#[cfg(test)]
mod tests;
