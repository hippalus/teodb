//! Iceberg ↔ TeoDB data-file conversions.

use std::collections::HashMap;

use super::TypeLookup;
use super::scalar::{convert_datum_bounds, convert_teo_scalar_bounds};
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::file::{DataContent, DataFile, FileFormat};

pub fn teodb_data_file_to_iceberg(
    df: &DataFile,
    partition_spec: &iceberg::spec::PartitionSpec,
) -> TeoDBResult<iceberg::spec::DataFile> {
    let content = match df.content {
        DataContent::Data => iceberg::spec::DataContentType::Data,
        DataContent::PositionDelete => iceberg::spec::DataContentType::PositionDeletes,
        DataContent::EqualityDelete => iceberg::spec::DataContentType::EqualityDeletes,
    };

    let format = match df.format {
        FileFormat::Parquet => iceberg::spec::DataFileFormat::Parquet,
    };

    let lower_bounds = convert_teo_scalar_bounds(&df.lower_bounds)?;
    let upper_bounds = convert_teo_scalar_bounds(&df.upper_bounds)?;

    let mut builder = iceberg::spec::DataFileBuilder::default();
    builder
        .content(content)
        .file_path(df.path.to_uri())
        .file_format(format)
        .record_count(df.record_count)
        .file_size_in_bytes(df.file_size_bytes)
        .column_sizes(df.column_sizes.clone())
        .value_counts(df.value_counts.clone())
        .null_value_counts(df.null_value_counts.clone())
        .nan_value_counts(df.nan_value_counts.clone())
        .lower_bounds(lower_bounds)
        .upper_bounds(upper_bounds)
        .partition_spec_id(df.partition_spec_id)
        .partition(teodb_partition_values_to_iceberg(&df.partition_values, partition_spec)?);

    if let Some(sort_id) = df.sort_order_id {
        builder.sort_order_id(sort_id);
    }

    if !df.split_offsets.is_empty() {
        builder.split_offsets(Some(df.split_offsets.clone()));
    }

    if !df.equality_ids.is_empty() {
        builder.equality_ids(Some(df.equality_ids.clone()));
    }

    if let Some(ref km) = df.key_metadata {
        builder.key_metadata(Some(km.clone()));
    }

    builder
        .build()
        .map_err(|e| TeoDBError::Catalog(format!("failed to build iceberg DataFile: {e}")))
}

// A faithful Iceberg→TeoDB data-file conversion needs the file plus its schema,
// type lookup, and partition spec/values — grouping them into a struct would
// only obscure a straight-line converter.
#[allow(clippy::too_many_arguments)]
pub fn iceberg_data_file_to_teodb(
    df: &iceberg::spec::DataFile,
    schema_id: i32,
    type_lookup: &TypeLookup,
    partition_spec_id: i32,
    partition_spec: &iceberg::spec::PartitionSpec,
    schema: &iceberg::spec::Schema,
) -> TeoDBResult<DataFile> {
    let content = match df.content_type() {
        iceberg::spec::DataContentType::Data => DataContent::Data,
        iceberg::spec::DataContentType::PositionDeletes => DataContent::PositionDelete,
        iceberg::spec::DataContentType::EqualityDeletes => DataContent::EqualityDelete,
    };

    let format = match df.file_format() {
        iceberg::spec::DataFileFormat::Parquet => FileFormat::Parquet,
        other => {
            return Err(TeoDBError::Catalog(format!("unsupported file format: {other:?}")));
        }
    };

    let path = super::iceberg_location_to_teodb(df.file_path())?;

    let lower_bounds = convert_datum_bounds(df.lower_bounds(), type_lookup)?;
    let upper_bounds = convert_datum_bounds(df.upper_bounds(), type_lookup)?;
    let partition_values = iceberg_partition_values_to_teodb(df.partition(), partition_spec, schema)?;

    Ok(DataFile {
        content,
        path,
        format,
        partition_spec_id,
        sort_order_id: df.sort_order_id(),
        schema_id,
        partition_values,
        record_count: df.record_count(),
        file_size_bytes: df.file_size_in_bytes(),
        column_sizes: df.column_sizes().clone(),
        value_counts: df.value_counts().clone(),
        null_value_counts: df.null_value_counts().clone(),
        nan_value_counts: df.nan_value_counts().clone(),
        lower_bounds,
        upper_bounds,
        split_offsets: df
            .split_offsets()
            .map(|s| s.to_vec())
            .unwrap_or_default(),
        equality_ids: df.equality_ids().unwrap_or_default(),
        key_metadata: df.key_metadata().map(|b| b.to_vec()),
    })
}

fn teodb_partition_values_to_iceberg(
    values: &HashMap<i32, teodb_core::scalar::TeoScalar>,
    partition_spec: &iceberg::spec::PartitionSpec,
) -> TeoDBResult<iceberg::spec::Struct> {
    let fields = partition_spec
        .fields()
        .iter()
        .map(|field| {
            values
                .get(&field.field_id)
                .map(|value| {
                    if value.is_null() {
                        return Ok(None);
                    }
                    let datum = super::scalar::teo_scalar_to_datum(value)?;
                    let primitive: iceberg::spec::PrimitiveLiteral = datum.into();
                    Ok(Some(iceberg::spec::Literal::Primitive(primitive)))
                })
                .transpose()
                .map(|value| value.flatten())
        })
        .collect::<TeoDBResult<Vec<_>>>()?;

    Ok(iceberg::spec::Struct::from_iter(fields))
}

fn iceberg_partition_values_to_teodb(
    values: &iceberg::spec::Struct,
    partition_spec: &iceberg::spec::PartitionSpec,
    schema: &iceberg::spec::Schema,
) -> TeoDBResult<HashMap<i32, teodb_core::scalar::TeoScalar>> {
    if partition_spec.fields().is_empty() {
        return Ok(HashMap::new());
    }

    let partition_type = partition_spec
        .partition_type(schema)
        .map_err(|e| TeoDBError::Catalog(format!("failed to derive Iceberg partition type: {e}")))?;
    let partition_type_fields = partition_type.fields();
    if partition_type_fields.len() != partition_spec.fields().len()
        || values.fields().len() != partition_spec.fields().len()
    {
        return Err(TeoDBError::Catalog(format!(
            "partition tuple shape mismatch: spec fields={}, type fields={}, values={}",
            partition_spec.fields().len(),
            partition_type_fields.len(),
            values.fields().len()
        )));
    }

    let mut converted = HashMap::new();
    for ((spec_field, typed_field), literal) in partition_spec
        .fields()
        .iter()
        .zip(partition_type_fields.iter())
        .zip(values.iter())
    {
        let Some(literal) = literal else {
            converted.insert(spec_field.field_id, teodb_core::scalar::TeoScalar::Null);
            continue;
        };
        let scalar = literal_to_teo_scalar(literal, &typed_field.field_type)?;
        converted.insert(spec_field.field_id, scalar);
    }

    Ok(converted)
}

fn literal_to_teo_scalar(
    literal: &iceberg::spec::Literal,
    field_type: &iceberg::spec::Type,
) -> TeoDBResult<teodb_core::scalar::TeoScalar> {
    let iceberg::spec::Type::Primitive(primitive_type) = field_type else {
        return Err(TeoDBError::Catalog(format!(
            "unsupported non-primitive partition field type: {field_type:?}"
        )));
    };
    let primitive = literal
        .as_primitive_literal()
        .ok_or_else(|| TeoDBError::Catalog(format!("unsupported non-primitive partition literal: {literal:?}")))?;
    let dt = super::schema::iceberg_primitive_to_teodb(primitive_type)?;
    super::scalar::primitive_literal_to_teo_scalar(&primitive, &dt)
}
