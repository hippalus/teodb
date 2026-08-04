//! Iceberg partition conversion and path helpers.

use std::collections::HashMap;
use std::fmt::Write;

use iceberg::spec::Transform;

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::FieldId;
use teodb_core::scalar::TeoScalar;
use teodb_core::schema::{
    PartitionField, PartitionSpec, PartitionTransform, SchemaDefinition, TeoDataType, UnboundPartitionSpec,
};

pub fn iceberg_partition_spec_to_teodb(spec: &iceberg::spec::PartitionSpec) -> TeoDBResult<PartitionSpec> {
    let fields = spec
        .fields()
        .iter()
        .map(|f| {
            Ok(PartitionField {
                source_id: f.source_id,
                field_id: f.field_id,
                name: f.name.clone(),
                transform: iceberg_transform_to_teodb(&f.transform)?,
            })
        })
        .collect::<TeoDBResult<Vec<_>>>()?;

    Ok(PartitionSpec {
        spec_id: spec.spec_id(),
        fields,
    })
}

pub(crate) fn iceberg_transform_to_teodb(t: &Transform) -> TeoDBResult<PartitionTransform> {
    match t {
        Transform::Identity => Ok(PartitionTransform::Identity),
        Transform::Year => Ok(PartitionTransform::Year),
        Transform::Month => Ok(PartitionTransform::Month),
        Transform::Day => Ok(PartitionTransform::Day),
        Transform::Hour => Ok(PartitionTransform::Hour),
        Transform::Void => Ok(PartitionTransform::Void),
        Transform::Bucket(n) => Ok(PartitionTransform::Bucket { num_buckets: *n }),
        Transform::Truncate(w) => Ok(PartitionTransform::Truncate { width: *w }),
        other => Err(TeoDBError::InvalidArgument {
            field: "transform".into(),
            message: format!("unsupported partition transform: {other:?}"),
        }),
    }
}

pub fn teodb_partition_transform_to_iceberg(t: &PartitionTransform) -> Transform {
    match t {
        PartitionTransform::Identity => Transform::Identity,
        PartitionTransform::Year => Transform::Year,
        PartitionTransform::Month => Transform::Month,
        PartitionTransform::Day => Transform::Day,
        PartitionTransform::Hour => Transform::Hour,
        PartitionTransform::Void => Transform::Void,
        PartitionTransform::Bucket { num_buckets } => Transform::Bucket(*num_buckets),
        PartitionTransform::Truncate { width } => Transform::Truncate(*width),
    }
}

pub fn teodb_unbound_partition_spec_to_iceberg(
    spec: &UnboundPartitionSpec,
) -> TeoDBResult<iceberg::spec::UnboundPartitionSpec> {
    let fields = spec
        .fields
        .iter()
        .map(|field| iceberg::spec::UnboundPartitionField {
            source_id: field.source_id,
            field_id: field.field_id,
            name: field.name.clone(),
            transform: teodb_partition_transform_to_iceberg(&field.transform),
        });
    let mut builder = iceberg::spec::UnboundPartitionSpec::builder();
    if let Some(spec_id) = spec.spec_id {
        builder = builder.with_spec_id(spec_id);
    }
    builder
        .add_partition_fields(fields)
        .map(iceberg::spec::UnboundPartitionSpecBuilder::build)
        .map_err(|error| TeoDBError::InvalidArgument {
            field: "partition_spec".into(),
            message: error.to_string(),
        })
}

pub fn apply_partition_transform_to_scalar(
    value: &TeoScalar,
    data_type: &TeoDataType,
    transform: &PartitionTransform,
) -> TeoDBResult<TeoScalar> {
    if value.is_null() {
        return Ok(TeoScalar::Null);
    }

    let iceberg_transform = teodb_partition_transform_to_iceberg(transform);
    let source_type = iceberg::spec::Type::Primitive(super::schema::teodb_to_iceberg_primitive(data_type)?);
    let result_type = iceberg_transform
        .result_type(&source_type)
        .map_err(|e| TeoDBError::Catalog(format!("invalid partition transform: {e}")))?;
    let iceberg::spec::Type::Primitive(result_primitive) = result_type else {
        return Err(TeoDBError::Catalog(format!(
            "partition transform returned non-primitive type: {result_type:?}"
        )));
    };
    let result_type = super::schema::iceberg_primitive_to_teodb(&result_primitive)?;
    let datum = super::scalar::teo_scalar_to_datum(value)?;
    let function = iceberg::transform::create_transform_function(&iceberg_transform)
        .map_err(|e| TeoDBError::Catalog(format!("failed to create partition transform: {e}")))?;

    match function
        .transform_literal(&datum)
        .map_err(|e| TeoDBError::Catalog(format!("failed to apply partition transform: {e}")))?
    {
        Some(transformed) => super::scalar::datum_to_teo_scalar(&transformed, &result_type),
        None => Ok(TeoScalar::Null),
    }
}

/// Build the canonical Iceberg partition directory for already-transformed
/// partition values.
///
/// Apache Iceberg's Java implementation applies `URLEncoder` independently to
/// every field name and human-readable value. Keeping that exact encoding
/// makes paths portable across Iceberg engines and prevents `/`, `\`, `%`, or
/// absolute/path-traversal input from creating extra object-key segments.
pub fn iceberg_partition_path(
    schema: &SchemaDefinition,
    spec: &PartitionSpec,
    values: &HashMap<FieldId, TeoScalar>,
) -> TeoDBResult<String> {
    let mut components = Vec::with_capacity(spec.fields.len());
    for field in &spec.fields {
        let source = schema
            .columns
            .iter()
            .find(|column| column.id == field.source_id)
            .ok_or_else(|| TeoDBError::InvalidArgument {
                field: "partition_spec".into(),
                message: format!(
                    "partition field '{}' references missing source field {}",
                    field.name, field.source_id
                ),
            })?;
        let value = values
            .get(&field.field_id)
            .ok_or_else(|| TeoDBError::InvalidArgument {
                field: "partition_values".into(),
                message: format!(
                    "partition value for field '{}' ({}) is missing",
                    field.name, field.field_id
                ),
            })?;

        let transform = teodb_partition_transform_to_iceberg(&field.transform);
        let source_type = iceberg::spec::Type::Primitive(super::schema::teodb_to_iceberg_primitive(&source.data_type)?);
        let result_type = transform
            .result_type(&source_type)
            .map_err(|error| TeoDBError::Catalog(format!("invalid partition transform: {error}")))?;
        let literal = match value {
            TeoScalar::Null => None,
            value => Some(iceberg::spec::Literal::Primitive(
                super::scalar::teo_scalar_to_datum(value)?
                    .literal()
                    .clone(),
            )),
        };
        let human = transform.to_human_string(&result_type, literal.as_ref());
        components.push(format!(
            "{}={}",
            iceberg_url_encode(&field.name),
            iceberg_url_encode(&human)
        ));
    }

    Ok(components.join("/"))
}

/// Match `java.net.URLEncoder.encode(value, UTF_8)`, which is the canonical
/// encoding used by Apache Iceberg's `PartitionSpec.partitionToPath`.
fn iceberg_url_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match *byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' => {
                encoded.push(char::from(*byte));
            }
            b' ' => encoded.push('+'),
            byte => {
                write!(&mut encoded, "%{byte:02X}").expect("writing to String cannot fail");
            }
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    use teodb_core::schema::{ColumnMeta, PartitionField, PartitionTransform};

    #[test]
    fn partition_path_matches_iceberg_url_encoding_and_cannot_add_segments() {
        let schema = SchemaDefinition {
            schema_id: 0,
            columns: vec![ColumnMeta {
                id: 1,
                name: "source".into(),
                data_type: TeoDataType::Utf8,
                nullable: false,
                doc: None,
            }],
            identifier_field_ids: vec![],
        };
        let spec = PartitionSpec {
            spec_id: 0,
            fields: vec![PartitionField {
                source_id: 1,
                field_id: 1000,
                name: "region/name".into(),
                transform: PartitionTransform::Identity,
            }],
        };
        let values = HashMap::from([(1000, TeoScalar::Utf8("../eu west%=\\absolute".into()))]);

        let path = iceberg_partition_path(&schema, &spec, &values).unwrap();

        assert_eq!(path, "region%2Fname=..%2Feu+west%25%3D%5Cabsolute");
        assert_eq!(path.split('/').count(), 1);
    }

    #[test]
    fn partition_path_uses_iceberg_human_temporal_values() {
        let schema = SchemaDefinition {
            schema_id: 0,
            columns: vec![ColumnMeta {
                id: 1,
                name: "event_ts".into(),
                data_type: TeoDataType::TimestampMicros { tz: None },
                nullable: false,
                doc: None,
            }],
            identifier_field_ids: vec![],
        };
        let spec = PartitionSpec {
            spec_id: 0,
            fields: vec![PartitionField {
                source_id: 1,
                field_id: 1000,
                name: "event_day".into(),
                transform: PartitionTransform::Day,
            }],
        };
        let values = HashMap::from([(1000, TeoScalar::Int32(0))]);

        assert_eq!(
            iceberg_partition_path(&schema, &spec, &values).unwrap(),
            "event_day=1970-01-01"
        );
    }
}
