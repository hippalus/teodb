//! Table-creation service.
//!
//! Parses the REST/Flight table-definition vocabulary (column type keywords,
//! partition transforms) into an Iceberg schema + partition spec, commits the
//! table through the catalog, and clears any stale buffer/idempotency state
//! left by a previous incarnation of the same name. The handler keeps only the
//! DTO→domain mapping and response shaping.

use std::collections::HashMap;

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::TableIdent;
use teodb_core::location::{ObjectLocation, StorageScheme};
use teodb_core::schema::{SchemaDefinition, TeoDataType, UnboundPartitionSpec};
use teodb_core::table::{CreateTableRequestBuilder, PartitionFieldSpec, PartitionTransformSpec};

use crate::service::DdlService;

/// Parse a column data-type keyword (`int64`, `string`, `timestamp`, …) into a
/// `TeoDataType`.
pub fn data_type_from_keyword(s: &str) -> TeoDBResult<TeoDataType> {
    match s.to_lowercase().as_str() {
        "boolean" | "bool" => Ok(TeoDataType::Boolean),
        "int8" | "tinyint" => Ok(TeoDataType::Int8),
        "int16" | "smallint" => Ok(TeoDataType::Int16),
        "int32" | "int" | "integer" => Ok(TeoDataType::Int32),
        "int64" | "bigint" | "long" => Ok(TeoDataType::Int64),
        "uint8" => Ok(TeoDataType::UInt8),
        "uint16" => Ok(TeoDataType::UInt16),
        "uint32" => Ok(TeoDataType::UInt32),
        "uint64" => Ok(TeoDataType::UInt64),
        "float32" | "float" => Ok(TeoDataType::Float32),
        "float64" | "double" => Ok(TeoDataType::Float64),
        "date" | "date32" => Ok(TeoDataType::Date32),
        "timestamp" => Ok(TeoDataType::TimestampMicros { tz: Some("UTC".into()) }),
        "time" => Ok(TeoDataType::Time64Micros),
        "string" | "utf8" | "text" | "varchar" => Ok(TeoDataType::Utf8),
        "binary" | "bytes" => Ok(TeoDataType::Binary),
        other => Err(TeoDBError::InvalidArgument {
            field: "data_type".into(),
            message: format!("unsupported data type: '{other}'"),
        }),
    }
}

pub fn partition_field_specs(fields: &[(String, String)]) -> TeoDBResult<Vec<PartitionFieldSpec>> {
    fields
        .iter()
        .map(|(column, transform)| parse_transform(transform).map(|parsed| PartitionFieldSpec::new(column, parsed)))
        .collect()
}

fn parse_transform(s: &str) -> TeoDBResult<PartitionTransformSpec> {
    let s = s.trim().to_lowercase();

    if let Some(inner) = s
        .strip_prefix("bucket(")
        .and_then(|r| r.strip_suffix(')'))
    {
        let n: u32 = inner
            .trim()
            .parse()
            .map_err(|_| TeoDBError::InvalidArgument {
                field: "transform".into(),
                message: format!("invalid bucket count: '{inner}'"),
            })?;
        return Ok(PartitionTransformSpec::Bucket(n));
    }

    if let Some(inner) = s
        .strip_prefix("truncate(")
        .and_then(|r| r.strip_suffix(')'))
    {
        let w: u32 = inner
            .trim()
            .parse()
            .map_err(|_| TeoDBError::InvalidArgument {
                field: "transform".into(),
                message: format!("invalid truncate width: '{inner}'"),
            })?;
        return Ok(PartitionTransformSpec::Truncate(w));
    }

    match s.as_str() {
        "identity" => Ok(PartitionTransformSpec::Identity),
        "year" => Ok(PartitionTransformSpec::Year),
        "month" => Ok(PartitionTransformSpec::Month),
        "day" => Ok(PartitionTransformSpec::Day),
        "hour" => Ok(PartitionTransformSpec::Hour),
        other => Err(TeoDBError::InvalidArgument {
            field: "transform".into(),
            message: format!(
                "unsupported partition transform: '{other}'. \
                 Supported: identity, year, month, day, hour, bucket(N), truncate(W)"
            ),
        }),
    }
}

impl DdlService {
    /// Create a table and discard any stale buffer/idempotency receipts left by a
    /// previous incarnation of the same name.
    pub async fn create_table(
        &self,
        ident: TableIdent,
        schema: SchemaDefinition,
        partition_spec: UnboundPartitionSpec,
        properties: HashMap<String, String>,
    ) -> TeoDBResult<()> {
        let location = ObjectLocation::parse(&format!(
            "{}/{}/{}",
            self.default_warehouse_uri, ident.namespace, ident.name
        ))
        .unwrap_or_else(|_| ObjectLocation {
            scheme: StorageScheme::S3,
            bucket: Some("warehouse".into()),
            key: format!("{}/{}", ident.namespace, ident.name),
        });

        let req = CreateTableRequestBuilder::new(ident.clone(), schema, location)
            .partition_spec(partition_spec)
            .properties(properties)
            .build()?;

        self.catalog.create_table(req).await?;

        // Discard any stale buffer and idempotency receipts from a previous
        // incarnation of this table.
        self.buffers.remove(&ident);
        self.idempotency.evict_table(&ident);
        Ok(())
    }
}
