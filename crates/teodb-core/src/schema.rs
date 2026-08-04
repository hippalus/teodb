use serde::{Deserialize, Serialize};

use crate::ident::FieldId;

/// TeoDB's own data type enum. Keeps `teodb-core` free of Arrow and
/// DataFusion dependencies. Conversion to `arrow_schema::DataType` lives
/// in downstream crates (`teodb-storage`, `teodb-query`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TeoDataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    /// Fixed-point decimal with precision and scale.
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    Date32,
    TimestampMicros {
        tz: Option<String>,
    },
    Time64Micros,
    Utf8,
    Binary,
    FixedSizeBinary(i32),
}

impl std::fmt::Display for TeoDataType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Boolean => formatter.write_str("boolean"),
            Self::Int8 => formatter.write_str("int8"),
            Self::Int16 => formatter.write_str("int16"),
            Self::Int32 => formatter.write_str("int32"),
            Self::Int64 => formatter.write_str("int64"),
            Self::UInt8 => formatter.write_str("uint8"),
            Self::UInt16 => formatter.write_str("uint16"),
            Self::UInt32 => formatter.write_str("uint32"),
            Self::UInt64 => formatter.write_str("uint64"),
            Self::Float32 => formatter.write_str("float32"),
            Self::Float64 => formatter.write_str("float64"),
            Self::Decimal128 { precision, scale } => write!(formatter, "decimal128({precision},{scale})"),
            Self::Date32 => formatter.write_str("date32"),
            Self::TimestampMicros { tz: None } => formatter.write_str("timestamp_micros"),
            Self::TimestampMicros { tz: Some(tz) } => write!(formatter, "timestamp_micros({tz})"),
            Self::Time64Micros => formatter.write_str("time64_micros"),
            Self::Utf8 => formatter.write_str("utf8"),
            Self::Binary => formatter.write_str("binary"),
            Self::FixedSizeBinary(size) => write!(formatter, "fixed_size_binary({size})"),
        }
    }
}

/// One column of a table schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub id: FieldId,
    pub name: String,
    pub data_type: TeoDataType,
    pub nullable: bool,
    pub doc: Option<String>,
}

/// A versioned schema. The `schema_id` is referenced by snapshots and data
/// files so that schema evolution does not require rewriting historical data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaDefinition {
    pub schema_id: i32,
    pub columns: Vec<ColumnMeta>,
    /// Field IDs used as table-level identifiers; reserved for upserts/deletes.
    pub identifier_field_ids: Vec<FieldId>,
}

impl SchemaDefinition {
    pub fn by_id(&self, id: FieldId) -> Option<&ColumnMeta> {
        self.columns.iter().find(|c| c.id == id)
    }

    pub fn by_name(&self, name: &str) -> Option<&ColumnMeta> {
        self.columns.iter().find(|c| c.name == name)
    }
}

/// Iceberg partition transform.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket { num_buckets: u32 },
    Truncate { width: u32 },
    Void,
}

impl std::fmt::Display for PartitionTransform {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Identity => formatter.write_str("identity"),
            Self::Year => formatter.write_str("year"),
            Self::Month => formatter.write_str("month"),
            Self::Day => formatter.write_str("day"),
            Self::Hour => formatter.write_str("hour"),
            Self::Bucket { num_buckets } => write!(formatter, "bucket[{num_buckets}]"),
            Self::Truncate { width } => write!(formatter, "truncate[{width}]"),
            Self::Void => formatter.write_str("void"),
        }
    }
}

/// A single partition field referencing a source column by stable `FieldId`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionField {
    /// References a column in the table schema by stable id.
    pub source_id: FieldId,
    /// The partition field's own stable id (used in manifest metadata).
    pub field_id: FieldId,
    /// Display name; never used for correctness.
    pub name: String,
    pub transform: PartitionTransform,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionSpec {
    pub spec_id: i32,
    pub fields: Vec<PartitionField>,
}

/// A partition field that has not yet been bound by the catalog.
///
/// Iceberg assigns missing partition field IDs when a table is created. Core
/// keeps that transport detail in a domain type so callers do not depend on
/// the Iceberg crate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnboundPartitionField {
    pub source_id: FieldId,
    pub field_id: Option<FieldId>,
    pub name: String,
    pub transform: PartitionTransform,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnboundPartitionSpec {
    pub spec_id: Option<i32>,
    pub fields: Vec<UnboundPartitionField>,
}

impl UnboundPartitionSpec {
    pub fn unpartitioned() -> Self {
        Self {
            spec_id: Some(0),
            fields: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NullOrder {
    NullsFirst,
    NullsLast,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortField {
    pub source_id: FieldId,
    pub transform: PartitionTransform,
    pub direction: SortDirection,
    pub null_order: NullOrder,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortOrder {
    pub order_id: i32,
    pub fields: Vec<SortField>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> SchemaDefinition {
        SchemaDefinition {
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
                    doc: Some("User's name".into()),
                },
                ColumnMeta {
                    id: 3,
                    name: "amount".into(),
                    data_type: TeoDataType::Decimal128 {
                        precision: 18,
                        scale: 2,
                    },
                    nullable: false,
                    doc: None,
                },
            ],
            identifier_field_ids: vec![1],
        }
    }

    #[test]
    fn lookup_by_id() {
        let s = sample_schema();
        assert_eq!(s.by_id(1).unwrap().name, "id");
        assert_eq!(s.by_id(2).unwrap().name, "name");
        assert!(s.by_id(999).is_none());
    }

    #[test]
    fn lookup_by_name() {
        let s = sample_schema();
        assert_eq!(s.by_name("amount").unwrap().id, 3);
        assert!(s.by_name("missing").is_none());
    }

    #[test]
    fn schema_serde_roundtrip() {
        let s = sample_schema();
        let json = serde_json::to_string(&s).unwrap();
        let s2: SchemaDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(s, s2);
    }

    #[test]
    fn partition_spec_serde() {
        let spec = PartitionSpec {
            spec_id: 0,
            fields: vec![PartitionField {
                source_id: 1,
                field_id: 1000,
                name: "id_bucket".into(),
                transform: PartitionTransform::Bucket { num_buckets: 16 },
            }],
        };
        let json = serde_json::to_string(&spec).unwrap();
        let spec2: PartitionSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, spec2);
    }

    #[test]
    fn sort_order_serde() {
        let order = SortOrder {
            order_id: 0,
            fields: vec![SortField {
                source_id: 1,
                transform: PartitionTransform::Identity,
                direction: SortDirection::Asc,
                null_order: NullOrder::NullsLast,
            }],
        };
        let json = serde_json::to_string(&order).unwrap();
        let order2: SortOrder = serde_json::from_str(&json).unwrap();
        assert_eq!(order, order2);
    }
}
