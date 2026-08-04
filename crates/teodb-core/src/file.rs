use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::ident::{FieldId, SequenceNumber, SnapshotId};
use crate::location::ObjectLocation;
use crate::scalar::{ColumnBounds, TeoScalar};
use crate::schema::{PartitionSpec, SchemaDefinition, SortOrder};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FileFormat {
    Parquet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataContent {
    /// A normal data file: rows that exist in the table.
    Data,
    /// A position-delete file: tombstones referencing (file, row_pos) pairs.
    PositionDelete,
    /// An equality-delete file: tombstones matching column-equality predicates.
    EqualityDelete,
}

/// File-level metadata for a single data or delete file. Fields mirror the
/// Iceberg `DataFile` spec for a 1:1 catalog mapping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFile {
    pub content: DataContent,
    pub path: ObjectLocation,
    pub format: FileFormat,

    pub partition_spec_id: i32,
    pub sort_order_id: Option<i32>,
    pub schema_id: i32,

    /// Partition values keyed by the *partition field's* `field_id`
    /// (not the source column's id), because partition values may be
    /// transformed (year/month/day/bucket/truncate).
    pub partition_values: HashMap<FieldId, TeoScalar>,

    pub record_count: u64,
    pub file_size_bytes: u64,

    pub column_sizes: HashMap<FieldId, u64>,
    pub value_counts: HashMap<FieldId, u64>,
    pub null_value_counts: HashMap<FieldId, u64>,
    pub nan_value_counts: HashMap<FieldId, u64>,
    pub lower_bounds: ColumnBounds,
    pub upper_bounds: ColumnBounds,

    /// Page-level offsets within the Parquet file; populated from the
    /// PageIndex during write. Used by the row-group pruner.
    pub split_offsets: Vec<i64>,

    /// For equality-delete files: which columns the deletes match on.
    /// Empty for `Data` and `PositionDelete`.
    pub equality_ids: Vec<FieldId>,

    /// Optional encryption key reference; reserved for envelope encryption.
    pub key_metadata: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SnapshotOperation {
    Append,
    Overwrite,
    Replace,
    Delete,
}

/// An Iceberg snapshot with TeoDB-specific extensions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub snapshot_id: SnapshotId,
    pub parent_snapshot_id: Option<SnapshotId>,
    pub sequence_number: SequenceNumber,
    pub timestamp_ms: i64,
    pub operation: SnapshotOperation,
    pub data_files: Vec<DataFile>,
    pub delete_files: Vec<DataFile>,
    pub summary: HashMap<String, String>,
}

/// Full table metadata combining Iceberg-level state with TeoDB extensions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableMetadata {
    pub table_uuid: uuid::Uuid,
    pub namespace: String,
    pub table_name: String,
    pub table_location: ObjectLocation,

    pub current_snapshot_id: Option<SnapshotId>,
    pub current_schema_id: i32,
    pub current_partition_spec_id: i32,
    pub current_sort_order_id: i32,

    pub schemas: Vec<SchemaDefinition>,
    pub partition_specs: Vec<PartitionSpec>,
    pub sort_orders: Vec<SortOrder>,

    /// Snapshot summaries in table history. Live files are attached only to
    /// `current_snapshot` to keep ordinary metadata loads small.
    pub snapshots: Vec<Snapshot>,
    pub current_snapshot: Option<Snapshot>,
    pub properties: HashMap<String, String>,
}

impl TableMetadata {
    /// Attach the current snapshot's live data and delete files.
    pub fn with_live_files(mut self, files: Vec<DataFile>) -> crate::error::TeoDBResult<Self> {
        let (data_files, delete_files): (Vec<_>, Vec<_>) = files
            .into_iter()
            .partition(|file| file.content == DataContent::Data);
        match self.current_snapshot.as_mut() {
            Some(snapshot) => {
                snapshot.data_files = data_files;
                snapshot.delete_files = delete_files;
            }
            None if data_files.is_empty() && delete_files.is_empty() => {}
            None => {
                return Err(crate::error::TeoDBError::MetadataCorruption {
                    table: crate::ident::TableIdent::new(&self.namespace, &self.table_name),
                    message: "catalog returned live files for a table without a current snapshot".into(),
                });
            }
        }
        Ok(self)
    }

    /// Returns the schema referenced by `current_schema_id`.
    pub fn current_schema(&self) -> crate::error::TeoDBResult<&SchemaDefinition> {
        self.schemas
            .iter()
            .find(|s| s.schema_id == self.current_schema_id)
            .ok_or_else(|| {
                crate::error::TeoDBError::Internal(format!(
                    "current_schema_id {} not found in schemas list",
                    self.current_schema_id,
                ))
            })
    }

    /// Returns the partition spec referenced by `current_partition_spec_id`.
    pub fn current_partition_spec(&self) -> crate::error::TeoDBResult<&PartitionSpec> {
        self.partition_specs
            .iter()
            .find(|p| p.spec_id == self.current_partition_spec_id)
            .ok_or_else(|| {
                crate::error::TeoDBError::Internal(format!(
                    "current_partition_spec_id {} not found in partition_specs list",
                    self.current_partition_spec_id,
                ))
            })
    }

    /// Returns the sort order referenced by `current_sort_order_id`.
    pub fn current_sort_order(&self) -> crate::error::TeoDBResult<&SortOrder> {
        self.sort_orders
            .iter()
            .find(|s| s.order_id == self.current_sort_order_id)
            .ok_or_else(|| {
                crate::error::TeoDBError::Internal(format!(
                    "current_sort_order_id {} not found in sort_orders list",
                    self.current_sort_order_id,
                ))
            })
    }

    /// Look up a schema by its id. Returns `None` if not found.
    pub fn schema_by_id(&self, id: i32) -> Option<&SchemaDefinition> {
        self.schemas.iter().find(|s| s.schema_id == id)
    }

    pub fn snapshot_by_id(&self, id: SnapshotId) -> Option<&Snapshot> {
        self.snapshots
            .iter()
            .find(|snapshot| snapshot.snapshot_id == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;

    fn sample_table_metadata() -> TableMetadata {
        TableMetadata {
            table_uuid: uuid::Uuid::nil(),
            namespace: "analytics".into(),
            table_name: "events".into(),
            table_location: ObjectLocation {
                scheme: crate::location::StorageScheme::S3,
                bucket: Some("warehouse".into()),
                key: "analytics/events".into(),
            },
            current_snapshot_id: None,
            current_schema_id: 0,
            current_partition_spec_id: 0,
            current_sort_order_id: 0,
            schemas: vec![SchemaDefinition {
                schema_id: 0,
                columns: vec![ColumnMeta {
                    id: 1,
                    name: "id".into(),
                    data_type: TeoDataType::Int64,
                    nullable: false,
                    doc: None,
                }],
                identifier_field_ids: vec![1],
            }],
            partition_specs: vec![PartitionSpec {
                spec_id: 0,
                fields: vec![],
            }],
            sort_orders: vec![SortOrder {
                order_id: 0,
                fields: vec![],
            }],
            snapshots: vec![],
            current_snapshot: None,
            properties: HashMap::new(),
        }
    }

    #[test]
    fn current_schema_lookup() {
        let meta = sample_table_metadata();
        let schema = meta.current_schema().unwrap();
        assert_eq!(schema.schema_id, 0);
        assert_eq!(schema.columns.len(), 1);
    }

    #[test]
    fn current_partition_spec_lookup() {
        let meta = sample_table_metadata();
        let spec = meta.current_partition_spec().unwrap();
        assert_eq!(spec.spec_id, 0);
    }

    #[test]
    fn current_sort_order_lookup() {
        let meta = sample_table_metadata();
        let order = meta.current_sort_order().unwrap();
        assert_eq!(order.order_id, 0);
    }

    #[test]
    fn schema_by_id_found() {
        let meta = sample_table_metadata();
        assert!(meta.schema_by_id(0).is_some());
        assert!(meta.schema_by_id(99).is_none());
    }

    #[test]
    fn serde_roundtrip() {
        let meta = sample_table_metadata();
        let json = serde_json::to_string(&meta).unwrap();
        let meta2: TableMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, meta2);
    }
}
