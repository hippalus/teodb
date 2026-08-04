//! Serializable scan descriptor for snapshot-pinned distributed scans.
//!
//! The planning node resolves a table's snapshot once, captures all metadata
//! needed to scan it, and ships this descriptor to executors. Executors
//! never re-resolve the table from the catalog — they build their scans
//! entirely from the descriptor, guaranteeing snapshot isolation.

use serde::{Deserialize, Serialize};

use crate::file::{DataFile, Snapshot};
use crate::ident::SnapshotId;
use crate::location::ObjectLocation;
use crate::schema::{PartitionSpec, SchemaDefinition, SortOrder};

/// Self-contained scan descriptor produced by the planning node and consumed
/// by executors. Carries everything needed to build a DataFusion scan plan
/// without touching the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotScanDescriptor {
    /// Table UUID from catalog metadata.
    pub table_uuid: uuid::Uuid,
    /// Namespace the table belongs to.
    pub namespace: String,
    /// Table name.
    pub table_name: String,
    /// Root location of the table on object storage.
    pub table_location: ObjectLocation,

    /// Snapshot that this scan is bound to.
    pub snapshot_id: SnapshotId,

    /// Full schema definition (not just the ID) — executors need the column
    /// metadata to build Arrow schemas correctly even if the table has evolved.
    pub schema: SchemaDefinition,
    /// Full partition spec (not just the ID) — needed for partition pruning.
    pub partition_spec: PartitionSpec,
    /// Full sort order (not just the ID) — needed for merge-ordered scans.
    pub sort_order: SortOrder,

    /// Data files to scan, already filtered by the planning node's pruning.
    pub data_files: Vec<DataFile>,
    /// Delete files that apply to the data files.
    pub delete_files: Vec<DataFile>,
}

impl SnapshotScanDescriptor {
    /// Create a descriptor from table metadata and its current snapshot.
    ///
    /// Captures a complete, self-contained view of the table at the given
    /// snapshot. The caller is responsible for pinning the snapshot in the
    /// `ActiveSnapshotRegistry` before calling this.
    pub fn from_metadata(
        metadata: &crate::file::TableMetadata,
        snapshot: &Snapshot,
    ) -> crate::error::TeoDBResult<Self> {
        let schema = metadata.current_schema()?.clone();
        let partition_spec = metadata.current_partition_spec()?.clone();
        let sort_order = metadata.current_sort_order()?.clone();

        Ok(Self {
            table_uuid: metadata.table_uuid,
            namespace: metadata.namespace.clone(),
            table_name: metadata.table_name.clone(),
            table_location: metadata.table_location.clone(),
            snapshot_id: snapshot.snapshot_id,
            schema,
            partition_spec,
            sort_order,
            data_files: snapshot
                .data_files
                .iter()
                .filter(|f| f.content == crate::file::DataContent::Data)
                .cloned()
                .collect(),
            delete_files: snapshot.delete_files.clone(),
        })
    }

    /// Total number of records across all data files.
    pub fn total_record_count(&self) -> u64 {
        self.data_files
            .iter()
            .map(|f| f.record_count)
            .sum()
    }

    /// Total size in bytes across all data files.
    pub fn total_size_bytes(&self) -> u64 {
        self.data_files
            .iter()
            .map(|f| f.file_size_bytes)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::*;
    use crate::location::StorageScheme;
    use crate::schema::*;
    use std::collections::HashMap;

    fn test_metadata_with_snapshot() -> (TableMetadata, Snapshot) {
        let snapshot = Snapshot {
            snapshot_id: 42,
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: 1000,
            operation: SnapshotOperation::Append,
            data_files: vec![DataFile {
                content: DataContent::Data,
                path: ObjectLocation {
                    scheme: StorageScheme::Local,
                    bucket: None,
                    key: "data/test/part-0.parquet".into(),
                },
                format: FileFormat::Parquet,
                partition_spec_id: 0,
                sort_order_id: Some(0),
                schema_id: 0,
                partition_values: HashMap::new(),
                record_count: 1000,
                file_size_bytes: 50_000,
                column_sizes: HashMap::new(),
                value_counts: HashMap::new(),
                null_value_counts: HashMap::new(),
                nan_value_counts: HashMap::new(),
                lower_bounds: HashMap::new(),
                upper_bounds: HashMap::new(),
                split_offsets: vec![],
                equality_ids: vec![],
                key_metadata: None,
            }],
            delete_files: vec![],
            summary: HashMap::new(),
        };

        let metadata = TableMetadata {
            table_uuid: uuid::Uuid::nil(),
            namespace: "default".into(),
            table_name: "events".into(),
            table_location: ObjectLocation {
                scheme: StorageScheme::Local,
                bucket: None,
                key: "data/test".into(),
            },
            current_snapshot_id: Some(42),
            current_schema_id: 0,
            current_partition_spec_id: 0,
            current_sort_order_id: 0,
            schemas: vec![SchemaDefinition {
                schema_id: 0,
                columns: vec![ColumnMeta {
                    id: 1,
                    name: "event_id".into(),
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
            snapshots: vec![snapshot.clone()],
            current_snapshot: Some(snapshot.clone()),
            properties: HashMap::new(),
        };

        (metadata, snapshot)
    }

    #[test]
    fn descriptor_from_metadata() {
        let (metadata, snapshot) = test_metadata_with_snapshot();
        let desc = SnapshotScanDescriptor::from_metadata(&metadata, &snapshot).unwrap();

        assert_eq!(desc.snapshot_id, 42);
        assert_eq!(desc.namespace, "default");
        assert_eq!(desc.table_name, "events");
        assert_eq!(desc.data_files.len(), 1);
        assert_eq!(desc.delete_files.len(), 0);
        assert_eq!(desc.total_record_count(), 1000);
        assert_eq!(desc.total_size_bytes(), 50_000);
    }

    #[test]
    fn descriptor_serde_roundtrip() {
        let (metadata, snapshot) = test_metadata_with_snapshot();
        let desc = SnapshotScanDescriptor::from_metadata(&metadata, &snapshot).unwrap();

        let json = serde_json::to_string(&desc).unwrap();
        let roundtripped: SnapshotScanDescriptor = serde_json::from_str(&json).unwrap();

        assert_eq!(roundtripped.snapshot_id, desc.snapshot_id);
        assert_eq!(roundtripped.table_name, desc.table_name);
        assert_eq!(roundtripped.data_files.len(), desc.data_files.len());
        assert_eq!(roundtripped.schema.schema_id, desc.schema.schema_id);
    }

    #[test]
    fn descriptor_filters_non_data_files() {
        let (metadata, mut snapshot) = test_metadata_with_snapshot();
        // Add a delete file to the data_files list.
        snapshot.data_files.push(DataFile {
            content: DataContent::PositionDelete,
            path: ObjectLocation {
                scheme: StorageScheme::Local,
                bucket: None,
                key: "data/test/delete-0.parquet".into(),
            },
            format: FileFormat::Parquet,
            partition_spec_id: 0,
            sort_order_id: None,
            schema_id: 0,
            partition_values: HashMap::new(),
            record_count: 10,
            file_size_bytes: 500,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            split_offsets: vec![],
            equality_ids: vec![],
            key_metadata: None,
        });

        let desc = SnapshotScanDescriptor::from_metadata(&metadata, &snapshot).unwrap();
        // Only the Data file should be included.
        assert_eq!(desc.data_files.len(), 1);
        assert_eq!(desc.data_files[0].content, DataContent::Data);
    }
}
