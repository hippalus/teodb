//! Table-metadata fixtures shared by tests.

use std::collections::HashMap;
use std::sync::Arc;

use teodb_core::file::{Snapshot, SnapshotOperation, TableMetadata};
use teodb_core::ident::SnapshotId;
use teodb_core::location::ObjectLocation;
use teodb_core::schema::{ColumnMeta, PartitionSpec, SchemaDefinition, SortOrder, TeoDataType};

pub fn table_metadata(location: &str) -> Arc<TableMetadata> {
    Arc::new(base_metadata(location))
}

pub fn table_metadata_with_snapshot(location: &str, snapshot_id: SnapshotId) -> Arc<TableMetadata> {
    let mut metadata = base_metadata(location);
    metadata.current_snapshot_id = Some(snapshot_id);
    let snapshot = Snapshot {
        snapshot_id,
        parent_snapshot_id: None,
        sequence_number: 1,
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
        operation: SnapshotOperation::Append,
        data_files: Vec::new(),
        delete_files: Vec::new(),
        summary: HashMap::new(),
    };
    metadata.snapshots.push(snapshot.clone());
    metadata.current_snapshot = Some(snapshot);
    Arc::new(metadata)
}

fn base_metadata(location: &str) -> TableMetadata {
    let table_location = ObjectLocation::parse(location).expect("test table location");
    let mut segments = table_location
        .key
        .split('/')
        .filter(|segment| !segment.is_empty())
        .rev();
    let table_name = segments.next().unwrap_or("events").to_owned();
    let namespace = segments.next().unwrap_or("default").to_owned();

    TableMetadata {
        table_uuid: teodb_core::TableUuid::now_v7(),
        namespace,
        table_name,
        table_location,
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
            fields: Vec::new(),
        }],
        sort_orders: vec![SortOrder {
            order_id: 0,
            fields: Vec::new(),
        }],
        snapshots: Vec::new(),
        current_snapshot: None,
        properties: HashMap::new(),
    }
}
