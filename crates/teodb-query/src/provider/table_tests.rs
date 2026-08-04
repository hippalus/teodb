use super::*;
use std::sync::Arc;

use datafusion::datasource::{TableProvider, TableType};
use std::collections::HashMap;
use teodb_core::file::TableMetadata;
use teodb_core::file::{DataContent, DataFile};
use teodb_core::ident::TableIdent;
use teodb_core::location::{ObjectLocation, StorageScheme};
use teodb_core::schema::*;

use teodb_test_support::{MockCatalog, stub_storage_factory};

fn test_metadata() -> TableMetadata {
    TableMetadata {
        table_uuid: uuid::Uuid::nil(),
        namespace: "default".into(),
        table_name: "test".into(),
        table_location: ObjectLocation {
            scheme: StorageScheme::Local,
            bucket: None,
            key: "data/test".into(),
        },
        current_snapshot_id: None,
        current_schema_id: 0,
        current_partition_spec_id: 0,
        current_sort_order_id: 0,
        schemas: vec![SchemaDefinition {
            schema_id: 0,
            columns: vec![ColumnMeta {
                id: 1,
                name: "x".into(),
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

#[tokio::test]
async fn provider_schema() {
    let provider = TeoTableProvider::try_new(
        TableIdent {
            namespace: "default".into(),
            name: "test".into(),
        },
        Arc::new(test_metadata()),
        Arc::new(MockCatalog::empty()),
        stub_storage_factory(),
    )
    .unwrap();

    assert_eq!(provider.schema().fields().len(), 1);
    assert_eq!(provider.schema().field(0).name(), "x");
    assert_eq!(provider.table_type(), TableType::Base);
}

fn data_file(key: &str, records: u64) -> DataFile {
    DataFile {
        content: DataContent::Data,
        path: ObjectLocation {
            scheme: StorageScheme::Local,
            bucket: None,
            key: key.into(),
        },
        format: teodb_core::file::FileFormat::Parquet,
        partition_spec_id: 0,
        sort_order_id: None,
        schema_id: 0,
        partition_values: HashMap::new(),
        record_count: records,
        file_size_bytes: 1024,
        column_sizes: HashMap::new(),
        value_counts: HashMap::new(),
        null_value_counts: HashMap::new(),
        nan_value_counts: HashMap::new(),
        lower_bounds: HashMap::new(),
        upper_bounds: HashMap::new(),
        split_offsets: vec![],
        equality_ids: vec![],
        key_metadata: None,
    }
}

#[test]
fn split_by_deletes_keeps_positions_file_scoped() {
    let files = vec![data_file("data/a.parquet", 10), data_file("data/b.parquet", 10)];

    let mut set = super::delete::PositionDeleteSet::new();
    set.insert_for_test("s3://wh/ns/t/data/b.parquet", 3);
    set.insert_for_test("s3://wh/ns/t/data/b.parquet", 5);

    let (clean, deleted) = split_by_deletes(files, Some(&set));
    assert_eq!(clean.len(), 1);
    assert_eq!(clean[0].path.key, "data/a.parquet");
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].0.path.key, "data/b.parquet");
    assert_eq!(
        deleted[0].1,
        [3, 5].into_iter().collect(),
        "only file b's positions, resolved through the absolute-URI entry"
    );
}

#[test]
fn split_without_delete_set_is_all_clean() {
    let files = vec![data_file("data/a.parquet", 10)];
    let (clean, deleted) = split_by_deletes(files, None);
    assert_eq!(clean.len(), 1);
    assert!(deleted.is_empty());
}

#[tokio::test]
async fn provider_scan_no_snapshot() {
    let provider = TeoTableProvider::try_new(
        TableIdent {
            namespace: "default".into(),
            name: "test".into(),
        },
        Arc::new(test_metadata()),
        Arc::new(MockCatalog::empty()),
        stub_storage_factory(),
    )
    .unwrap();

    let state = datafusion::execution::SessionStateBuilder::new().build();
    let plan = provider
        .scan(&state, None, &[], None)
        .await
        .unwrap();
    assert!(plan.schema().fields().len() == 1);
}
