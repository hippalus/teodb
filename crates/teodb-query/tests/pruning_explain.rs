//! EXPLAIN-based pruning-effectiveness tests (D3).
//!
//! These prove that file pruning survives the whole pipeline — SQL text →
//! DataFusion planner → filter pushdown → `TeoTableProvider::scan` — by
//! asserting which data files appear in the EXPLAIN'd physical plan. The
//! pure pruning functions have their own unit tests; what's verified here
//! is the integration: filters actually reach the provider and the pruned
//! file set actually shapes the `DataSourceExec`.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::SessionContext;

use teodb_core::file::{DataContent, DataFile, FileFormat, Snapshot, SnapshotOperation, TableMetadata};
use teodb_core::ident::TableIdent;
use teodb_core::location::{ObjectLocation, StorageScheme};
use teodb_core::scalar::TeoScalar;
use teodb_core::schema::{
    ColumnMeta, PartitionField, PartitionSpec, PartitionTransform, SchemaDefinition, SortOrder, TeoDataType,
};
use teodb_query::TeoTableProvider;
use teodb_test_support::{MockCatalog, stub_storage_factory};

/// A data file with an identity `region` partition value and `x` bounds.
fn data_file(name: &str, region: &str, x_lo: i64, x_hi: i64) -> DataFile {
    let mut partition_values = HashMap::new();
    // Keyed by the partition field's id (1000), as the catalog stores it — not by
    // the source column's id (1), which is what `lower_bounds` below uses.
    partition_values.insert(1000, TeoScalar::Utf8(region.into()));
    let mut lower_bounds = HashMap::new();
    lower_bounds.insert(2, TeoScalar::Int64(x_lo));
    let mut upper_bounds = HashMap::new();
    upper_bounds.insert(2, TeoScalar::Int64(x_hi));

    DataFile {
        content: DataContent::Data,
        path: ObjectLocation {
            scheme: StorageScheme::Local,
            bucket: None,
            key: format!("data/{name}.parquet"),
        },
        format: FileFormat::Parquet,
        partition_spec_id: 0,
        sort_order_id: None,
        schema_id: 0,
        partition_values,
        record_count: 100,
        file_size_bytes: 1024,
        column_sizes: HashMap::new(),
        value_counts: HashMap::new(),
        null_value_counts: HashMap::new(),
        nan_value_counts: HashMap::new(),
        lower_bounds,
        upper_bounds,
        split_offsets: vec![],
        equality_ids: vec![],
        key_metadata: None,
    }
}

fn metadata(partition_transform: PartitionTransform, files: Vec<DataFile>) -> TableMetadata {
    TableMetadata {
        table_uuid: uuid::Uuid::nil(),
        namespace: "default".into(),
        table_name: "events".into(),
        table_location: ObjectLocation {
            scheme: StorageScheme::Local,
            bucket: None,
            key: "data/events".into(),
        },
        current_snapshot_id: Some(1),
        current_schema_id: 0,
        current_partition_spec_id: 0,
        current_sort_order_id: 0,
        schemas: vec![SchemaDefinition {
            schema_id: 0,
            columns: vec![
                ColumnMeta {
                    id: 1,
                    name: "region".into(),
                    data_type: TeoDataType::Utf8,
                    nullable: false,
                    doc: None,
                },
                ColumnMeta {
                    id: 2,
                    name: "x".into(),
                    data_type: TeoDataType::Int64,
                    nullable: false,
                    doc: None,
                },
            ],
            identifier_field_ids: vec![1],
        }],
        partition_specs: vec![PartitionSpec {
            spec_id: 0,
            fields: vec![PartitionField {
                source_id: 1,
                field_id: 1000,
                name: "region".into(),
                transform: partition_transform,
            }],
        }],
        sort_orders: vec![SortOrder {
            order_id: 0,
            fields: vec![],
        }],
        snapshots: vec![],
        current_snapshot: Some(Snapshot {
            snapshot_id: 1,
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: 0,
            operation: SnapshotOperation::Append,
            data_files: files,
            delete_files: vec![],
            summary: HashMap::new(),
        }),
        properties: HashMap::new(),
    }
}

/// Register a provider over `meta` and return the full EXPLAIN text of `sql`.
async fn explain(meta: TableMetadata, sql: &str) -> String {
    let provider = TeoTableProvider::try_new(
        TableIdent::new("default", "events"),
        Arc::new(meta),
        Arc::new(MockCatalog::empty()),
        stub_storage_factory(),
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("events", Arc::new(provider))
        .unwrap();

    let batches = ctx
        .sql(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

fn identity_fixture() -> TableMetadata {
    metadata(
        PartitionTransform::Identity,
        vec![
            data_file("us_low", "us", 0, 50),
            data_file("us_high", "us", 100, 200),
            data_file("eu_low", "eu", 0, 50),
        ],
    )
}

#[tokio::test]
async fn no_filter_scans_all_files() {
    let plan = explain(identity_fixture(), "SELECT * FROM events").await;
    assert!(plan.contains("us_low.parquet"), "plan:\n{plan}");
    assert!(plan.contains("us_high.parquet"), "plan:\n{plan}");
    assert!(plan.contains("eu_low.parquet"), "plan:\n{plan}");
}

#[tokio::test]
async fn partition_filter_prunes_other_partitions() {
    let plan = explain(identity_fixture(), "SELECT * FROM events WHERE region = 'us'").await;
    assert!(plan.contains("us_low.parquet"), "plan:\n{plan}");
    assert!(plan.contains("us_high.parquet"), "plan:\n{plan}");
    assert!(!plan.contains("eu_low.parquet"), "eu file must be pruned:\n{plan}");
    // Inexact pushdown: the filter must still be applied above the scan.
    assert!(plan.contains("FilterExec"), "filter must remain in plan:\n{plan}");
}

#[tokio::test]
async fn statistics_filter_prunes_by_bounds() {
    let plan = explain(identity_fixture(), "SELECT * FROM events WHERE x > 60").await;
    assert!(plan.contains("us_high.parquet"), "plan:\n{plan}");
    assert!(!plan.contains("us_low.parquet"), "x≤50 file must be pruned:\n{plan}");
    assert!(!plan.contains("eu_low.parquet"), "x≤50 file must be pruned:\n{plan}");
}

#[tokio::test]
async fn mixed_filter_prunes_on_both_dimensions() {
    // The non-partition conjunct (x > 150) must not disable partition
    // pruning on region, and bounds must prune within the partition.
    let plan = explain(
        identity_fixture(),
        "SELECT * FROM events WHERE region = 'us' AND x > 150",
    )
    .await;
    assert!(plan.contains("us_high.parquet"), "plan:\n{plan}");
    assert!(!plan.contains("us_low.parquet"), "stats must prune x≤50:\n{plan}");
    assert!(!plan.contains("eu_low.parquet"), "partition must prune eu:\n{plan}");
}

#[tokio::test]
async fn fully_pruned_scan_becomes_empty() {
    let plan = explain(identity_fixture(), "SELECT * FROM events WHERE x > 1000").await;
    assert!(!plan.contains(".parquet"), "all files must be pruned:\n{plan}");
}

#[tokio::test]
async fn bucket_partition_keeps_files_and_filter() {
    // Bucket partition values are transformed; a raw `region = 'us'`
    // predicate proves nothing about them. Files must survive and the
    // filter must still execute above the scan (correctness over pruning).
    let meta = metadata(
        PartitionTransform::Bucket { num_buckets: 16 },
        vec![data_file("bucket_a", "us", 0, 50), data_file("bucket_b", "eu", 0, 50)],
    );
    let plan = explain(meta, "SELECT * FROM events WHERE region = 'us'").await;
    assert!(plan.contains("bucket_a.parquet"), "plan:\n{plan}");
    assert!(
        plan.contains("bucket_b.parquet"),
        "transformed values must not prune:\n{plan}"
    );
    assert!(plan.contains("FilterExec"), "filter must remain in plan:\n{plan}");
}
