use super::*;
use std::collections::HashMap;
use std::sync::Arc;
use teodb_core::file::{DataContent, DataFile, FileFormat, Snapshot, SnapshotOperation, TableMetadata};
use teodb_core::ident::{SnapshotId, TableIdent};
use teodb_core::location::ObjectLocation;
use teodb_core::schema::{
    ColumnMeta, NullOrder, PartitionSpec, PartitionTransform, SchemaDefinition, SortDirection, SortField, SortOrder,
    TeoDataType,
};
use teodb_test_support::{MockCatalog, in_memory_backend, single_backend_factory, table_metadata_with_snapshot};

fn test_metadata(snapshot_id: SnapshotId) -> TableMetadata {
    TableMetadata {
        table_uuid: uuid::Uuid::nil(),
        namespace: "test".into(),
        table_name: "compaction_test".into(),
        table_location: ObjectLocation::parse("s3://bucket/test/compaction_test").unwrap(),
        current_snapshot_id: Some(snapshot_id),
        current_schema_id: 0,
        current_partition_spec_id: 0,
        current_sort_order_id: 1,
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
            order_id: 1,
            fields: vec![SortField {
                source_id: 1,
                transform: PartitionTransform::Identity,
                direction: SortDirection::Asc,
                null_order: NullOrder::NullsFirst,
            }],
        }],
        snapshots: vec![],
        current_snapshot: Some(Snapshot {
            snapshot_id,
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: 0,
            operation: SnapshotOperation::Append,
            data_files: vec![],
            delete_files: vec![],
            summary: HashMap::new(),
        }),
        properties: HashMap::new(),
    }
}

#[test]
fn plan_from_group() {
    let group = crate::selection::CompactionGroup {
        partition_spec_id: 0,
        partition_values: HashMap::new(),
        files: vec![DataFile {
            content: DataContent::Data,
            path: ObjectLocation::parse("s3://b/data/a.parquet").unwrap(),
            format: FileFormat::Parquet,
            partition_spec_id: 0,
            sort_order_id: Some(1),
            schema_id: 0,
            partition_values: HashMap::new(),
            record_count: 1000,
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
        }],
        delete_files: vec![],
        base_snapshot_id: 42,
    };

    let plan = CompactionPlan::from_group(group, TableIdent::new("ns", "tbl"), 128 * 1024 * 1024);
    assert_eq!(plan.inputs.len(), 1);
    assert_eq!(plan.base_snapshot_id, 42);
    assert_eq!(plan.output_target_bytes, 128 * 1024 * 1024);
}

#[test]
fn metadata_sort_order() {
    let meta = test_metadata(1);
    let order = meta.current_sort_order().unwrap();
    assert_eq!(order.fields.len(), 1);
    assert_eq!(order.fields[0].source_id, 1);
}

// Streaming compaction e2e (in-memory object store)

/// A catalog serving `ns.events` with a committed snapshot; its write ops
/// return the same metadata so a compaction commit succeeds.
fn compaction_catalog() -> MockCatalog {
    let metadata = table_metadata_with_snapshot("s3://test/events", 7);
    MockCatalog::builder()
        .namespaces(["ns"])
        .tables([TableIdent::new("ns", "events")])
        .serves_any(metadata.clone())
        .commit_result(metadata)
        .build()
}

fn arrow_id_schema() -> arrow::datatypes::SchemaRef {
    let mut metadata = HashMap::new();
    metadata.insert("PARQUET:field_id".to_owned(), "1".to_owned());
    Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false).with_metadata(metadata),
    ]))
}

/// Write a small parquet input file into the shared backend and return
/// its DataFile entry.
async fn write_input(backend: &teodb_storage::ObjectStoreBackend, key: &str, values: Vec<i64>) -> DataFile {
    let schema = arrow_id_schema();
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow::array::Int64Array::from(values))],
    )
    .unwrap();
    let target = ObjectLocation {
        scheme: teodb_core::location::StorageScheme::S3,
        bucket: Some("test".into()),
        key: key.to_owned(),
    };
    let spec = teodb_storage::parquet::WriteSpec::builder(schema)
        .schema_id(0)
        .partition_spec_id(0)
        .build()
        .unwrap();
    teodb_storage::parquet::write_sorted_parquet(backend, &target, vec![batch], &spec)
        .await
        .unwrap()
}

fn compactor_over(backend: Arc<teodb_storage::ObjectStoreBackend>, spill: std::path::PathBuf) -> Compactor {
    Compactor::new(
        Arc::new(compaction_catalog()),
        single_backend_factory(backend.clone()),
        teodb_query::ObjectStoreRegistration::new("s3://test", backend.as_object_store()).unwrap(),
    )
    .with_memory_limit(64 * 1024 * 1024, spill)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_streams_inputs_into_committed_output() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = in_memory_backend();
    let in0 = write_input(&backend, "events/data/in0.parquet", vec![5, 1, 9]).await;
    let in1 = write_input(&backend, "events/data/in1.parquet", vec![3, 7]).await;

    let compactor = compactor_over(backend.clone(), tmp.path().to_path_buf());
    let plan = CompactionPlan {
        table: TableIdent::new("ns", "events"),
        base_snapshot_id: 7,
        partition_spec_id: 0,
        partition_values: HashMap::new(),
        inputs: vec![in0, in1],
        deletes: vec![],
        output_target_bytes: 128 * 1024 * 1024,
    };

    match compactor.compact(plan).await.unwrap() {
        CompactionOutcome::Committed { added, removed } => {
            assert_eq!(removed.len(), 2);
            let rows: u64 = added.iter().map(|f| f.record_count).sum();
            assert_eq!(rows, 5, "all input rows must be rewritten");
            assert!(!added.is_empty());
        }
        other => panic!("expected Committed, got {other:?}"),
    }
}

/// Write a position-delete parquet file (file_path, pos) into the backend and
/// return its `DataFile` entry.
async fn write_position_delete(
    backend: &teodb_storage::ObjectStoreBackend,
    key: &str,
    entries: &[(&str, i64)],
) -> DataFile {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("file_path", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("pos", arrow::datatypes::DataType::Int64, false),
    ]));
    let paths: Vec<&str> = entries.iter().map(|(p, _)| *p).collect();
    let positions: Vec<i64> = entries.iter().map(|(_, p)| *p).collect();
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::StringArray::from(paths)),
            Arc::new(arrow::array::Int64Array::from(positions)),
        ],
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    use teodb_core::traits::storage::Storage;
    backend
        .put(&teodb_core::location::ObjectPath::new(key), bytes::Bytes::from(buf))
        .await
        .unwrap();

    DataFile {
        content: DataContent::PositionDelete,
        path: ObjectLocation {
            scheme: teodb_core::location::StorageScheme::S3,
            bucket: Some("test".into()),
            key: key.to_owned(),
        },
        format: FileFormat::Parquet,
        partition_spec_id: 0,
        sort_order_id: None,
        schema_id: 0,
        partition_values: HashMap::new(),
        record_count: entries.len() as u64,
        file_size_bytes: 0,
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

/// Compaction must apply position deletes: the deleted row is absent from the
/// output, surviving rows are counted once, and a fully-resolved delete file
/// is removed by the commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_applies_position_deletes() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = in_memory_backend();
    let in0 = write_input(&backend, "events/data/in0.parquet", vec![5, 1, 9]).await;
    let in1 = write_input(&backend, "events/data/in1.parquet", vec![3, 7]).await;
    // Delete row 0 of in0 (value 5).
    let del = write_position_delete(&backend, "events/data/del0.parquet", &[("events/data/in0.parquet", 0)]).await;
    let del_loc = del.path.clone();

    let compactor = compactor_over(backend.clone(), tmp.path().to_path_buf());
    let plan = CompactionPlan {
        table: TableIdent::new("ns", "events"),
        base_snapshot_id: 7,
        partition_spec_id: 0,
        partition_values: HashMap::new(),
        inputs: vec![in0, in1],
        deletes: vec![del],
        output_target_bytes: 128 * 1024 * 1024,
    };

    match compactor.compact(plan).await.unwrap() {
        CompactionOutcome::Committed { added, removed } => {
            let rows: u64 = added.iter().map(|f| f.record_count).sum();
            assert_eq!(rows, 4, "5 input rows minus 1 deleted");
            assert!(
                removed.contains(&del_loc),
                "the fully-resolved delete file must be removed"
            );
            assert_eq!(removed.len(), 3, "two inputs plus the resolved delete file");
        }
        other => panic!("expected Committed, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn count_position_deletes_is_keyed_by_target_data_file() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = in_memory_backend();
    let in0 = write_input(&backend, "events/data/in0.parquet", vec![5, 1, 9]).await;
    let in1 = write_input(&backend, "events/data/in1.parquet", vec![3, 7]).await;
    let del = write_position_delete(
        &backend,
        "events/data/del0.parquet",
        &[
            ("events/data/in0.parquet", 0),
            ("events/data/in0.parquet", 99),
            ("events/data/in1.parquet", 1),
            ("events/data/not-an-input.parquet", 0),
            ("events/data/in1.parquet", -1),
        ],
    )
    .await;

    let compactor = compactor_over(backend.clone(), tmp.path().to_path_buf());
    let counts = compactor
        .count_position_deletes(&[del], &[in0.clone(), in1.clone()])
        .await
        .unwrap();

    assert_eq!(counts.get(&in0.path.to_uri()), Some(&1));
    assert_eq!(counts.get(&in1.path.to_uri()), Some(&1));
    assert_eq!(
        counts.get("s3://test/events/data/del0.parquet"),
        None,
        "counts must be keyed by target data file, not delete file"
    );
}

/// An unreadable input must abort the plan. Skipping it would let the
/// commit remove the input without rewriting its rows — data loss.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_aborts_when_an_input_is_unreadable() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = in_memory_backend();
    let in0 = write_input(&backend, "events/data/in0.parquet", vec![5, 1, 9]).await;
    let mut missing = in0.clone();
    missing.path = ObjectLocation {
        scheme: teodb_core::location::StorageScheme::S3,
        bucket: Some("test".into()),
        key: "events/data/does-not-exist.parquet".into(),
    };

    let compactor = compactor_over(backend.clone(), tmp.path().to_path_buf());
    let plan = CompactionPlan {
        table: TableIdent::new("ns", "events"),
        base_snapshot_id: 7,
        partition_spec_id: 0,
        partition_values: HashMap::new(),
        inputs: vec![in0, missing],
        deletes: vec![],
        output_target_bytes: 128 * 1024 * 1024,
    };

    // The plan must abort (no commit) — the scan now fails fast on the missing
    // input rather than silently rewriting fewer rows; the row-conservation
    // check remains as a backstop for inputs that read as empty.
    let err = compactor.compact(plan).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("not found") || msg.contains("inputs claim"),
        "unreadable input must fail the plan, got: {err}"
    );
}
