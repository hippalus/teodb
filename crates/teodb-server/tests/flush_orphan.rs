//! Flush-orphan e2e: a flush that uploads its Parquet file
//! but permanently fails the catalog commit leaves an orphan under
//! `{table}/data/` — the orphan sweeper must reclaim it without touching
//! anything else, and the buffered rows must survive for the next flush
//! attempt (no data loss).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::TryStreamExt;

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::file::{DataFile, TableMetadata};
use teodb_core::ident::TableIdent;
use teodb_core::location::ObjectPath;
use teodb_core::traits::catalog::{Catalog, CommitAppend, CommitReplace, CommitStatus, CreateTableRequest};
use teodb_core::traits::storage::{Storage, StorageFactory};
use teodb_ingest::buffer::{BufferRegistry, TableBuffer};
use teodb_ingest::flush::{FlushOutcome, Flusher};
use teodb_storage::ObjectStoreBackend;
use teodb_test_support::{MockCatalog, in_memory_backend, single_backend_factory, table_metadata};

const TABLE_LOCATION: &str = "s3://warehouse/ns/events";
const TABLE_UUID: uuid::Uuid = uuid::Uuid::from_u128(1);

fn test_metadata() -> Arc<TableMetadata> {
    let mut metadata = table_metadata(TABLE_LOCATION).as_ref().clone();
    metadata.table_uuid = TABLE_UUID;
    Arc::new(metadata)
}

/// Catalog whose `commit_append` always fails with a permanent error.
/// The table itself exists and has no committed snapshots.
struct FailingCommitCatalog;

#[async_trait]
impl Catalog for FailingCommitCatalog {
    async fn list_namespaces(&self) -> TeoDBResult<Vec<String>> {
        Ok(vec!["ns".into()])
    }
    async fn create_namespace(&self, _ns: &str, _props: HashMap<String, String>) -> TeoDBResult<()> {
        Ok(())
    }
    async fn drop_namespace(&self, _ns: &str) -> TeoDBResult<()> {
        Ok(())
    }
    async fn list_tables(&self, _ns: &str) -> TeoDBResult<Vec<TableIdent>> {
        Ok(vec![TableIdent::new("ns", "events")])
    }
    async fn load_table(&self, _ident: &TableIdent) -> TeoDBResult<Arc<TableMetadata>> {
        Ok(test_metadata())
    }
    async fn create_table(&self, _req: CreateTableRequest) -> TeoDBResult<Arc<TableMetadata>> {
        unimplemented!()
    }
    async fn drop_table(&self, _ident: &TableIdent) -> TeoDBResult<()> {
        unimplemented!()
    }
    async fn load_live_files(&self, _ident: &TableIdent) -> TeoDBResult<Vec<DataFile>> {
        Ok(vec![])
    }
    async fn load_all_referenced_file_paths(&self, _ident: &TableIdent) -> TeoDBResult<HashSet<String>> {
        // No snapshot ever committed — nothing is referenced.
        Ok(HashSet::new())
    }
    async fn commit_append(&self, _req: CommitAppend) -> TeoDBResult<Arc<TableMetadata>> {
        Err(TeoDBError::Catalog(
            "permanent commit failure (simulated catalog outage)".into(),
        ))
    }

    async fn check_append_status(&self, _req: &CommitAppend) -> TeoDBResult<CommitStatus> {
        Ok(CommitStatus::NotCommitted)
    }

    async fn commit_replace(&self, _req: CommitReplace) -> TeoDBResult<Arc<TableMetadata>> {
        unimplemented!()
    }
    async fn update_table_properties(
        &self,
        _ident: &TableIdent,
        _expected: HashMap<String, String>,
        _updates: HashMap<String, String>,
        _removals: Vec<String>,
    ) -> TeoDBResult<Arc<TableMetadata>> {
        unimplemented!()
    }
}

fn test_batch() -> arrow::record_batch::RecordBatch {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
        "id",
        arrow::datatypes::DataType::Int64,
        false,
    )]));
    arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3]))])
        .unwrap()
}

async fn test_flusher(
    catalog: Arc<dyn Catalog>,
    storage_factory: Arc<dyn StorageFactory>,
) -> (tempfile::TempDir, Flusher) {
    let directory = tempfile::tempdir().unwrap();
    let wal = Arc::new(
        teodb_storage::wal::WalManager::open(teodb_storage::wal::WalConfig {
            root_dir: directory.path().to_path_buf(),
            fsync_on_append: false,
            ..Default::default()
        })
        .await
        .unwrap(),
    );
    let registry = Arc::new(BufferRegistry::new(wal.clone(), 64 * 1024 * 1024, 48 * 1024 * 1024));
    (directory, Flusher::new(registry, catalog, storage_factory, wal))
}

async fn list_data_files(backend: &ObjectStoreBackend) -> Vec<String> {
    backend
        .list(&ObjectPath::new("ns/events/data"))
        .await
        .expect("list")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect")
        .into_iter()
        .map(|o| o.path.as_str().to_string())
        .collect()
}

#[tokio::test]
async fn failed_commit_orphan_is_reclaimed_by_sweeper() {
    let backend = in_memory_backend();
    let factory = single_backend_factory(backend.clone());
    let catalog = FailingCommitCatalog;

    let buffer = TableBuffer::new(
        TableIdent::new("ns", "events"),
        test_metadata(),
        0,
        64 * 1024 * 1024,
        48 * 1024 * 1024,
    );
    buffer
        .insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();

    // Flush: the Parquet upload succeeds, the catalog commit fails.
    let (_directory, flusher) = test_flusher(Arc::new(catalog), factory.clone()).await;
    let err = flusher
        .flush_once(&buffer)
        .await
        .expect_err("commit failure must surface");
    assert!(
        err.to_string()
            .contains("permanent commit failure"),
        "got: {err}"
    );

    // The upload is now an orphan under {table}/data/ …
    let orphans = list_data_files(&backend).await;
    assert_eq!(orphans.len(), 1, "exactly one orphan parquet: {orphans:?}");
    assert!(orphans[0].ends_with(".parquet"));

    // … and the rows are back in the buffer for the next attempt (I1: no
    // ACKed data is lost to a failed flush).
    assert!(
        buffer.has_pending(),
        "rows must return to pending after a failed commit"
    );

    // The sweeper reclaims the orphan: nothing in the table's history
    // references it (no snapshot was ever committed).
    let sweeper = teodb_distributed::orphan::OrphanSweeper::new(
        Arc::new(FailingCommitCatalog),
        single_backend_factory(backend.clone()),
        Duration::ZERO,
    );
    let report = sweeper
        .sweep(&TableIdent::new("ns", "events"))
        .await
        .expect("sweep");

    assert_eq!(report.scanned, 1);
    assert_eq!(report.deleted, 1);
    assert!(list_data_files(&backend).await.is_empty(), "orphan reclaimed");
}

#[tokio::test]
async fn young_orphan_survives_min_age_grace() {
    let backend = in_memory_backend();
    let factory = single_backend_factory(backend.clone());

    let buffer = TableBuffer::new(
        TableIdent::new("ns", "events"),
        test_metadata(),
        0,
        64 * 1024 * 1024,
        48 * 1024 * 1024,
    );
    buffer
        .insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    let (_directory, flusher) = test_flusher(Arc::new(FailingCommitCatalog), factory.clone()).await;
    flusher
        .flush_once(&buffer)
        .await
        .expect_err("commit fails");

    // A freshly written orphan is inside another writer's race window —
    // the min_age grace must protect it.
    let sweeper = teodb_distributed::orphan::OrphanSweeper::new(
        Arc::new(FailingCommitCatalog),
        single_backend_factory(backend.clone()),
        Duration::from_secs(3600),
    );
    let report = sweeper
        .sweep(&TableIdent::new("ns", "events"))
        .await
        .expect("sweep");

    assert_eq!(report.deleted, 0);
    assert_eq!(list_data_files(&backend).await.len(), 1);
}

#[tokio::test]
async fn successful_flush_then_retry_after_failure_commits_all_rows() {
    // Sanity: after the failed attempt, a working catalog commits the same
    // rows — the orphan from the failed attempt stays unreferenced.
    let backend = in_memory_backend();
    let factory = single_backend_factory(backend.clone());

    let buffer = TableBuffer::new(
        TableIdent::new("ns", "events"),
        test_metadata(),
        0,
        64 * 1024 * 1024,
        48 * 1024 * 1024,
    );
    buffer
        .insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();

    let (_failed_directory, failing_flusher) = test_flusher(Arc::new(FailingCommitCatalog), factory.clone()).await;
    failing_flusher
        .flush_once(&buffer)
        .await
        .expect_err("first attempt fails");

    let metadata = test_metadata();
    let catalog = MockCatalog::builder()
        .serves_any(metadata.clone())
        .commit_result(metadata)
        .build();
    let (_success_directory, successful_flusher) = test_flusher(Arc::new(catalog), factory.clone()).await;
    let outcome = successful_flusher
        .flush_once(&buffer)
        .await
        .expect("retry succeeds");
    assert!(matches!(outcome, FlushOutcome::Committed { record_count: 3, .. }));
    assert!(!buffer.has_pending(), "rows committed on retry");

    // Two uploads happened; only the second is referenced by the catalog.
    assert_eq!(list_data_files(&backend).await.len(), 2);
}
