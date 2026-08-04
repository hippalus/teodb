use super::*;

use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, TryStreamExt};

use teodb_core::error::TeoDBResult;
use teodb_core::ident::{SnapshotId, TableIdent};
use teodb_core::location::{ObjectLocation, ObjectPath};
use teodb_core::query_id::QueryId;
use teodb_core::snapshot_pin::{ActiveSnapshotRegistry, InMemorySnapshotRegistry};
use teodb_core::traits::storage::{ObjectMeta, Storage, StorageFactory};
use teodb_storage::ObjectStoreBackend;

use teodb_test_support::{MockCatalog, SnapshotFiles, in_memory_backend, single_backend_factory, table_metadata};

const TABLE_LOCATION: &str = "s3://warehouse/ns/events";

/// A catalog serving the test table and reporting `uris` as the file paths
/// referenced across its snapshot history.
fn referencing_catalog(uris: &[&str]) -> MockCatalog {
    MockCatalog::builder()
        .serves_any(table_metadata(TABLE_LOCATION))
        .referenced(uris.iter().copied())
        .build()
}

async fn put(backend: &ObjectStoreBackend, key: &str) {
    backend
        .put(&ObjectPath::new(key), Bytes::from_static(b"x"))
        .await
        .expect("put test object");
}

async fn exists(backend: &ObjectStoreBackend, key: &str) -> bool {
    backend.head(&ObjectPath::new(key)).await.is_ok()
}

/// Storage double that violates the scoped-list contract by appending one
/// object outside the requested prefix. The sweeper must still refuse to
/// delete it.
struct AnomalousListingStorage {
    inner: Arc<dyn Storage>,
    anomaly: ObjectMeta,
}

#[async_trait]
impl Storage for AnomalousListingStorage {
    async fn get(&self, path: &ObjectPath) -> TeoDBResult<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &ObjectPath, range: Range<u64>) -> TeoDBResult<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn head(&self, path: &ObjectPath) -> TeoDBResult<ObjectMeta> {
        self.inner.head(path).await
    }

    async fn put(&self, path: &ObjectPath, bytes: Bytes) -> TeoDBResult<ObjectMeta> {
        self.inner.put(path, bytes).await
    }

    async fn delete(&self, path: &ObjectPath) -> TeoDBResult<()> {
        self.inner.delete(path).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> TeoDBResult<()> {
        self.inner.copy(from, to).await
    }

    async fn list(
        &self,
        prefix: &ObjectPath,
    ) -> TeoDBResult<Pin<Box<dyn Stream<Item = TeoDBResult<ObjectMeta>> + Send>>> {
        let mut objects = self
            .inner
            .list(prefix)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        objects.push(self.anomaly.clone());
        Ok(Box::pin(futures::stream::iter(objects.into_iter().map(Ok))))
    }
}

struct ExactStorageFactory {
    storage: Arc<dyn Storage>,
}

#[async_trait]
impl StorageFactory for ExactStorageFactory {
    async fn resolve(&self, location: &ObjectLocation) -> TeoDBResult<(Arc<dyn Storage>, ObjectPath)> {
        Ok((Arc::clone(&self.storage), ObjectPath::new(location.key.clone())))
    }
}

#[test]
fn sweep_report_values() {
    let report = SweepReport {
        scanned: 10,
        deleted: 3,
        retained: 7,
    };
    assert_eq!(report.scanned, 10);
    assert_eq!(report.deleted, 3);
    assert_eq!(report.retained, 7);
}

#[tokio::test]
async fn sweep_deletes_only_unreferenced_data_files() {
    let backend = in_memory_backend();
    put(&backend, "ns/events/data/committed.parquet").await;
    put(&backend, "ns/events/data/historical.parquet").await;
    put(&backend, "ns/events/data/orphan.parquet").await;
    // Catalog-owned files: must never be listed, considered, or deleted.
    put(&backend, "ns/events/metadata/v1.metadata.json").await;
    put(&backend, "ns/events/metadata/snap-1-manifest-list.avro").await;

    let catalog = referencing_catalog(&[
        "s3://warehouse/ns/events/data/committed.parquet",
        // Referenced only by a historical snapshot — still protected.
        "s3://warehouse/ns/events/data/historical.parquet",
    ]);
    let sweeper = OrphanSweeper::new(
        Arc::new(catalog),
        single_backend_factory(backend.clone()),
        Duration::ZERO,
    );

    let report = sweeper
        .sweep(&TableIdent::new("ns", "events"))
        .await
        .expect("sweep");

    // Only the data/ subtree is scanned: 3 files, not 5.
    assert_eq!(report.scanned, 3);
    assert_eq!(report.deleted, 1);
    assert_eq!(report.retained, 2);

    assert!(!exists(&backend, "ns/events/data/orphan.parquet").await);
    assert!(exists(&backend, "ns/events/data/committed.parquet").await);
    assert!(exists(&backend, "ns/events/data/historical.parquet").await);
    assert!(exists(&backend, "ns/events/metadata/v1.metadata.json").await);
    assert!(exists(&backend, "ns/events/metadata/snap-1-manifest-list.avro").await);
}

#[tokio::test]
async fn mw_t17_sweep_supports_legacy_and_nested_layouts_and_contains_list_anomalies() {
    let backend = in_memory_backend();
    let legacy = "ns/events/data/legacy-flat.parquet";
    let nested = "ns/events/data/region=eu/writer-a/commit-p0000-f0000.parquet";
    let nested_orphan = "ns/events/data/region=us/writer-b/orphan-p0000-f0000.parquet";
    let outside_data = "ns/events/metadata/must-not-delete.json";

    for key in [legacy, nested, nested_orphan, outside_data] {
        put(&backend, key).await;
    }

    let anomaly = backend
        .head(&ObjectPath::new(outside_data))
        .await
        .expect("anomaly metadata");
    let anomalous_storage: Arc<dyn Storage> = Arc::new(AnomalousListingStorage {
        inner: backend.clone(),
        anomaly,
    });
    let factory: Arc<dyn StorageFactory> = Arc::new(ExactStorageFactory {
        storage: anomalous_storage,
    });
    let catalog = referencing_catalog(&[
        "s3://warehouse/ns/events/data/legacy-flat.parquet",
        "s3://warehouse/ns/events/data/region=eu/writer-a/commit-p0000-f0000.parquet",
    ]);
    let sweeper = OrphanSweeper::new(Arc::new(catalog), factory, Duration::ZERO);

    let report = sweeper
        .sweep(&TableIdent::new("ns", "events"))
        .await
        .expect("sweep");

    assert_eq!(
        report,
        SweepReport {
            scanned: 4,
            deleted: 1,
            retained: 3,
        }
    );
    assert!(exists(&backend, legacy).await);
    assert!(exists(&backend, nested).await);
    assert!(!exists(&backend, nested_orphan).await);
    assert!(
        exists(&backend, outside_data).await,
        "defense-in-depth prefix validation must contain a backend list anomaly"
    );
}

#[tokio::test]
async fn sweep_retains_files_younger_than_min_age() {
    let backend = in_memory_backend();
    put(&backend, "ns/events/data/orphan.parquet").await;

    let sweeper = OrphanSweeper::new(
        Arc::new(referencing_catalog(&[])),
        single_backend_factory(backend.clone()),
        Duration::from_secs(3600),
    );

    let report = sweeper
        .sweep(&TableIdent::new("ns", "events"))
        .await
        .expect("sweep");

    assert_eq!(report.scanned, 1);
    assert_eq!(report.deleted, 0);
    assert!(exists(&backend, "ns/events/data/orphan.parquet").await);
}

#[tokio::test]
async fn sweep_without_retention_deletes_true_orphans_despite_pins() {
    // Pins no longer skip the table: a pinned snapshot's files are part
    // of the protected history, so true orphans stay safe to delete.
    let backend = in_memory_backend();
    put(&backend, "ns/events/data/committed.parquet").await;
    put(&backend, "ns/events/data/orphan.parquet").await;

    let table = TableIdent::new("ns", "events");
    let registry = Arc::new(InMemorySnapshotRegistry::new());
    registry
        .pin(QueryId::new(), table.clone(), 42)
        .await
        .expect("pin");

    let sweeper = OrphanSweeper::new(
        Arc::new(referencing_catalog(&[
            "s3://warehouse/ns/events/data/committed.parquet",
        ])),
        single_backend_factory(backend.clone()),
        Duration::ZERO,
    )
    .with_snapshot_registry(registry);

    let report = sweeper.sweep(&table).await.expect("sweep");

    assert_eq!(
        report,
        SweepReport {
            scanned: 2,
            deleted: 1,
            retained: 1,
        }
    );
    assert!(!exists(&backend, "ns/events/data/orphan.parquet").await);
    assert!(exists(&backend, "ns/events/data/committed.parquet").await);
}

// Snapshot expiration

use teodb_core::snapshot_retention::SnapshotRetention;

const HOUR_MS: i64 = 3_600_000;

/// Two-snapshot history: snapshot 1 (expired-eligible, references the
/// compacted-away file) and snapshot 2 (current, references the live file).
fn two_snapshot_catalog() -> MockCatalog {
    let now = chrono::Utc::now().timestamp_millis();
    MockCatalog::builder()
        .serves_any(table_metadata(TABLE_LOCATION))
        .snapshots(
            vec![
                SnapshotFiles::new(
                    1,
                    now - 10 * HOUR_MS,
                    ["s3://warehouse/ns/events/data/compacted-away.parquet"],
                ),
                SnapshotFiles::new(2, now, ["s3://warehouse/ns/events/data/live.parquet"]),
            ],
            Some(2),
        )
        .build()
}

fn hour_retention() -> SnapshotRetention {
    SnapshotRetention {
        max_age: Duration::from_secs(3600),
        keep_last: 1,
    }
}

#[tokio::test]
async fn retention_sweep_reclaims_files_of_expired_snapshots() {
    let backend = in_memory_backend();
    put(&backend, "ns/events/data/compacted-away.parquet").await;
    put(&backend, "ns/events/data/live.parquet").await;
    put(&backend, "ns/events/data/orphan.parquet").await;

    let sweeper = OrphanSweeper::new(
        Arc::new(two_snapshot_catalog()),
        single_backend_factory(backend.clone()),
        Duration::ZERO,
    )
    .with_retention(hour_retention());

    let report = sweeper
        .sweep(&TableIdent::new("ns", "events"))
        .await
        .expect("sweep");

    assert_eq!(
        report,
        SweepReport {
            scanned: 3,
            deleted: 2,
            retained: 1,
        }
    );
    // Snapshot 1 expired: its exclusive file is reclaimed along with the orphan.
    assert!(!exists(&backend, "ns/events/data/compacted-away.parquet").await);
    assert!(!exists(&backend, "ns/events/data/orphan.parquet").await);
    assert!(exists(&backend, "ns/events/data/live.parquet").await);
}

#[tokio::test]
async fn retention_sweep_protects_pinned_snapshot_files() {
    let backend = in_memory_backend();
    put(&backend, "ns/events/data/compacted-away.parquet").await;
    put(&backend, "ns/events/data/live.parquet").await;

    let table = TableIdent::new("ns", "events");
    let registry = Arc::new(InMemorySnapshotRegistry::new());
    // A running query still reads snapshot 1 — it must not expire.
    registry
        .pin(QueryId::new(), table.clone(), 1)
        .await
        .expect("pin");

    let sweeper = OrphanSweeper::new(
        Arc::new(two_snapshot_catalog()),
        single_backend_factory(backend.clone()),
        Duration::ZERO,
    )
    .with_snapshot_registry(registry)
    .with_retention(hour_retention());

    let report = sweeper.sweep(&table).await.expect("sweep");

    assert_eq!(report.deleted, 0);
    assert!(exists(&backend, "ns/events/data/compacted-away.parquet").await);
    assert!(exists(&backend, "ns/events/data/live.parquet").await);
}

/// Registry stub: no pins on the first read, a pin on snapshot 1 on the
/// re-check — simulating a query that pinned mid-sweep.
struct LatePinRegistry {
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl ActiveSnapshotRegistry for LatePinRegistry {
    async fn pin(&self, _query_id: QueryId, _table: TableIdent, _snapshot_id: SnapshotId) -> TeoDBResult<()> {
        Ok(())
    }
    async fn release(&self, _query_id: QueryId) -> TeoDBResult<()> {
        Ok(())
    }
    async fn active_snapshots(&self, _table: &TableIdent) -> TeoDBResult<Vec<SnapshotId>> {
        let call = self
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(if call == 0 { vec![] } else { vec![1] })
    }
}

#[tokio::test]
async fn retention_sweep_aborts_when_pin_appears_mid_sweep() {
    let backend = in_memory_backend();
    put(&backend, "ns/events/data/compacted-away.parquet").await;
    put(&backend, "ns/events/data/live.parquet").await;

    let sweeper = OrphanSweeper::new(
        Arc::new(two_snapshot_catalog()),
        single_backend_factory(backend.clone()),
        Duration::ZERO,
    )
    .with_snapshot_registry(Arc::new(LatePinRegistry {
        calls: std::sync::atomic::AtomicUsize::new(0),
    }))
    .with_retention(hour_retention());

    let report = sweeper
        .sweep(&TableIdent::new("ns", "events"))
        .await
        .expect("sweep");

    // Snapshot 1 got pinned between the protection walk and the delete
    // phase: nothing may be deleted this cycle.
    assert_eq!(report.deleted, 0);
    assert!(exists(&backend, "ns/events/data/compacted-away.parquet").await);
    assert!(exists(&backend, "ns/events/data/live.parquet").await);
}
