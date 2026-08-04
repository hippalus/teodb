//! Snapshot pin tracking for query-safe file retention.
//!
//! Active snapshot pins prevent orphan sweeping and compaction from removing
//! files still needed by running queries.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::TeoDBResult;
use crate::ident::{SnapshotId, TableIdent};
use crate::query_id::QueryId;

/// A held snapshot pin. Dropping it releases the pin through the registry.
pub struct SnapshotPin {
    pub query_id: QueryId,
    pub table: TableIdent,
    pub snapshot_id: SnapshotId,
    registry: Arc<dyn ActiveSnapshotRegistry>,
}

impl SnapshotPin {
    pub fn new(
        query_id: QueryId,
        table: TableIdent,
        snapshot_id: SnapshotId,
        registry: Arc<dyn ActiveSnapshotRegistry>,
    ) -> Self {
        Self {
            query_id,
            table,
            snapshot_id,
            registry,
        }
    }
}

impl Drop for SnapshotPin {
    fn drop(&mut self) {
        let query_id = self.query_id;

        // Prefer a guaranteed synchronous release: it works on every drop path,
        // including runtime shutdown and panics, where `tokio::spawn` would
        // either panic (no runtime) or schedule a task that never runs.
        if self.registry.release_sync(query_id) {
            return;
        }

        // Otherwise schedule the async release, but only if a runtime is
        // present — `tokio::spawn` panics outside one. A best-effort fallback.
        let registry = self.registry.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    if let Err(e) = registry.release(query_id).await {
                        tracing::warn!(query_id = %query_id, error = %e, "failed to release snapshot pin");
                    }
                });
            }
            Err(_) => {
                tracing::warn!(
                    query_id = %query_id,
                    "no async runtime available to release snapshot pin on drop; pin may linger until registry GC"
                );
            }
        }
    }
}

/// Registry of snapshot pins held by active queries.
///
/// For single-node mode this is in-memory. For distributed production,
/// pins are scheduler-owned and visible to sweepers.
#[async_trait]
pub trait ActiveSnapshotRegistry: Send + Sync + 'static {
    /// Pin a snapshot for the given query. The pin prevents sweepers from
    /// deleting files referenced by this snapshot.
    async fn pin(&self, query_id: QueryId, table: TableIdent, snapshot_id: SnapshotId) -> TeoDBResult<()>;

    /// Release all pins held by the given query.
    async fn release(&self, query_id: QueryId) -> TeoDBResult<()>;

    /// List all snapshot IDs currently pinned for a table across all queries.
    async fn active_snapshots(&self, table: &TableIdent) -> TeoDBResult<Vec<SnapshotId>>;

    /// Renew the lease deadline for every pin held by the given query.
    ///
    /// Long-running queries call this periodically so a leased pin (in a
    /// distributed registry) does not expire while the query is still active.
    /// Registries without lease semantics treat this as a no-op.
    async fn renew(&self, _query_id: QueryId) -> TeoDBResult<()> {
        Ok(())
    }

    /// Expire and remove stale leased pins for a table whose lease deadline has
    /// passed (e.g. left behind by a crashed node). Returns the number of pins
    /// removed. Registries without lease semantics treat this as a no-op.
    async fn expire_stale(&self, _table: &TableIdent) -> TeoDBResult<usize> {
        Ok(0)
    }

    /// Best-effort synchronous, non-blocking release for `SnapshotPin::drop`.
    ///
    /// Returns `true` if the pin was released synchronously (so `Drop` need not
    /// schedule anything); `false` if the caller should fall back to scheduling
    /// the async [`release`](Self::release). Implementations that can release
    /// without async work (e.g. an in-memory lock) should override this so pins
    /// are guaranteed to release even during runtime shutdown or panics. Must
    /// never block or panic. Defaults to `false`.
    fn release_sync(&self, _query_id: QueryId) -> bool {
        false
    }
}

/// In-memory snapshot pin registry for single-node mode.
pub struct InMemorySnapshotRegistry {
    pins: parking_lot::RwLock<PinState>,
}

#[derive(Clone)]
struct TablePin {
    table: TableIdent,
}

#[derive(Clone)]
struct QueryPin {
    query_id: QueryId,
    snapshot_id: SnapshotId,
}

#[derive(Default)]
struct PinState {
    by_query: HashMap<QueryId, Vec<TablePin>>,
    by_table: HashMap<TableIdent, Vec<QueryPin>>,
}

impl PinState {
    fn pin(&mut self, query_id: QueryId, table: TableIdent, snapshot_id: SnapshotId) {
        self.by_query
            .entry(query_id)
            .or_default()
            .push(TablePin { table: table.clone() });
        self.by_table
            .entry(table)
            .or_default()
            .push(QueryPin { query_id, snapshot_id });
    }

    fn release(&mut self, query_id: QueryId) {
        let Some(pins) = self.by_query.remove(&query_id) else {
            return;
        };

        for pin in pins {
            let Some(table_pins) = self.by_table.get_mut(&pin.table) else {
                continue;
            };
            table_pins.retain(|entry| entry.query_id != query_id);
            if table_pins.is_empty() {
                self.by_table.remove(&pin.table);
            }
        }
    }

    fn active_snapshots(&self, table: &TableIdent) -> Vec<SnapshotId> {
        self.by_table
            .get(table)
            .into_iter()
            .flatten()
            .map(|entry| entry.snapshot_id)
            .collect()
    }
}

impl InMemorySnapshotRegistry {
    pub fn new() -> Self {
        Self {
            pins: parking_lot::RwLock::new(PinState::default()),
        }
    }
}

impl Default for InMemorySnapshotRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ActiveSnapshotRegistry for InMemorySnapshotRegistry {
    async fn pin(&self, query_id: QueryId, table: TableIdent, snapshot_id: SnapshotId) -> TeoDBResult<()> {
        self.pins
            .write()
            .pin(query_id, table, snapshot_id);
        Ok(())
    }

    async fn release(&self, query_id: QueryId) -> TeoDBResult<()> {
        self.pins.write().release(query_id);
        Ok(())
    }

    async fn active_snapshots(&self, table: &TableIdent) -> TeoDBResult<Vec<SnapshotId>> {
        Ok(self.pins.read().active_snapshots(table))
    }

    fn release_sync(&self, query_id: QueryId) -> bool {
        // parking_lot lock, no async work — safe to release directly in `Drop`,
        // guaranteeing pins clear even on shutdown/panic paths.
        self.pins.write().release(query_id);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pin_and_release() {
        let registry = InMemorySnapshotRegistry::new();
        let table = TableIdent::new("ns", "t1");
        let qid = QueryId::new();

        registry
            .pin(qid, table.clone(), 42)
            .await
            .unwrap();
        assert_eq!(registry.active_snapshots(&table).await.unwrap(), vec![42]);

        registry.release(qid).await.unwrap();
        assert!(
            registry
                .active_snapshots(&table)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn multiple_queries_same_table() {
        let registry = InMemorySnapshotRegistry::new();
        let table = TableIdent::new("ns", "t1");
        let q1 = QueryId::new();
        let q2 = QueryId::new();

        registry.pin(q1, table.clone(), 10).await.unwrap();
        registry.pin(q2, table.clone(), 20).await.unwrap();

        let mut snaps = registry.active_snapshots(&table).await.unwrap();
        snaps.sort();
        assert_eq!(snaps, vec![10, 20]);

        registry.release(q1).await.unwrap();
        assert_eq!(registry.active_snapshots(&table).await.unwrap(), vec![20]);
    }

    #[tokio::test]
    async fn dropping_pin_releases_synchronously() {
        let registry: Arc<dyn ActiveSnapshotRegistry> = Arc::new(InMemorySnapshotRegistry::new());
        let table = TableIdent::new("ns", "t1");
        let qid = QueryId::new();
        registry.pin(qid, table.clone(), 7).await.unwrap();

        let pin = SnapshotPin::new(qid, table.clone(), 7, registry.clone());
        drop(pin);

        // In-memory release is synchronous, so the pin is gone immediately
        // without awaiting a spawned task.
        assert!(
            registry
                .active_snapshots(&table)
                .await
                .unwrap()
                .is_empty(),
            "dropping a pin must release it synchronously"
        );
    }

    #[test]
    fn dropping_pin_without_runtime_does_not_panic() {
        // No tokio runtime in scope: the synchronous release path must handle
        // it (the previous tokio::spawn would have panicked).
        let registry: Arc<dyn ActiveSnapshotRegistry> = Arc::new(InMemorySnapshotRegistry::new());
        let table = TableIdent::new("ns", "t1");
        let qid = QueryId::new();
        let pin = SnapshotPin::new(qid, table, 1, registry);
        drop(pin); // must not panic
    }

    #[tokio::test]
    async fn release_only_target_query() {
        let registry = InMemorySnapshotRegistry::new();
        let t1 = TableIdent::new("ns", "t1");
        let t2 = TableIdent::new("ns", "t2");
        let q1 = QueryId::new();
        let q2 = QueryId::new();

        registry.pin(q1, t1.clone(), 1).await.unwrap();
        registry.pin(q2, t2.clone(), 2).await.unwrap();

        registry.release(q1).await.unwrap();
        assert!(
            registry
                .active_snapshots(&t1)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(registry.active_snapshots(&t2).await.unwrap(), vec![2]);
    }

    #[tokio::test]
    async fn release_removes_query_from_every_table_index() {
        let registry = InMemorySnapshotRegistry::new();
        let t1 = TableIdent::new("ns", "t1");
        let t2 = TableIdent::new("ns", "t2");
        let q1 = QueryId::new();
        let q2 = QueryId::new();

        registry.pin(q1, t1.clone(), 1).await.unwrap();
        registry.pin(q1, t2.clone(), 2).await.unwrap();
        registry.pin(q2, t2.clone(), 3).await.unwrap();

        registry.release(q1).await.unwrap();

        assert!(
            registry
                .active_snapshots(&t1)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(registry.active_snapshots(&t2).await.unwrap(), vec![3]);
    }

    #[tokio::test]
    async fn release_sync_keeps_table_index_consistent() {
        let registry = InMemorySnapshotRegistry::new();
        let table = TableIdent::new("ns", "t1");
        let qid = QueryId::new();

        registry
            .pin(qid, table.clone(), 11)
            .await
            .unwrap();
        assert!(registry.release_sync(qid));
        assert!(
            registry
                .active_snapshots(&table)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
