use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::file::{DataFile, TableMetadata};
use teodb_core::ident::{SnapshotId, TableIdent};
use teodb_core::query_id::QueryId;
use teodb_core::snapshot_pin::ActiveSnapshotRegistry;
use teodb_core::traits::catalog::{Catalog, CommitAppend, CommitStatus};
use teodb_test_support::table_metadata;

use super::registry::{LeasedPin, PIN_PREFIX};
use super::{CatalogSnapshotRegistry, SnapshotRegistryConfig};

/// A minimal stateful catalog: it actually persists property updates with CAS
/// semantics so the registry's read-after-write behavior can be tested.
struct StatefulCatalog {
    metadata: Mutex<Arc<TableMetadata>>,
}

impl StatefulCatalog {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            metadata: Mutex::new(table_metadata("s3://bucket/ns/t")),
        })
    }

    fn pin_count(&self) -> usize {
        self.metadata
            .lock()
            .unwrap()
            .properties
            .keys()
            .filter(|key| key.starts_with(PIN_PREFIX))
            .count()
    }
}

#[async_trait]
impl Catalog for StatefulCatalog {
    async fn list_namespaces(&self) -> TeoDBResult<Vec<String>> {
        Ok(vec![])
    }
    async fn create_namespace(&self, _: &str, _: HashMap<String, String>) -> TeoDBResult<()> {
        Ok(())
    }
    async fn drop_namespace(&self, _: &str) -> TeoDBResult<()> {
        Ok(())
    }
    async fn list_tables(&self, _: &str) -> TeoDBResult<Vec<TableIdent>> {
        Ok(vec![])
    }
    async fn load_table(&self, _: &TableIdent) -> TeoDBResult<Arc<TableMetadata>> {
        Ok(self.metadata.lock().unwrap().clone())
    }
    async fn create_table(
        &self,
        _: teodb_core::traits::catalog::CreateTableRequest,
    ) -> TeoDBResult<Arc<TableMetadata>> {
        unimplemented!()
    }
    async fn drop_table(&self, _: &TableIdent) -> TeoDBResult<()> {
        Ok(())
    }
    async fn load_live_files(&self, _: &TableIdent) -> TeoDBResult<Vec<DataFile>> {
        Ok(vec![])
    }
    async fn commit_append(&self, _: CommitAppend) -> TeoDBResult<Arc<TableMetadata>> {
        unimplemented!()
    }
    async fn check_append_status(&self, _: &CommitAppend) -> TeoDBResult<CommitStatus> {
        Ok(CommitStatus::NotCommitted)
    }
    async fn commit_replace(&self, _: teodb_core::traits::catalog::CommitReplace) -> TeoDBResult<Arc<TableMetadata>> {
        unimplemented!()
    }
    async fn update_table_properties(
        &self,
        _: &TableIdent,
        expected: HashMap<String, String>,
        updates: HashMap<String, String>,
        removals: Vec<String>,
    ) -> TeoDBResult<Arc<TableMetadata>> {
        let mut guard = self.metadata.lock().unwrap();
        let current = &guard.properties;
        for (key, expected_val) in &expected {
            let actual = current.get(key).map(String::as_str).unwrap_or("");
            if actual != expected_val.as_str() {
                return Err(TeoDBError::Conflict {
                    resource: format!("property '{key}'"),
                    expected: expected_val.clone(),
                    actual: actual.to_string(),
                });
            }
        }
        let mut rebuilt = guard.as_ref().clone();
        rebuilt.properties.extend(updates);
        for key in removals {
            rebuilt.properties.remove(&key);
        }
        *guard = Arc::new(rebuilt);
        Ok(guard.clone())
    }
}

fn registry(catalog: Arc<dyn Catalog>, node: &str) -> CatalogSnapshotRegistry {
    CatalogSnapshotRegistry::new(catalog, node.into(), SnapshotRegistryConfig::default())
}

fn table() -> TableIdent {
    TableIdent::new("ns", "t")
}

#[tokio::test]
async fn pin_is_visible_then_released() {
    let catalog = StatefulCatalog::new();
    let reg = registry(catalog.clone(), "node-a");
    let q = QueryId::new();

    reg.pin(q, table(), 42).await.unwrap();
    assert_eq!(reg.active_snapshots(&table()).await.unwrap(), vec![42]);

    reg.release(q).await.unwrap();
    assert!(
        reg.active_snapshots(&table())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(catalog.pin_count(), 0);
}

#[tokio::test]
async fn pins_are_visible_across_registry_instances() {
    // Two registries over the same catalog model two data nodes: node-b's
    // sweep must see node-a's pin.
    let catalog = StatefulCatalog::new();
    let node_a = registry(catalog.clone(), "node-a");
    let node_b = registry(catalog.clone(), "node-b");
    let q = QueryId::new();

    node_a.pin(q, table(), 7).await.unwrap();
    assert_eq!(node_b.active_snapshots(&table()).await.unwrap(), vec![7]);
}

#[tokio::test]
async fn expired_lease_is_not_active_and_is_swept() {
    let catalog = StatefulCatalog::new();
    let reg = CatalogSnapshotRegistry::new(
        catalog.clone(),
        "node-a".into(),
        SnapshotRegistryConfig {
            lease_ttl: Duration::from_millis(0),
            max_cas_retries: 8,
        },
    );
    let q = QueryId::new();
    reg.pin(q, table(), 99).await.unwrap();

    // ttl 0 sets deadline = pin time; sleep so the read clock is strictly past
    // it (millisecond resolution would otherwise make the boundary racy).
    std::thread::sleep(Duration::from_millis(2));
    assert!(
        reg.active_snapshots(&table())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(catalog.pin_count(), 1, "expired pin still stored until swept");

    let expired = reg.expire_stale(&table()).await.unwrap();
    assert_eq!(expired, 1);
    assert_eq!(catalog.pin_count(), 0);
}

#[tokio::test]
async fn renew_extends_an_active_lease() {
    let catalog = StatefulCatalog::new();
    let reg = CatalogSnapshotRegistry::new(
        catalog.clone(),
        "node-a".into(),
        SnapshotRegistryConfig {
            lease_ttl: Duration::from_secs(60),
            max_cas_retries: 8,
        },
    );
    let q = QueryId::new();
    reg.pin(q, table(), 5).await.unwrap();

    let before = stored_deadline(&catalog, q);
    std::thread::sleep(Duration::from_millis(5));
    reg.renew(q).await.unwrap();
    let after = stored_deadline(&catalog, q);

    assert!(after > before, "renew must push the deadline forward");
    assert_eq!(reg.active_snapshots(&table()).await.unwrap(), vec![5]);
}

#[tokio::test]
async fn multiple_queries_pin_same_table() {
    let catalog = StatefulCatalog::new();
    let reg = registry(catalog.clone(), "node-a");
    let q1 = QueryId::new();
    let q2 = QueryId::new();

    reg.pin(q1, table(), 1).await.unwrap();
    reg.pin(q2, table(), 2).await.unwrap();

    let snaps: HashSet<SnapshotId> = reg
        .active_snapshots(&table())
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(snaps, HashSet::from([1, 2]));

    reg.release(q1).await.unwrap();
    assert_eq!(reg.active_snapshots(&table()).await.unwrap(), vec![2]);
}

#[test]
fn leased_pin_roundtrips() {
    let pin = LeasedPin {
        snapshot_id: 123,
        owner: "node-x".into(),
        created_ms: 1000,
        deadline_ms: 2000,
    };
    assert_eq!(LeasedPin::decode(&pin.encode()), Some(pin));
    assert_eq!(LeasedPin::decode("garbage"), None);
    assert_eq!(LeasedPin::decode("1;o;2;3;extra"), None);
}

fn stored_deadline(catalog: &StatefulCatalog, query_id: QueryId) -> u64 {
    let key = CatalogSnapshotRegistry::pin_key(query_id);
    let guard = catalog.metadata.lock().unwrap();
    LeasedPin::decode(guard.properties.get(&key).expect("pin present"))
        .expect("decodable")
        .deadline_ms
}
