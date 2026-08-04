//! [`CatalogSnapshotRegistry`] implementation. See the module docs for the
//! cross-node and lease design.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use parking_lot::Mutex;
use tracing::{debug, warn};

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::{SnapshotId, TableIdent};
use teodb_core::query_id::QueryId;
use teodb_core::snapshot_pin::ActiveSnapshotRegistry;
use teodb_core::traits::catalog::Catalog;

/// Property-key prefix for a single query's snapshot pin on a table. The full
/// key is `teodb.pin.<query_id>`, so each query owns at most one pin per table.
pub(super) const PIN_PREFIX: &str = "teodb.pin.";

/// Configuration for [`CatalogSnapshotRegistry`].
#[derive(Debug, Clone)]
pub struct SnapshotRegistryConfig {
    /// How long a pin's lease is valid before any node may expire it as stale.
    /// Long-running queries must renew before this elapses. Should comfortably
    /// exceed the renew interval and the orphan-sweep cycle.
    pub lease_ttl: Duration,
    /// Maximum CAS retries when concurrent property writes on the same table
    /// conflict. Each retry reloads the current properties.
    pub max_cas_retries: usize,
}

impl Default for SnapshotRegistryConfig {
    fn default() -> Self {
        Self {
            lease_ttl: Duration::from_secs(300),
            max_cas_retries: 8,
        }
    }
}

/// A leased snapshot pin, encoded into a single table property value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LeasedPin {
    pub(super) snapshot_id: SnapshotId,
    pub(super) owner: String,
    pub(super) created_ms: u64,
    pub(super) deadline_ms: u64,
}

impl LeasedPin {
    pub(super) fn encode(&self) -> String {
        format!(
            "{};{};{};{}",
            self.snapshot_id, self.owner, self.created_ms, self.deadline_ms
        )
    }

    /// Parse a stored value. Returns `None` for malformed values so callers can
    /// treat them conservatively (expire on cleanup, ignore on read).
    pub(super) fn decode(value: &str) -> Option<Self> {
        let mut parts = value.split(';');
        let snapshot_id = parts.next()?.parse().ok()?;
        let owner = parts.next()?.to_string();
        let created_ms = parts.next()?.parse().ok()?;
        let deadline_ms = parts.next()?.parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(Self {
            snapshot_id,
            owner,
            created_ms,
            deadline_ms,
        })
    }
}

/// Cross-node snapshot pin registry backed by Iceberg table properties.
pub struct CatalogSnapshotRegistry {
    catalog: Arc<dyn Catalog>,
    node_id: String,
    config: SnapshotRegistryConfig,
    /// Tables this node pinned per query, so `release`/`renew` target the right
    /// properties without scanning every table in the catalog.
    held: Mutex<HashMap<QueryId, Vec<TableIdent>>>,
}

impl CatalogSnapshotRegistry {
    pub fn new(catalog: Arc<dyn Catalog>, node_id: String, config: SnapshotRegistryConfig) -> Self {
        Self {
            catalog,
            node_id,
            config,
            held: Mutex::new(HashMap::new()),
        }
    }

    pub(super) fn pin_key(query_id: QueryId) -> String {
        format!("{PIN_PREFIX}{query_id}")
    }

    fn lease_deadline(&self, now_ms: u64) -> u64 {
        now_ms.saturating_add(self.config.lease_ttl.as_millis() as u64)
    }

    fn record_local(&self, query_id: QueryId, table: TableIdent) {
        let mut held = self.held.lock();
        let tables = held.entry(query_id).or_default();
        if !tables.contains(&table) {
            tables.push(table);
        }
    }

    fn forget_local(&self, query_id: QueryId) -> Vec<TableIdent> {
        self.held
            .lock()
            .remove(&query_id)
            .unwrap_or_default()
    }

    fn tables_for(&self, query_id: QueryId) -> Vec<TableIdent> {
        self.held
            .lock()
            .get(&query_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Set a single pin property via CAS, retrying on concurrent-write
    /// conflicts. `expected` is the current value of `key` (empty when absent),
    /// so a racing writer that changed the *same* key forces a reload.
    async fn cas_set(&self, table: &TableIdent, key: &str, value: String) -> TeoDBResult<()> {
        for attempt in 0..=self.config.max_cas_retries {
            let metadata = self.catalog.load_table(table).await?;
            let current = metadata
                .properties
                .get(key)
                .cloned()
                .unwrap_or_default();
            let expected = HashMap::from([(key.to_string(), current)]);
            let updates = HashMap::from([(key.to_string(), value.clone())]);
            match self
                .catalog
                .update_table_properties(table, expected, updates, vec![])
                .await
            {
                Ok(_) => return Ok(()),
                Err(TeoDBError::Conflict { .. }) if attempt < self.config.max_cas_retries => {
                    debug!(table = %table, key, attempt, "snapshot pin CAS conflict, retrying");
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Err(TeoDBError::Conflict {
            resource: format!("snapshot pin '{key}'"),
            expected: "exclusive CAS".into(),
            actual: "exhausted retries".into(),
        })
    }

    /// Remove the given pin keys via CAS, retrying on conflict.
    async fn cas_remove(&self, table: &TableIdent, keys: &[String]) -> TeoDBResult<()> {
        for attempt in 0..=self.config.max_cas_retries {
            let metadata = self.catalog.load_table(table).await?;
            let props = &metadata.properties;
            let expected: HashMap<String, String> = keys
                .iter()
                .map(|key| (key.clone(), props.get(key).cloned().unwrap_or_default()))
                .collect();
            // Nothing to remove (already gone): treat as success.
            if expected.values().all(String::is_empty) {
                return Ok(());
            }
            match self
                .catalog
                .update_table_properties(table, expected, HashMap::new(), keys.to_vec())
                .await
            {
                Ok(_) => return Ok(()),
                Err(TeoDBError::Conflict { .. }) if attempt < self.config.max_cas_retries => {
                    debug!(table = %table, attempt, "snapshot pin removal CAS conflict, retrying");
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Err(TeoDBError::Conflict {
            resource: "snapshot pin removal".into(),
            expected: "exclusive CAS".into(),
            actual: "exhausted retries".into(),
        })
    }
}

#[async_trait]
impl ActiveSnapshotRegistry for CatalogSnapshotRegistry {
    async fn pin(&self, query_id: QueryId, table: TableIdent, snapshot_id: SnapshotId) -> TeoDBResult<()> {
        let now = now_ms();
        let pin = LeasedPin {
            snapshot_id,
            owner: self.node_id.clone(),
            created_ms: now,
            deadline_ms: self.lease_deadline(now),
        };
        self.cas_set(&table, &Self::pin_key(query_id), pin.encode())
            .await?;
        self.record_local(query_id, table);
        Ok(())
    }

    async fn release(&self, query_id: QueryId) -> TeoDBResult<()> {
        let tables = self.forget_local(query_id);
        let key = Self::pin_key(query_id);
        let mut last_error = None;
        for table in tables {
            // Best effort: a failed release leaves a pin that the lease will
            // expire, so log and continue rather than abandoning other tables.
            if let Err(error) = self
                .cas_remove(&table, std::slice::from_ref(&key))
                .await
            {
                warn!(table = %table, query_id = %query_id, error = %error, "failed to release snapshot pin; lease will expire it");
                last_error = Some(error);
            }
        }
        match last_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn active_snapshots(&self, table: &TableIdent) -> TeoDBResult<Vec<SnapshotId>> {
        // A load failure here must propagate: the orphan sweep treats it as a
        // retention failure (skip deletion) rather than assuming "no pins".
        let metadata = self.catalog.load_table(table).await?;
        let now = now_ms();
        Ok(metadata
            .properties
            .iter()
            .filter(|(key, _)| key.starts_with(PIN_PREFIX))
            .filter_map(|(_, value)| LeasedPin::decode(value))
            .filter(|pin| pin.deadline_ms >= now)
            .map(|pin| pin.snapshot_id)
            .collect())
    }

    async fn renew(&self, query_id: QueryId) -> TeoDBResult<()> {
        let tables = self.tables_for(query_id);
        let key = Self::pin_key(query_id);
        for table in tables {
            // Reload to preserve snapshot_id/created and only bump the deadline,
            // retrying on conflict via cas_set's loop.
            let now = now_ms();
            let metadata = self.catalog.load_table(&table).await?;
            let Some(mut pin) = metadata
                .properties
                .get(&key)
                .and_then(|value| LeasedPin::decode(value))
            else {
                // Pin already expired/removed: nothing to renew on this table.
                continue;
            };
            pin.deadline_ms = self.lease_deadline(now);
            self.cas_set(&table, &key, pin.encode()).await?;
        }
        Ok(())
    }

    async fn expire_stale(&self, table: &TableIdent) -> TeoDBResult<usize> {
        for attempt in 0..=self.config.max_cas_retries {
            let metadata = self.catalog.load_table(table).await?;
            let now = now_ms();
            let props = &metadata.properties;
            let stale: Vec<String> = props
                .iter()
                .filter(|(key, _)| key.starts_with(PIN_PREFIX))
                .filter(|(_, value)| LeasedPin::decode(value).is_none_or(|pin| pin.deadline_ms < now))
                .map(|(key, _)| key.clone())
                .collect();
            if stale.is_empty() {
                return Ok(0);
            }
            let expected: HashMap<String, String> = stale
                .iter()
                .map(|key| (key.clone(), props.get(key).cloned().unwrap_or_default()))
                .collect();
            match self
                .catalog
                .update_table_properties(table, expected, HashMap::new(), stale.clone())
                .await
            {
                Ok(_) => return Ok(stale.len()),
                Err(TeoDBError::Conflict { .. }) if attempt < self.config.max_cas_retries => {
                    debug!(table = %table, attempt, "expire-stale CAS conflict, retrying");
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Err(TeoDBError::Conflict {
            resource: "expire stale snapshot pins".into(),
            expected: "exclusive CAS".into(),
            actual: "exhausted retries".into(),
        })
    }
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
