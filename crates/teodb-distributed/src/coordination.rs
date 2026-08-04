//! Catalog-CAS based compaction coordination for multi-instance deployments.
//!
//! Uses Iceberg table properties as a distributed lock. Before compacting a
//! table, a node writes `teodb.compaction.owner = <node_id>` and
//! `teodb.compaction.lock_ts = <epoch_ms>` via the catalog's CAS-guarded
//! property update. If another node already holds the lock (and it hasn't
//! expired), the attempt is skipped.
//!
//! This avoids the need for a coordination service or leader election — the
//! catalog itself provides the CAS primitive.
//!
//! ## Correctness vs. mutual exclusion (no fencing token)
//!
//! This lock is **advisory**: it has no monotonic epoch / fencing token, so it
//! is not strictly exclusive under clock skew or a stale owner. An expired lock
//! can be stolen by another node while the original owner is still (slowly)
//! compacting, so two nodes may briefly do redundant compaction work for the
//! same table.
//!
//! That is wasteful but **not** a correctness bug. Data-file correctness is
//! fenced at commit time, not by this lock: `commit_replace` performs a
//! snapshot-id CAS against the base snapshot (see
//! `teodb-catalog`/`CommitReplace`). The first writer to commit wins; the
//! loser's commit fails with `Conflict` and its output files are abandoned as
//! orphans (reclaimed by the sweeper). So even if the lock is double-held, at
//! most one compaction can commit. The lock only reduces wasted work; it is
//! deliberately not relied upon for isolation. Introducing a real fencing
//! token would require a monotonic epoch the catalog can CAS on and is left as
//! a future optimization.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use teodb_core::error::TeoDBError;
use teodb_core::ident::TableIdent;
use teodb_core::traits::catalog::Catalog;

const PROP_OWNER: &str = "teodb.compaction.owner";
const PROP_LOCK_TS: &str = "teodb.compaction.lock_ts";

/// Configuration for the compaction lock.
#[derive(Debug, Clone)]
pub struct CompactionLockConfig {
    /// How long before a lock is considered stale and can be stolen.
    /// Should be at least 2x the compaction interval.
    pub lock_ttl: Duration,
}

impl Default for CompactionLockConfig {
    fn default() -> Self {
        Self {
            lock_ttl: Duration::from_secs(2 * 3600), // 2 hours
        }
    }
}

/// Outcome of a lock acquisition attempt.
#[derive(Debug)]
pub enum LockOutcome {
    /// Lock acquired; the caller should proceed with compaction.
    Acquired,
    /// Another node holds a valid (non-expired) lock.
    HeldBy { owner: String, locked_since_ms: u64 },
    /// Lock acquisition failed due to a transient error.
    Failed(TeoDBError),
}

/// Distributed compaction lock using Iceberg table properties as CAS storage.
pub struct CompactionLock {
    catalog: Arc<dyn Catalog>,
    node_id: String,
    config: CompactionLockConfig,
}

impl CompactionLock {
    pub fn new(catalog: Arc<dyn Catalog>, node_id: String, config: CompactionLockConfig) -> Self {
        Self {
            catalog,
            node_id,
            config,
        }
    }

    /// Try to acquire the compaction lock for a table. Returns `Acquired` if
    /// this node now owns the lock, `HeldBy` if another node has it, or
    /// `Failed` on transient errors.
    pub async fn try_acquire(&self, table: &TableIdent) -> LockOutcome {
        let now_ms = now_epoch_ms();

        // Load current table metadata to read existing lock properties.
        let metadata = match self.catalog.load_table(table).await {
            Ok(m) => m,
            Err(e) => return LockOutcome::Failed(e),
        };

        let props = &metadata.properties;
        let current_owner = props.get(PROP_OWNER).cloned().unwrap_or_default();
        let current_ts: u64 = props
            .get(PROP_LOCK_TS)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        // If we already own the lock, refresh it.
        if current_owner == self.node_id {
            return self
                .refresh_lock(table, &current_owner, current_ts, now_ms)
                .await;
        }

        // If another node holds it and it's not expired, skip.
        if !current_owner.is_empty() && !self.is_expired(current_ts, now_ms) {
            debug!(
                table = %table,
                owner = %current_owner,
                locked_since_ms = current_ts,
                "compaction lock held by another node"
            );
            return LockOutcome::HeldBy {
                owner: current_owner,
                locked_since_ms: current_ts,
            };
        }

        // Lock is free or expired — attempt to acquire via CAS.
        if !current_owner.is_empty() {
            info!(
                table = %table,
                stale_owner = %current_owner,
                stale_ts = current_ts,
                "stealing expired compaction lock"
            );
        }

        let expected = if current_owner.is_empty() {
            // No lock set — expect empty values.
            HashMap::from([
                (PROP_OWNER.to_string(), String::new()),
                (PROP_LOCK_TS.to_string(), String::new()),
            ])
        } else {
            // Expired lock — expect stale values for CAS.
            HashMap::from([
                (PROP_OWNER.to_string(), current_owner.clone()),
                (PROP_LOCK_TS.to_string(), current_ts.to_string()),
            ])
        };

        let updates = HashMap::from([
            (PROP_OWNER.to_string(), self.node_id.clone()),
            (PROP_LOCK_TS.to_string(), now_ms.to_string()),
        ]);

        match self
            .catalog
            .update_table_properties(table, expected, updates, vec![])
            .await
        {
            Ok(_) => {
                info!(table = %table, node = %self.node_id, "compaction lock acquired");
                LockOutcome::Acquired
            }
            Err(TeoDBError::Conflict { .. }) => {
                debug!(table = %table, "compaction lock CAS conflict, another node won");
                // Re-read to report the winner.
                match self.catalog.load_table(table).await {
                    Ok(m) => {
                        let p = &m.properties;
                        LockOutcome::HeldBy {
                            owner: p.get(PROP_OWNER).cloned().unwrap_or_default(),
                            locked_since_ms: p
                                .get(PROP_LOCK_TS)
                                .and_then(|v| v.parse().ok())
                                .unwrap_or(0),
                        }
                    }
                    Err(e) => LockOutcome::Failed(e),
                }
            }
            Err(e) => LockOutcome::Failed(e),
        }
    }

    /// Release the compaction lock for a table. Only succeeds if this node
    /// currently owns the lock.
    pub async fn release(&self, table: &TableIdent) {
        let metadata = match self.catalog.load_table(table).await {
            Ok(m) => m,
            Err(e) => {
                warn!(table = %table, error = %e, "failed to load table for lock release");
                return;
            }
        };

        let props = &metadata.properties;
        let current_owner = props.get(PROP_OWNER).cloned().unwrap_or_default();
        let current_ts = props
            .get(PROP_LOCK_TS)
            .cloned()
            .unwrap_or_default();

        if current_owner != self.node_id {
            debug!(
                table = %table,
                current_owner = %current_owner,
                our_id = %self.node_id,
                "not releasing lock we don't own"
            );
            return;
        }

        let expected = HashMap::from([
            (PROP_OWNER.to_string(), current_owner),
            (PROP_LOCK_TS.to_string(), current_ts),
        ]);

        let removals = vec![PROP_OWNER.to_string(), PROP_LOCK_TS.to_string()];

        match self
            .catalog
            .update_table_properties(table, expected, HashMap::new(), removals)
            .await
        {
            Ok(_) => {
                info!(table = %table, node = %self.node_id, "compaction lock released");
            }
            Err(e) => {
                warn!(
                    table = %table,
                    error = %e,
                    "failed to release compaction lock (may have been stolen)"
                );
            }
        }
    }

    /// Refresh our own lock timestamp. Used when we already hold the lock.
    async fn refresh_lock(&self, table: &TableIdent, current_owner: &str, current_ts: u64, now_ms: u64) -> LockOutcome {
        let expected = HashMap::from([
            (PROP_OWNER.to_string(), current_owner.to_string()),
            (PROP_LOCK_TS.to_string(), current_ts.to_string()),
        ]);

        let updates = HashMap::from([
            (PROP_OWNER.to_string(), self.node_id.clone()),
            (PROP_LOCK_TS.to_string(), now_ms.to_string()),
        ]);

        match self
            .catalog
            .update_table_properties(table, expected, updates, vec![])
            .await
        {
            Ok(_) => {
                debug!(table = %table, node = %self.node_id, "compaction lock refreshed");
                LockOutcome::Acquired
            }
            Err(TeoDBError::Conflict { .. }) => {
                warn!(
                    table = %table,
                    "our lock was stolen while refreshing"
                );
                LockOutcome::HeldBy {
                    owner: "unknown".to_string(),
                    locked_since_ms: 0,
                }
            }
            Err(e) => LockOutcome::Failed(e),
        }
    }

    fn is_expired(&self, lock_ts: u64, now_ms: u64) -> bool {
        if lock_ts == 0 {
            return true;
        }
        let age_ms = now_ms.saturating_sub(lock_ts);
        age_ms > self.config.lock_ttl.as_millis() as u64
    }
}

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use teodb_test_support::MockCatalog;

    #[test]
    fn expired_lock_detection() {
        let cfg = CompactionLockConfig {
            lock_ttl: Duration::from_secs(60),
        };
        let lock = CompactionLock {
            catalog: Arc::new(MockCatalog::empty()),
            node_id: "node-1".to_string(),
            config: cfg,
        };

        let now = now_epoch_ms();
        assert!(lock.is_expired(now - 120_000, now));
        assert!(!lock.is_expired(now - 30_000, now));
        assert!(lock.is_expired(0, now));
    }
}
