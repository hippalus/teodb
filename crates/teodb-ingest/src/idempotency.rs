//! Per-table idempotency-key index for at-least-once ingest dedupe.
//!
//! Scope: **per stable writer**. Keys are tracked in memory by the writer that
//! served the original request and rebuilt from its WAL replay on restart, so the
//! dedupe window after a restart covers exactly the unflushed (replayable)
//! records. In a multi-writer deployment a retry routed to a different writer
//! is not deduplicated — clients that need cross-node dedupe must pin a
//! table's ingest traffic to one writer (e.g. consistent hashing at the LB).
//!
//! Protocol (claim-before-WAL):
//! 1. `claim` the key before the WAL append. Exactly one concurrent caller
//!    wins (`Claim::Acquired`); the rest see `InProgress` (retry later, 409)
//!    or `Duplicate` (original receipt, 200).
//! 2. The winner appends to the WAL and inserts into the buffer, then
//!    `complete`s the claim with the receipt — or `abort`s it on failure so
//!    a client retry can win the key again.
//!
//! The index is bounded per table (oldest completed entries evicted beyond
//! the cap) and entries expire after a TTL; both bounds are config knobs.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use moka::sync::Cache;
use papaya::HashMap as PapayaHashMap;
use parking_lot::Mutex;
use teodb_core::ident::{Generation, TableIdent};

/// What the original (or current) request produced for a key.
#[derive(Debug, Clone)]
pub struct IngestReceipt {
    pub batch_id: uuid::Uuid,
    pub writer_id: teodb_core::write_protocol::WriterId,
    pub generation: Generation,
    pub accepted_rows: u64,
}

/// Outcome of claiming a key before the WAL append.
#[derive(Debug)]
pub enum Claim {
    /// Caller owns the key: it must `complete` or `abort` it.
    Acquired,
    /// The key already completed; serve the original receipt.
    Duplicate(IngestReceipt),
    /// Another request holding this key is still in flight.
    InProgress,
}

#[derive(Debug, Clone)]
struct CompletedEntry {
    receipt: IngestReceipt,
    /// Preserve the historical dedupe TTL start time: the original claim.
    claimed_at: Instant,
}

struct TableIndex {
    in_progress: Mutex<HashMap<String, Instant>>,
    completed: Cache<String, CompletedEntry>,
}

impl TableIndex {
    fn new(ttl: Duration, max_keys: usize) -> Self {
        let mut completed = Cache::builder().max_capacity(max_keys as u64);
        if !ttl.is_zero() {
            completed = completed.time_to_live(ttl);
        }
        Self {
            in_progress: Mutex::new(HashMap::new()),
            completed: completed.build(),
        }
    }
}

/// Bounded, TTL'd idempotency-key index over all tables on this writer.
pub struct IdempotencyIndex {
    tables: PapayaHashMap<TableIdent, Arc<TableIndex>>,
    ttl: Duration,
    max_keys_per_table: usize,
}

impl IdempotencyIndex {
    pub fn new(ttl: Duration, max_keys_per_table: usize) -> Self {
        Self {
            tables: PapayaHashMap::new(),
            ttl,
            max_keys_per_table: max_keys_per_table.max(1),
        }
    }

    fn table_index(&self, table: &TableIdent) -> Arc<TableIndex> {
        self.tables
            .pin()
            .get_or_insert_with(table.clone(), || {
                Arc::new(TableIndex::new(self.ttl, self.max_keys_per_table))
            })
            .clone()
    }

    fn get_table_index(&self, table: &TableIdent) -> Option<Arc<TableIndex>> {
        self.tables.pin().get(table).cloned()
    }

    fn completed_duplicate(&self, index: &TableIndex, key: &str, now: Instant) -> Option<IngestReceipt> {
        let completed = index.completed.get(key)?;
        if now.duration_since(completed.claimed_at) <= self.ttl {
            return Some(completed.receipt);
        }
        index.completed.invalidate(key);
        None
    }

    /// Claim a key before the WAL append. Exactly one concurrent caller per
    /// key acquires it.
    pub fn claim(&self, table: &TableIdent, key: &str) -> Claim {
        let now = Instant::now();
        let index = self.table_index(table);

        if let Some(receipt) = self.completed_duplicate(&index, key, now) {
            return Claim::Duplicate(receipt);
        }

        let mut in_progress = index.in_progress.lock();
        if let Some(receipt) = self.completed_duplicate(&index, key, now) {
            return Claim::Duplicate(receipt);
        }
        if let Some(claimed_at) = in_progress.get(key).copied() {
            if now.duration_since(claimed_at) <= self.ttl {
                return Claim::InProgress;
            }
            in_progress.remove(key);
        }

        in_progress.insert(key.to_string(), now);
        Claim::Acquired
    }

    /// Mark an acquired key as completed with its receipt.
    pub fn complete(&self, table: &TableIdent, key: &str, receipt: IngestReceipt) {
        let Some(index) = self.get_table_index(table) else {
            return;
        };
        let mut in_progress = index.in_progress.lock();
        if let Some(claimed_at) = in_progress.remove(key) {
            index
                .completed
                .insert(key.to_string(), CompletedEntry { receipt, claimed_at });
            index.completed.run_pending_tasks();
        }
    }

    /// Release an acquired key after a failure so a retry can win it again.
    pub fn abort(&self, table: &TableIdent, key: &str) {
        if let Some(index) = self.get_table_index(table) {
            index.in_progress.lock().remove(key);
        }
    }

    /// Insert a completed key directly (WAL replay rebuild).
    pub fn record_completed(&self, table: &TableIdent, key: &str, receipt: IngestReceipt) {
        let now = Instant::now();
        let index = self.table_index(table);
        index.completed.insert(
            key.to_string(),
            CompletedEntry {
                receipt,
                claimed_at: now,
            },
        );
        index.completed.run_pending_tasks();
    }

    /// Forget every key for a table. Must be called when the table is
    /// dropped or recreated: receipts reference the old incarnation, and
    /// serving them would silently swallow ingests into the new one.
    pub fn evict_table(&self, table: &TableIdent) {
        self.tables.pin().remove(table);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn receipt(rows: u64) -> IngestReceipt {
        IngestReceipt {
            batch_id: uuid::Uuid::now_v7(),
            writer_id: teodb_core::write_protocol::WriterId::from_uuid(uuid::Uuid::from_u128(1)),
            generation: 1,
            accepted_rows: rows,
        }
    }

    fn table() -> TableIdent {
        TableIdent::new("ns", "t")
    }

    #[test]
    fn duplicate_returns_original_receipt() {
        let index = IdempotencyIndex::new(Duration::from_secs(60), 100);
        assert!(matches!(index.claim(&table(), "k"), Claim::Acquired));
        let original = receipt(42);
        let batch_id = original.batch_id;
        index.complete(&table(), "k", original);

        match index.claim(&table(), "k") {
            Claim::Duplicate(r) => {
                assert_eq!(r.batch_id, batch_id);
                assert_eq!(r.accepted_rows, 42);
            }
            other => panic!("expected Duplicate, got {other:?}"),
        }
    }

    #[test]
    fn in_flight_key_reports_in_progress() {
        let index = IdempotencyIndex::new(Duration::from_secs(60), 100);
        assert!(matches!(index.claim(&table(), "k"), Claim::Acquired));
        assert!(matches!(index.claim(&table(), "k"), Claim::InProgress));
    }

    #[test]
    fn abort_releases_the_key() {
        let index = IdempotencyIndex::new(Duration::from_secs(60), 100);
        assert!(matches!(index.claim(&table(), "k"), Claim::Acquired));
        index.abort(&table(), "k");
        assert!(matches!(index.claim(&table(), "k"), Claim::Acquired));
    }

    #[test]
    fn keys_are_scoped_per_table() {
        let index = IdempotencyIndex::new(Duration::from_secs(60), 100);
        let other = TableIdent::new("ns", "other");
        assert!(matches!(index.claim(&table(), "k"), Claim::Acquired));
        assert!(matches!(index.claim(&other, "k"), Claim::Acquired));
    }

    #[test]
    fn expired_key_can_be_reclaimed() {
        let index = IdempotencyIndex::new(Duration::ZERO, 100);
        assert!(matches!(index.claim(&table(), "k"), Claim::Acquired));
        index.complete(&table(), "k", receipt(1));
        std::thread::sleep(Duration::from_millis(5));
        assert!(matches!(index.claim(&table(), "k"), Claim::Acquired));
    }

    #[test]
    fn cap_evicts_oldest_completed_keys() {
        let index = IdempotencyIndex::new(Duration::from_secs(60), 2);
        for key in ["a", "b", "c"] {
            assert!(matches!(index.claim(&table(), key), Claim::Acquired));
            index.complete(&table(), key, receipt(1));
        }
        // "a" was evicted to stay within the cap; "c" is still known.
        assert!(matches!(index.claim(&table(), "a"), Claim::Acquired));
        assert!(matches!(index.claim(&table(), "c"), Claim::Duplicate(_)));
    }

    #[test]
    fn in_flight_claims_survive_cap_pressure() {
        let index = IdempotencyIndex::new(Duration::from_secs(60), 1);
        assert!(matches!(index.claim(&table(), "inflight"), Claim::Acquired));
        for key in ["a", "b"] {
            assert!(matches!(index.claim(&table(), key), Claim::Acquired));
            index.complete(&table(), key, receipt(1));
        }
        assert!(
            matches!(index.claim(&table(), "inflight"), Claim::InProgress),
            "in-flight claim must not be evicted by cap pressure"
        );
    }

    #[test]
    fn evict_table_forgets_all_keys() {
        let index = IdempotencyIndex::new(Duration::from_secs(60), 100);
        assert!(matches!(index.claim(&table(), "k"), Claim::Acquired));
        index.complete(&table(), "k", receipt(1));
        index.evict_table(&table());
        assert!(matches!(index.claim(&table(), "k"), Claim::Acquired));
    }

    #[test]
    fn concurrent_claims_have_exactly_one_winner() {
        let index = Arc::new(IdempotencyIndex::new(Duration::from_secs(60), 100));
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let index = index.clone();
                std::thread::spawn(move || matches!(index.claim(&table(), "k"), Claim::Acquired))
            })
            .collect();
        let winners = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .filter(|&won| won)
            .count();
        assert_eq!(winners, 1, "exactly one concurrent claim must win");
    }
}
