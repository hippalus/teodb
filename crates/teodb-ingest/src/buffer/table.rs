//! In-memory hot buffer for ingested rows with generation-based visibility.
//!
//! Each table has a `TableBuffer` that holds pending and in-flight entries.
//! The `BufferRegistry` manages per-table buffers lazily.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use parking_lot::RwLock;
use tokio::sync::{Mutex, MutexGuard};
use tracing::debug;

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::file::TableMetadata;
use teodb_core::ident::{Generation, TableIdent};
use teodb_storage::wal::PreparedFlush;

/// A single buffered entry (one ingested batch).
#[derive(Debug, Clone)]
pub struct BufferEntry {
    pub batch_id: uuid::Uuid,
    pub generation: Generation,
    pub created_at_ms: i64,
    pub batch: RecordBatch,
    pub byte_size: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferStats {
    pub pending_bytes: u64,
    pub in_flight_bytes: u64,
    pub recently_committed_bytes: u64,
    pub reserved_bytes: u64,
    pub pending_entries: usize,
    pub in_flight_entries: usize,
    pub oldest_uncommitted_created_at_ms: Option<i64>,
}

/// Result of a successful insert into the buffer.
#[derive(Debug)]
pub struct InsertOk {
    pub generation: Generation,
    /// Set when pending bytes exceed the soft watermark.
    pub backpressure_signal: bool,
}

/// Snapshot of buffer state captured atomically for query planning.
pub struct BufferSnapshot {
    pub committed_high_water: Generation,
    pub batches: Vec<BufferEntry>,
}

#[derive(Debug, Clone)]
pub struct BlockedFlush {
    pub prepared: PreparedFlush,
    pub since_ms: i64,
    pub last_error: String,
    pub last_error_class: String,
    pub status_check_attempts: u32,
    pub last_recheck_ms: i64,
}

#[derive(Debug, Clone)]
enum TableFlushState {
    Idle,
    Prepared(PreparedFlush),
    Blocked(BlockedFlush),
}

struct BufferState {
    next_gen: Generation,
    committed_high_water: Generation,
    pending: Vec<BufferEntry>,
    in_flight: Vec<BufferEntry>,
    /// Flushed-and-committed entries retained for a grace window so that
    /// queries planned against a bounded-stale snapshot (metadata TTL
    /// cache) still see them through the overlay. The overlay's
    /// per-entry generation cutoff makes this exact: a fresh snapshot
    /// excludes them (gen <= cutoff), a stale one includes them.
    recently_committed: Vec<(Instant, Vec<BufferEntry>)>,
    pending_bytes: u64,
    in_flight_bytes: u64,
    /// Bytes reserved by `reserve` but not yet inserted (the in-flight window
    /// between capacity reservation and the post-WAL `insert_reserved`). Counted
    /// against `max_bytes` so concurrent reservations cannot over-admit.
    reserved_bytes: u64,
    reserved_generations: BTreeMap<Generation, u64>,
    flush_state: TableFlushState,
}

/// A capacity+generation reservation taken before the WAL append so the
/// subsequent buffer insertion is infallible (invariant I1: a WAL-durable batch
/// is always admitted). Resolve it with `insert_reserved` after a successful WAL
/// append, or `release_reservation` if the WAL append fails.
#[derive(Debug)]
pub struct Reservation {
    pub generation: Generation,
    byte_size: u64,
}

/// Per-table hot buffer. Holds ingested rows until flushed to Parquet.
pub struct TableBuffer {
    ident: TableIdent,
    /// Serializes the complete flush state machine for this table. Keeping the
    /// lock on the buffer lifecycle avoids an unbounded global lock registry.
    flush_lock: Mutex<()>,
    state: RwLock<BufferState>,
    metadata: RwLock<Arc<TableMetadata>>,
    max_bytes: u64,
    soft_watermark_bytes: u64,
    /// How long committed entries stay visible to stale-snapshot readers
    /// (zero = drop immediately on commit, the pre-cache behavior).
    committed_grace: Duration,
}

impl TableBuffer {
    pub fn new(
        ident: TableIdent,
        metadata: Arc<TableMetadata>,
        committed_generation: Generation,
        max_bytes: u64,
        soft_watermark_bytes: u64,
    ) -> Self {
        Self {
            ident,
            flush_lock: Mutex::new(()),
            state: RwLock::new(BufferState {
                next_gen: committed_generation.saturating_add(1),
                committed_high_water: committed_generation,
                pending: Vec::new(),
                in_flight: Vec::new(),
                recently_committed: Vec::new(),
                pending_bytes: 0,
                in_flight_bytes: 0,
                reserved_bytes: 0,
                reserved_generations: BTreeMap::new(),
                flush_state: TableFlushState::Idle,
            }),
            metadata: RwLock::new(metadata),
            max_bytes,
            soft_watermark_bytes,
            committed_grace: Duration::ZERO,
        }
    }

    /// Retain committed entries for `grace` after flush so readers planning
    /// against a bounded-stale snapshot still see them (see `BufferState`).
    pub fn with_committed_grace(mut self, grace: Duration) -> Self {
        self.committed_grace = grace;
        self
    }

    #[inline]
    pub fn ident(&self) -> &TableIdent {
        &self.ident
    }

    #[inline]
    pub fn table_uuid(&self) -> uuid::Uuid {
        self.metadata.read().table_uuid
    }

    #[inline]
    pub fn metadata(&self) -> Arc<TableMetadata> {
        self.metadata.read().clone()
    }

    /// Acquire exclusive ownership of this table's flush state machine.
    pub async fn lock_flush(&self) -> MutexGuard<'_, ()> {
        self.flush_lock.lock().await
    }

    /// Insert a batch after WAL append. Allocates a generation number.
    pub fn insert(&self, batch_id: uuid::Uuid, batch: RecordBatch) -> TeoDBResult<InsertOk> {
        let byte_size = batch_byte_size(&batch);

        let mut state = self.state.write();
        self.ensure_capacity(&state, byte_size, false)?;
        self.ensure_generation_available(state.next_gen)?;

        let generation = state.next_gen;
        let inserted = self.admit_entry(
            &mut state,
            BufferEntry {
                batch_id,
                generation,
                created_at_ms: chrono::Utc::now().timestamp_millis(),
                batch,
                byte_size,
            },
        );

        debug!(
            table = %self.ident,
            generation,
            pending_entries = state.pending.len(),
            pending_bytes = state.pending_bytes,
            "buffer insert"
        );

        Ok(inserted)
    }

    /// Reserve buffer capacity and a generation number atomically, before WAL
    /// append.
    ///
    /// The returned reservation counts against `max_bytes` until it is resolved,
    /// so concurrent requests cannot all pass capacity checks and then fail
    /// after becoming WAL-durable.
    pub fn reserve(&self, batch: &RecordBatch) -> TeoDBResult<Reservation> {
        let byte_size = batch_byte_size(batch);
        let mut state = self.state.write();
        if let TableFlushState::Blocked(blocked) = &state.flush_state {
            return Err(TeoDBError::FlushBlocked {
                table: self.ident.clone(),
                commit_id: blocked.prepared.commit_id,
            });
        }
        self.ensure_capacity(&state, byte_size, true)?;
        self.ensure_generation_available(state.next_gen)?;

        let generation = state.next_gen;
        state.next_gen = state
            .next_gen
            .checked_add(1)
            .expect("generation availability was checked");
        state.reserved_bytes += byte_size;
        state
            .reserved_generations
            .insert(generation, byte_size);

        Ok(Reservation { generation, byte_size })
    }

    /// Release a reservation whose WAL append failed before it became durable.
    pub fn release_reservation(&self, reservation: Reservation) {
        let mut state = self.state.write();
        if let Some(byte_size) = state
            .reserved_generations
            .remove(&reservation.generation)
        {
            state.reserved_bytes = state.reserved_bytes.saturating_sub(byte_size);
        }
    }

    /// Insert a batch using a pre-WAL capacity+generation reservation.
    ///
    /// This method is intentionally infallible: once the WAL append succeeds,
    /// the reservation guarantees buffer admission.
    pub fn insert_reserved(&self, batch_id: uuid::Uuid, reservation: Reservation, batch: RecordBatch) -> InsertOk {
        self.insert_reserved_at(batch_id, reservation, chrono::Utc::now().timestamp_millis(), batch)
    }

    pub fn insert_reserved_at(
        &self,
        batch_id: uuid::Uuid,
        reservation: Reservation,
        created_at_ms: i64,
        batch: RecordBatch,
    ) -> InsertOk {
        let byte_size = batch_byte_size(&batch);
        debug_assert_eq!(
            byte_size, reservation.byte_size,
            "reserved batch size changed between reservation and insertion"
        );

        let mut state = self.state.write();
        if let Some(reserved) = state
            .reserved_generations
            .remove(&reservation.generation)
        {
            state.reserved_bytes = state.reserved_bytes.saturating_sub(reserved);
        }

        let inserted = self.admit_entry(
            &mut state,
            BufferEntry {
                batch_id,
                generation: reservation.generation,
                created_at_ms,
                batch,
                byte_size,
            },
        );

        debug!(
            table = %self.ident,
            generation = reservation.generation,
            pending_entries = state.pending.len(),
            pending_bytes = state.pending_bytes,
            "buffer insert (reserved)"
        );

        inserted
    }

    /// Insert a batch with an explicit generation, used by WAL replay.
    ///
    /// Use this after a successful WAL append: the generation was assigned
    /// before the WAL write, so on replay the WAL record and buffer entry
    /// will carry identical generations.
    pub fn insert_with_generation(
        &self,
        batch_id: uuid::Uuid,
        generation: Generation,
        batch: RecordBatch,
    ) -> TeoDBResult<InsertOk> {
        self.insert_with_generation_at(batch_id, generation, chrono::Utc::now().timestamp_millis(), batch)
    }

    pub fn insert_with_generation_at(
        &self,
        batch_id: uuid::Uuid,
        generation: Generation,
        created_at_ms: i64,
        batch: RecordBatch,
    ) -> TeoDBResult<InsertOk> {
        let byte_size = batch_byte_size(&batch);

        let mut state = self.state.write();
        self.ensure_capacity(&state, byte_size, false)?;
        self.ensure_generation_available(generation)?;
        let inserted = self.admit_entry(
            &mut state,
            BufferEntry {
                batch_id,
                generation,
                created_at_ms,
                batch,
                byte_size,
            },
        );

        debug!(
            table = %self.ident,
            generation,
            pending_entries = state.pending.len(),
            pending_bytes = state.pending_bytes,
            "buffer insert (pre-reserved gen)"
        );

        Ok(inserted)
    }

    fn ensure_capacity(&self, state: &BufferState, byte_size: u64, include_reserved: bool) -> TeoDBResult<()> {
        let total = state
            .pending_bytes
            .checked_add(state.in_flight_bytes)
            .and_then(|value| value.checked_add(state.reserved_bytes))
            .and_then(|value| value.checked_add(byte_size))
            .ok_or_else(|| TeoDBError::Backpressure(format!("buffer byte accounting overflow for {}", self.ident)))?;
        if total <= self.max_bytes {
            return Ok(());
        }

        if include_reserved {
            return Err(TeoDBError::Backpressure(format!(
                "buffer for {} would exceed {}B limit (currently {}B, reserved {}B)",
                self.ident,
                self.max_bytes,
                state.pending_bytes + state.in_flight_bytes,
                state.reserved_bytes,
            )));
        }

        Err(TeoDBError::Backpressure(format!(
            "buffer for {} would exceed {}B limit (currently {}B)",
            self.ident,
            self.max_bytes,
            state.pending_bytes + state.in_flight_bytes,
        )))
    }

    fn ensure_generation_available(&self, generation: Generation) -> TeoDBResult<()> {
        if generation == 0 || generation == Generation::MAX {
            return Err(TeoDBError::WriteProtocol {
                table: self.ident.clone(),
                message: "writer-local generation space is exhausted".into(),
            });
        }
        Ok(())
    }

    fn admit_entry(&self, state: &mut BufferState, entry: BufferEntry) -> InsertOk {
        let generation = entry.generation;
        let byte_size = entry.byte_size;

        state.pending.push(entry);
        state.pending_bytes += byte_size;
        state.next_gen = state.next_gen.max(
            generation
                .checked_add(1)
                .expect("generation was validated"),
        );

        InsertOk {
            generation,
            backpressure_signal: state.pending_bytes + state.in_flight_bytes > self.soft_watermark_bytes,
        }
    }

    /// Atomic snapshot of all visible entries for query planning.
    pub fn snapshot_for_query(&self) -> BufferSnapshot {
        let state = self.state.read();
        let mut batches = Vec::with_capacity(state.pending.len() + state.in_flight.len());
        batches.extend(state.pending.iter().cloned());
        batches.extend(state.in_flight.iter().cloned());
        for (committed_at, entries) in &state.recently_committed {
            if committed_at.elapsed() <= self.committed_grace {
                batches.extend(entries.iter().cloned());
            }
        }

        BufferSnapshot {
            committed_high_water: state.committed_high_water,
            batches,
        }
    }

    /// Move pending → in-flight for the flusher. Returns the set to flush.
    pub fn drain_pending_to_in_flight(&self) -> Vec<BufferEntry> {
        let mut state = self.state.write();

        if !state.in_flight.is_empty() {
            // Already have an in-flight set (retry scenario). Return a clone
            // since the originals must stay for retry tracking.
            return state.in_flight.clone();
        }

        if state.pending.is_empty() {
            return Vec::new();
        }

        let lowest_reserved = state.reserved_generations.keys().next().copied();

        // Move out of pending via drain — no clone on the source entries. Do
        // not flush later generations past a still-reserved earlier generation,
        // otherwise `committed_high_water` could skip a WAL append that is still
        // in progress.
        let mut drained = Vec::new();
        let mut remaining = Vec::new();
        for entry in state.pending.drain(..) {
            if lowest_reserved.is_none_or(|reserved| entry.generation < reserved) {
                drained.push(entry);
            } else {
                remaining.push(entry);
            }
        }

        if drained.is_empty() {
            state.pending = remaining;
            state.pending_bytes = state.pending.iter().map(|e| e.byte_size).sum();
            return Vec::new();
        }

        let bytes: u64 = drained.iter().map(|e| e.byte_size).sum();
        let remaining_bytes: u64 = remaining.iter().map(|e| e.byte_size).sum();

        // One clone is required: we keep a copy in in_flight for failure
        // recovery (mark_flush_failed) and return ownership to the caller.
        // RecordBatch is Arc-based, so this is cheap (ref-count bumps).
        state.pending = remaining;
        state.in_flight = drained.clone();
        state.in_flight_bytes = bytes;
        state.pending_bytes = remaining_bytes;

        debug!(
            table = %self.ident,
            in_flight_entries = state.in_flight.len(),
            in_flight_bytes = state.in_flight_bytes,
            "drained pending to in-flight"
        );

        drained
    }

    pub fn set_prepared(&self, prepared: PreparedFlush) -> TeoDBResult<()> {
        let mut state = self.state.write();
        let actual_lo = state
            .in_flight
            .iter()
            .map(|entry| entry.generation)
            .min();
        let actual_hi = state
            .in_flight
            .iter()
            .map(|entry| entry.generation)
            .max();
        if actual_lo != Some(prepared.generations.lo)
            || actual_hi != Some(prepared.generations.hi)
            || prepared.table_uuid != self.metadata.read().table_uuid
        {
            return Err(TeoDBError::WriteProtocol {
                table: self.ident.clone(),
                message: format!(
                    "prepared range {}-{} does not match in-flight range {:?}-{:?}",
                    prepared.generations.lo, prepared.generations.hi, actual_lo, actual_hi
                ),
            });
        }
        match &state.flush_state {
            TableFlushState::Idle => state.flush_state = TableFlushState::Prepared(prepared),
            TableFlushState::Prepared(existing) if existing == &prepared => {}
            TableFlushState::Blocked(existing) if existing.prepared == prepared => {}
            _ => {
                return Err(TeoDBError::WriteProtocol {
                    table: self.ident.clone(),
                    message: "a different prepared flush already owns this table".into(),
                });
            }
        }
        Ok(())
    }

    /// Restore a durable prepared sidecar during startup. WAL replay first
    /// places records in `pending`; this method reclaims exactly the sidecar's
    /// immutable generation range as `in_flight` without pulling later
    /// generations into the ambiguous attempt.
    pub fn restore_prepared(&self, prepared: PreparedFlush) -> TeoDBResult<()> {
        let mut state = self.state.write();
        if !state.in_flight.is_empty() {
            return Err(TeoDBError::WriteProtocol {
                table: self.ident.clone(),
                message: "cannot restore prepared intent over existing in-flight entries".into(),
            });
        }

        if prepared.generations.hi <= state.committed_high_water {
            state.flush_state = TableFlushState::Prepared(prepared);
            return Ok(());
        }

        let mut restored = Vec::new();
        let mut remaining = Vec::new();
        for entry in state.pending.drain(..) {
            if prepared.generations.contains(entry.generation) {
                restored.push(entry);
            } else {
                remaining.push(entry);
            }
        }
        let actual_lo = restored
            .iter()
            .map(|entry| entry.generation)
            .min();
        let actual_hi = restored
            .iter()
            .map(|entry| entry.generation)
            .max();
        if actual_lo != Some(prepared.generations.lo) || actual_hi != Some(prepared.generations.hi) {
            state.pending.extend(restored);
            state.pending.extend(remaining);
            state
                .pending
                .sort_by_key(|entry| entry.generation);
            state.pending_bytes = state
                .pending
                .iter()
                .map(|entry| entry.byte_size)
                .sum();
            return Err(TeoDBError::WriteProtocol {
                table: self.ident.clone(),
                message: format!(
                    "prepared sidecar range {}-{} is not fully represented in WAL (found {:?}-{:?})",
                    prepared.generations.lo, prepared.generations.hi, actual_lo, actual_hi
                ),
            });
        }

        state.in_flight_bytes = restored.iter().map(|entry| entry.byte_size).sum();
        state.pending_bytes = remaining
            .iter()
            .map(|entry| entry.byte_size)
            .sum();
        state.in_flight = restored;
        state.pending = remaining;
        state.flush_state = TableFlushState::Prepared(prepared);
        Ok(())
    }

    pub fn prepared_flush(&self) -> Option<PreparedFlush> {
        match &self.state.read().flush_state {
            TableFlushState::Prepared(prepared) => Some(prepared.clone()),
            TableFlushState::Blocked(blocked) => Some(blocked.prepared.clone()),
            TableFlushState::Idle => None,
        }
    }

    pub fn blocked_flush(&self) -> Option<BlockedFlush> {
        match &self.state.read().flush_state {
            TableFlushState::Blocked(blocked) => Some(blocked.clone()),
            _ => None,
        }
    }

    pub fn mark_flush_blocked(
        &self,
        prepared: &PreparedFlush,
        last_error: String,
        status_check_attempts: u32,
    ) -> TeoDBResult<()> {
        self.mark_flush_blocked_with_class(prepared, last_error, "commit_status_unknown", status_check_attempts)
    }

    pub fn mark_flush_blocked_with_class(
        &self,
        prepared: &PreparedFlush,
        last_error: String,
        last_error_class: impl Into<String>,
        status_check_attempts: u32,
    ) -> TeoDBResult<()> {
        let now = chrono::Utc::now().timestamp_millis();
        let last_error_class = last_error_class.into();
        let mut state = self.state.write();
        match &state.flush_state {
            TableFlushState::Prepared(existing) if existing == prepared => {
                state.flush_state = TableFlushState::Blocked(BlockedFlush {
                    prepared: prepared.clone(),
                    since_ms: now,
                    last_error,
                    last_error_class,
                    status_check_attempts,
                    last_recheck_ms: now,
                });
                Ok(())
            }
            TableFlushState::Blocked(existing) if existing.prepared == *prepared => {
                let since_ms = existing.since_ms;
                state.flush_state = TableFlushState::Blocked(BlockedFlush {
                    prepared: prepared.clone(),
                    since_ms,
                    last_error,
                    last_error_class,
                    status_check_attempts,
                    last_recheck_ms: now,
                });
                Ok(())
            }
            _ => Err(TeoDBError::WriteProtocol {
                table: self.ident.clone(),
                message: "cannot block a flush that does not own prepared state".into(),
            }),
        }
    }

    pub fn mark_blocked_recheck(&self, last_error: String, attempts: u32) {
        let mut state = self.state.write();
        if let TableFlushState::Blocked(blocked) = &mut state.flush_state {
            blocked.last_error = last_error;
            blocked.last_error_class = "commit_status_unknown".into();
            blocked.status_check_attempts = attempts;
            blocked.last_recheck_ms = chrono::Utc::now().timestamp_millis();
        }
    }

    /// Mark in-flight entries as committed (flush succeeded).
    pub fn mark_committed(&self, gen_hi: Generation, new_metadata: Arc<TableMetadata>) -> TeoDBResult<()> {
        let mut state = self.state.write();
        let actual_hi = state
            .in_flight
            .iter()
            .map(|entry| entry.generation)
            .max();
        if let Some(actual_hi) = actual_hi
            && actual_hi != gen_hi
        {
            return Err(TeoDBError::WriteProtocol {
                table: self.ident.clone(),
                message: format!(
                    "flush completion generation {gen_hi} does not match in-flight high generation {actual_hi}"
                ),
            });
        }
        if actual_hi.is_none() && gen_hi > state.committed_high_water {
            return Err(TeoDBError::WriteProtocol {
                table: self.ident.clone(),
                message: format!("flush completion for generation {gen_hi} has no matching in-flight entries"),
            });
        }

        let committed = std::mem::take(&mut state.in_flight);
        if !self.committed_grace.is_zero() && !committed.is_empty() {
            state
                .recently_committed
                .push((Instant::now(), committed));
        }
        let grace = self.committed_grace;
        state
            .recently_committed
            .retain(|(at, _)| at.elapsed() <= grace);
        state.in_flight_bytes = 0;
        state.committed_high_water = state.committed_high_water.max(gen_hi);
        state.flush_state = TableFlushState::Idle;
        drop(state);

        *self.metadata.write() = new_metadata;

        debug!(table = %self.ident, gen_hi, "buffer committed");
        Ok(())
    }

    /// Roll back work that failed before a prepared owner was installed.
    pub fn rollback_unprepared_flush(&self) -> TeoDBResult<()> {
        let mut state = self.state.write();
        if !matches!(state.flush_state, TableFlushState::Idle) {
            return Err(TeoDBError::WriteProtocol {
                table: self.ident.clone(),
                message: "cannot roll back an unprepared flush while a prepared owner exists".into(),
            });
        }
        Self::merge_in_flight_back_to_pending(&mut state);

        debug!(
            table = %self.ident,
            pending_entries = state.pending.len(),
            "unprepared flush rolled back to pending"
        );
        Ok(())
    }

    /// Roll back an exact prepared or blocked owner after publication was
    /// definitively rejected and its durable sidecar was removed.
    pub fn mark_flush_failed(&self, prepared: &PreparedFlush) -> TeoDBResult<()> {
        let mut state = self.state.write();
        let owns_state = match &state.flush_state {
            TableFlushState::Prepared(existing) => existing == prepared,
            TableFlushState::Blocked(existing) => existing.prepared == *prepared,
            TableFlushState::Idle => false,
        };
        if !owns_state {
            return Err(TeoDBError::WriteProtocol {
                table: self.ident.clone(),
                message: "cannot fail a flush that does not own prepared state".into(),
            });
        }
        Self::merge_in_flight_back_to_pending(&mut state);
        state.flush_state = TableFlushState::Idle;

        debug!(
            table = %self.ident,
            commit_id = %prepared.commit_id,
            pending_entries = state.pending.len(),
            "prepared flush failed, merged in-flight back to pending"
        );
        Ok(())
    }

    fn merge_in_flight_back_to_pending(state: &mut BufferState) {
        let mut merged = std::mem::take(&mut state.in_flight);
        merged.append(&mut state.pending);
        state.pending = merged;
        state.pending_bytes += state.in_flight_bytes;
        state.in_flight_bytes = 0;
    }

    /// Replace the cached metadata with a fresh copy (e.g. after a snapshot conflict).
    pub fn refresh_metadata(&self, new_metadata: Arc<TableMetadata>) {
        *self.metadata.write() = new_metadata;
    }

    /// Returns the committed high-water generation.
    #[inline]
    pub fn committed_high_water(&self) -> Generation {
        self.state.read().committed_high_water
    }

    /// Returns true if there are pending or in-flight entries.
    #[inline]
    pub fn has_pending(&self) -> bool {
        let state = self.state.read();
        !state.pending.is_empty() || !state.in_flight.is_empty() || !matches!(state.flush_state, TableFlushState::Idle)
    }

    pub fn buffer_stats(&self) -> BufferStats {
        let state = self.state.read();
        BufferStats {
            pending_bytes: state.pending_bytes,
            in_flight_bytes: state.in_flight_bytes,
            recently_committed_bytes: state
                .recently_committed
                .iter()
                .flat_map(|(_, entries)| entries)
                .map(|entry| entry.byte_size)
                .sum(),
            reserved_bytes: state.reserved_bytes,
            pending_entries: state.pending.len(),
            in_flight_entries: state.in_flight.len(),
            oldest_uncommitted_created_at_ms: state
                .pending
                .iter()
                .chain(state.in_flight.iter())
                .map(|entry| entry.created_at_ms)
                .min(),
        }
    }

    /// Stats over all unflushed (pending + in-flight) entries.
    pub(super) fn unflushed_stats(&self) -> super::EvictionStats {
        let state = self.state.read();
        let rows = state
            .pending
            .iter()
            .chain(state.in_flight.iter())
            .map(|e| e.batch.num_rows() as u64)
            .sum();
        super::EvictionStats {
            rows,
            entries: state.pending.len() + state.in_flight.len(),
            bytes: state.pending_bytes + state.in_flight_bytes,
        }
    }
}

fn batch_byte_size(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|c| c.get_buffer_memory_size() as u64)
        .sum()
}
