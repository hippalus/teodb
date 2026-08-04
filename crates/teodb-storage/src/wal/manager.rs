//! Write-ahead log ownership and lifecycle.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicU64;

use ahash::AHashMap;
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::{Generation, TableIdent};
use teodb_core::write_protocol::WalTableKey;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::{info, warn};

use super::replay::{self, WalRecoveryMode};
use super::segment::{self, WalRecord};
use super::{gc, identity::WalIdentity, writer};

/// Configuration for the WAL subsystem.
#[derive(Debug, Clone)]
pub struct WalConfig {
    pub root_dir: PathBuf,
    /// Maximum segment size before rotation (default 256 MiB).
    pub max_segment_bytes: u64,
    /// Whether to call fsync after every append (must be `true` in production).
    pub fsync_on_append: bool,
    /// Soft watermark: when WAL directory usage exceeds this, return a backpressure hint (default 4 GiB).
    pub soft_watermark_bytes: u64,
    /// Hard cap: when WAL directory usage exceeds this, reject writes (default 8 GiB).
    pub hard_cap_bytes: u64,
    /// How replay responds to a structurally corrupt segment (default: fail).
    pub recovery_mode: WalRecoveryMode,
    /// Stable identity expected for this WAL root. Standalone/test callers may
    /// leave it empty and let the first boot persist generated values.
    pub identity: super::WalIdentityConfig,
    pub max_prepared_files: usize,
    pub max_prepared_bytes: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            root_dir: PathBuf::from("/tmp/teodb-wal"),
            max_segment_bytes: 256 * 1024 * 1024,
            fsync_on_append: true,
            soft_watermark_bytes: 4 * 1024 * 1024 * 1024, // 4 GiB
            hard_cap_bytes: 8 * 1024 * 1024 * 1024,       // 8 GiB
            recovery_mode: WalRecoveryMode::Fail,
            identity: super::WalIdentityConfig::default(),
            max_prepared_files: super::prepared::default_max_files(),
            max_prepared_bytes: super::prepared::default_max_bytes(),
        }
    }
}

/// The WAL manager. Owns the WAL directory, handles append, replay, and GC.
///
/// All writes are funneled through a dedicated writer task (see `writer.rs`)
/// that owns the open segment file, so appenders never hold a lock across
/// file I/O or fsync.
pub struct WalManager {
    pub(super) cfg: WalConfig,
    lease_file: std::fs::File,
    identity: Arc<StdMutex<WalIdentity>>,
    pub(super) committed: Arc<Mutex<AHashMap<WalTableKey, Generation>>>,
    checkpoint_persist_lock: Arc<Mutex<()>>,
    pub(super) writer_tx: mpsc::Sender<writer::WriterCommand>,
    /// Open segment's `seq + 1`, or 0 when none (published by the writer task
    /// so `gc()` never deletes the segment currently being written).
    pub(super) current_seq: Arc<AtomicU64>,
}

impl WalManager {
    /// Open or create the WAL directory. Acquires an exclusive advisory lock
    /// on `{root}/.lease` to prevent concurrent access. Loads persisted
    /// committed-generation checkpoint if it exists.
    pub async fn open(cfg: WalConfig) -> TeoDBResult<Self> {
        tokio::fs::create_dir_all(&cfg.root_dir)
            .await
            .map_err(|e| TeoDBError::wal(format!("failed to create WAL dir: {e}")))?;

        let lease_path = cfg.root_dir.join(".lease");
        let lease_file = Self::acquire_lease(&lease_path).await?;

        let identity_root = cfg.root_dir.clone();
        let identity_config = cfg.identity.clone();
        let identity = tokio::task::spawn_blocking(move || WalIdentity::open(&identity_root, &identity_config))
            .await
            .map_err(|error| TeoDBError::wal(format!("writer identity task failed: {error}")))??;

        let next_seq = Self::scan_next_seq(&cfg.root_dir).await?;

        let committed_generations = Self::load_committed_checkpoint(&cfg.root_dir).await?;
        if !committed_generations.is_empty() {
            info!(
                root = %cfg.root_dir.display(),
                tables = committed_generations.len(),
                "loaded committed-generation checkpoint"
            );
        }

        info!(root = %cfg.root_dir.display(), next_seq, "WAL opened");

        let current_seq = Arc::new(AtomicU64::new(0));
        let writer_tx = writer::SegmentWriter::spawn(
            cfg.root_dir.clone(),
            cfg.max_segment_bytes,
            cfg.fsync_on_append,
            next_seq,
            current_seq.clone(),
        );

        Ok(Self {
            cfg,
            lease_file,
            identity: Arc::new(StdMutex::new(identity)),
            committed: Arc::new(Mutex::new(committed_generations)),
            checkpoint_persist_lock: Arc::new(Mutex::new(())),
            writer_tx,
            current_seq,
        })
    }

    /// Stable writer identity bound to this WAL root.
    pub fn writer_identity(&self) -> teodb_core::write_protocol::ResolvedIdentity {
        self.identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .resolved()
    }

    /// Reconcile the local epoch with an own-writer catalog checkpoint before
    /// write admission opens.
    pub fn observe_epoch_and_bump(
        &self,
        observed: teodb_core::write_protocol::WriterEpoch,
    ) -> TeoDBResult<teodb_core::write_protocol::ResolvedIdentity> {
        self.identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .observe_epoch_and_bump(observed)
    }

    /// Durably persist an immutable prepared flush before the first catalog
    /// request is allowed to start.
    pub async fn persist_prepared(&self, prepared: &super::PreparedFlush) -> TeoDBResult<()> {
        let root = self.cfg.root_dir.clone();
        let prepared = prepared.clone();
        let writer_id = self.writer_identity().writer_id;
        let max_files = self.cfg.max_prepared_files;
        let max_bytes = self.cfg.max_prepared_bytes;
        tokio::task::spawn_blocking(move || super::prepared::persist(&root, &prepared, writer_id, max_files, max_bytes))
            .await
            .map_err(|error| TeoDBError::wal(format!("persist prepared task failed: {error}")))?
    }

    pub async fn list_prepared(&self) -> TeoDBResult<Vec<super::PreparedFlush>> {
        let root = self.cfg.root_dir.clone();
        let writer_id = self.writer_identity().writer_id;
        let max_files = self.cfg.max_prepared_files;
        let max_bytes = self.cfg.max_prepared_bytes;
        tokio::task::spawn_blocking(move || super::prepared::list(&root, writer_id, max_files, max_bytes))
            .await
            .map_err(|error| TeoDBError::wal(format!("list prepared task failed: {error}")))?
    }

    pub async fn remove_prepared(&self, table_uuid: uuid::Uuid) -> TeoDBResult<()> {
        let root = self.cfg.root_dir.clone();
        tokio::task::spawn_blocking(move || super::prepared::remove(&root, table_uuid))
            .await
            .map_err(|error| TeoDBError::wal(format!("remove prepared task failed: {error}")))?
    }

    /// Append a WAL record. Returns `Ok(())` once the record is durable
    /// (written and, when `fsync_on_append` is set, fsynced).
    pub async fn append(&self, record: &WalRecord) -> TeoDBResult<()> {
        let frame = segment::encode_frame(record)?;
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(writer::WriterCommand::Write(writer::WriteRequest {
                frame,
                ack: ack_tx,
            }))
            .await
            .map_err(|_| TeoDBError::wal("WAL writer task stopped"))?;
        ack_rx
            .await
            .map_err(|_| TeoDBError::wal("WAL writer dropped ack channel"))?
    }

    /// Close the current segment so the next append opens a fresh one,
    /// making the closed segment eligible for GC once fully committed.
    pub async fn rotate(&self) -> TeoDBResult<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.writer_tx
            .send(writer::WriterCommand::Rotate(done_tx))
            .await
            .map_err(|_| TeoDBError::wal("WAL writer task stopped"))?;
        done_rx
            .await
            .map_err(|_| TeoDBError::wal("WAL writer dropped rotate ack"))
    }

    /// Mark all records up to `generation` as committed for a table.
    /// Persists the checkpoint to disk for crash recovery.
    pub async fn mark_committed(&self, table: WalTableKey, generation: Generation) {
        {
            let mut committed = self.committed.lock().await;
            committed
                .entry(table)
                .and_modify(|g| *g = (*g).max(generation))
                .or_insert(generation);
        }
        // Serialize checkpoint snapshots and writes. The snapshot is taken
        // only after acquiring this lock so a delayed older caller can never
        // overwrite a newer durable cutoff.
        let _persist_guard = self.checkpoint_persist_lock.lock().await;
        let committed_snapshot = self.committed.lock().await.clone();
        // Best-effort persist — WAL replay correctness doesn't depend on this
        // because the catalog is the authoritative source. This checkpoint
        // accelerates GC by avoiding segment re-scans.
        if let Err(e) = Self::persist_committed_checkpoint(&self.cfg.root_dir, &committed_snapshot).await {
            warn!(error = %e, "failed to persist committed-generation checkpoint (non-fatal)");
        }
    }

    /// Overwrite the committed cutoff for a table with the catalog-derived
    /// value. Unlike `mark_committed` (which only advances), seeding replaces
    /// a stale checkpoint entry left behind by a dropped incarnation of the
    /// same table name — a recreated table restarts generations at 1, so a
    /// stale high cutoff would silently mark its fresh, unflushed records as
    /// committed. The catalog is authoritative at startup, so overwriting is
    /// always safe for tables it knows about.
    pub async fn seed_committed(&self, table: WalTableKey, generation: Generation) {
        self.committed
            .lock()
            .await
            .insert(table, generation);
    }

    /// Durably record that a table was dropped. Appends a `DropTable`
    /// tombstone frame (voiding every earlier WAL record for the table on
    /// replay) and forgets the table's committed cutoff so a recreated
    /// incarnation starts from a clean slate.
    pub async fn append_drop_tombstone(&self, table: &TableIdent) -> TeoDBResult<()> {
        let record = WalRecord::drop_tombstone(table.clone());
        self.append(&record).await?;

        {
            let mut committed = self.committed.lock().await;
            committed.retain(|key, _| &key.ident != table);
        }
        let _persist_guard = self.checkpoint_persist_lock.lock().await;
        let committed_snapshot = self.committed.lock().await.clone();
        if let Err(e) = Self::persist_committed_checkpoint(&self.cfg.root_dir, &committed_snapshot).await {
            warn!(
                table = %table,
                error = %e,
                "failed to persist checkpoint after drop tombstone (non-fatal: \
                 startup seeding from the catalog corrects stale entries)"
            );
        }
        info!(table = %table, "WAL drop tombstone appended");
        Ok(())
    }

    /// Returns the committed generation for a table, or `None` if unknown.
    pub async fn committed_generation(&self, table: &WalTableKey) -> Option<Generation> {
        self.committed.lock().await.get(table).copied()
    }

    /// Validate a stable WAL snapshot and prepare bounded incremental replay
    /// with the local committed-generation cache applied.
    pub async fn prepare_replay(&self) -> TeoDBResult<replay::ReplayPlan> {
        let committed = self.committed.lock().await.clone();
        replay::WalReplay::new(&self.cfg.root_dir, &committed, self.cfg.recovery_mode)
            .prepare()
            .await
    }

    /// Validate a stable WAL snapshot and prepare bounded incremental replay
    /// without applying the local committed-generation cache.
    pub async fn prepare_replay_all(&self) -> TeoDBResult<replay::ReplayPlan> {
        let committed = AHashMap::new();
        replay::WalReplay::new(&self.cfg.root_dir, &committed, self.cfg.recovery_mode)
            .prepare()
            .await
    }

    /// Delete segments whose every frame is dead. Returns the number of
    /// deleted segments.
    ///
    /// A frame is dead when it is committed, or voided by a later drop
    /// tombstone. A tombstone frame is dead only once no earlier *retained*
    /// segment still holds records for its table — deleting it sooner would
    /// let those records resurrect on the next replay. Segments are processed
    /// in append order so that, within one pass, a table's voided records are
    /// removed before the tombstone that voids them; a crash mid-pass can
    /// only leave a tombstone with nothing left to void.
    pub async fn gc(&self) -> TeoDBResult<u64> {
        gc::WalGc::new(self).collect_garbage().await
    }

    /// Check WAL directory disk usage. Returns `Ok(true)` if under soft watermark,
    /// `Ok(false)` if above soft watermark but under hard cap (backpressure hint).
    /// Returns `Err(Wal)` if above hard cap.
    pub async fn check_capacity(&self) -> TeoDBResult<bool> {
        let used = self.disk_usage_bytes().await?;
        if used >= self.cfg.hard_cap_bytes {
            return Err(TeoDBError::wal(format!(
                "WAL capacity exceeded: {used} >= {} bytes",
                self.cfg.hard_cap_bytes
            )));
        }
        Ok(used < self.cfg.soft_watermark_bytes)
    }

    pub async fn disk_usage_bytes(&self) -> TeoDBResult<u64> {
        let mut total = 0u64;
        let mut pending_directories = vec![self.cfg.root_dir.clone()];
        while let Some(directory) = pending_directories.pop() {
            let mut entries = tokio::fs::read_dir(&directory)
                .await
                .map_err(|error| {
                    TeoDBError::wal_source(format!("read WAL directory {}", directory.display()), error)
                })?;
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|error| TeoDBError::wal_source("read WAL directory entry", error))?
            {
                let file_type = entry
                    .file_type()
                    .await
                    .map_err(|error| TeoDBError::wal_source("inspect WAL directory entry", error))?;
                if file_type.is_dir() {
                    pending_directories.push(entry.path());
                } else if file_type.is_file() {
                    let bytes = entry
                        .metadata()
                        .await
                        .map_err(|error| TeoDBError::wal_source("stat WAL file", error))?
                        .len();
                    total = total
                        .checked_add(bytes)
                        .ok_or_else(|| TeoDBError::wal("WAL disk usage exceeds u64"))?;
                }
            }
        }
        Ok(total)
    }

    /// Release the exclusive WAL lock. Should be called on graceful shutdown;
    /// dropping `WalManager` also releases the OS lock.
    pub async fn release_lease(&self) {
        let lease_path = self.cfg.root_dir.join(".lease");
        if let Err(e) = Self::unlock_lease(&self.lease_file) {
            warn!(path = %lease_path.display(), error = %e, "failed to unlock WAL lease file");
            return;
        }
        if let Err(e) = tokio::fs::remove_file(&lease_path).await {
            warn!(path = %lease_path.display(), error = %e, "failed to remove WAL lease file");
        } else {
            info!("WAL lease released");
        }
    }

    /// Count the number of WAL segment files (for health checks).
    pub fn segment_count(&self) -> TeoDBResult<usize> {
        let entries =
            std::fs::read_dir(&self.cfg.root_dir).map_err(|e| TeoDBError::wal(format!("cannot read WAL dir: {e}")))?;
        let count = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
            .count();
        Ok(count)
    }
}
