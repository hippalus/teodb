use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use papaya::HashMap as PapayaHashMap;
use tracing::{debug, warn};

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::TableIdent;

use super::TableBuffer;

/// Unflushed data discarded when a buffer was evicted.
#[derive(Debug, Clone, Copy, Default)]
pub struct EvictionStats {
    pub rows: u64,
    pub entries: usize,
    pub bytes: u64,
}

/// Registry managing per-table buffers with lock-free concurrent access.
pub struct BufferRegistry {
    inner: PapayaHashMap<TableIdent, Arc<TableBuffer>>,
    wal: Arc<teodb_storage::wal::WalManager>,
    max_bytes: u64,
    soft_watermark_bytes: u64,
    committed_grace: Duration,
    evicted_rows: AtomicU64,
}

impl BufferRegistry {
    pub fn new(wal: Arc<teodb_storage::wal::WalManager>, max_bytes: u64, soft_watermark_bytes: u64) -> Self {
        Self {
            inner: PapayaHashMap::new(),
            wal,
            max_bytes,
            soft_watermark_bytes,
            committed_grace: Duration::ZERO,
            evicted_rows: AtomicU64::new(0),
        }
    }

    /// Retain committed entries in every table buffer for `grace` after
    /// flush (see `TableBuffer::with_committed_grace`).
    pub fn with_committed_grace(mut self, grace: Duration) -> Self {
        self.committed_grace = grace;
        self
    }

    /// Get or lazily create a buffer for the given table, loading metadata
    /// from the catalog if needed.
    pub async fn get_or_load(
        &self,
        ident: &TableIdent,
        catalog: &dyn teodb_core::traits::catalog::Catalog,
    ) -> TeoDBResult<Arc<TableBuffer>> {
        if let Some(buffer) = self.inner.pin().get(ident) {
            return Ok(buffer.clone());
        }

        let metadata = retry_load_table(catalog, ident).await?;
        teodb_core::write_protocol::validate_writer_checkpoints(ident, &metadata.properties)?;
        let identity = self.wal.writer_identity();
        let checkpoint =
            teodb_core::write_protocol::parse_writer_checkpoint(ident, &metadata.properties, identity.writer_id)?;
        let committed_generation = if let Some(checkpoint) = checkpoint {
            self.wal
                .observe_epoch_and_bump(checkpoint.epoch)?;
            self.wal
                .seed_committed(
                    teodb_core::write_protocol::WalTableKey::new(metadata.table_uuid, ident.clone()),
                    checkpoint.generation,
                )
                .await;
            checkpoint.generation
        } else {
            self.wal
                .seed_committed(
                    teodb_core::write_protocol::WalTableKey::new(metadata.table_uuid, ident.clone()),
                    0,
                )
                .await;
            0
        };
        let map = self.inner.pin();
        Ok(map
            .get_or_insert_with(ident.clone(), || {
                Arc::new(
                    TableBuffer::new(
                        ident.clone(),
                        metadata,
                        committed_generation,
                        self.max_bytes,
                        self.soft_watermark_bytes,
                    )
                    .with_committed_grace(self.committed_grace),
                )
            })
            .clone())
    }

    #[inline]
    pub fn get(&self, ident: &TableIdent) -> Option<Arc<TableBuffer>> {
        self.inner.pin().get(ident).cloned()
    }

    /// Evict a table's buffer, returning statistics for discarded data.
    pub fn remove(&self, ident: &TableIdent) -> Option<EvictionStats> {
        let map = self.inner.pin();
        let buffer = map.remove(ident)?.clone();
        let stats = buffer.unflushed_stats();
        if stats.rows > 0 {
            self.evicted_rows
                .fetch_add(stats.rows, Ordering::Relaxed);
            warn!(
                table = %ident,
                rows = stats.rows,
                entries = stats.entries,
                bytes = stats.bytes,
                "buffer evicted with unflushed rows"
            );
        }
        Some(stats)
    }

    #[inline]
    pub fn evicted_rows_total(&self) -> u64 {
        self.evicted_rows.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn insert_for_test(&self, ident: TableIdent, buffer: Arc<TableBuffer>) {
        self.inner.pin().insert(ident, buffer);
    }

    pub fn tables(&self) -> Vec<TableIdent> {
        self.inner
            .pin()
            .iter()
            .map(|(ident, _)| ident.clone())
            .collect()
    }

    #[inline]
    pub fn table_count(&self) -> usize {
        self.inner.len()
    }
}

async fn retry_load_table(
    catalog: &dyn teodb_core::traits::catalog::Catalog,
    ident: &TableIdent,
) -> TeoDBResult<Arc<teodb_core::file::TableMetadata>> {
    const MAX_RETRIES: usize = 3;
    let mut attempt = 0usize;

    (|| async { catalog.load_table(ident).await })
        .retry(
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(100))
                .with_max_times(MAX_RETRIES),
        )
        .when(|error| matches!(error, TeoDBError::Catalog(_)))
        .notify(|_error, _backoff| {
            debug!(table = %ident, attempt, "catalog load failed; retrying");
            attempt += 1;
        })
        .await
}
