use std::sync::Arc;
use std::time::Duration;

use crate::config::TeoDBConfig;

pub(in crate::server) struct IngestComponents {
    pub(in crate::server) config: teodb_ingest::config::IngestConfig,
    pub(in crate::server) buffers: Arc<teodb_ingest::buffer::BufferRegistry>,
    pub(in crate::server) idempotency: Arc<teodb_ingest::idempotency::IdempotencyIndex>,
}

pub(in crate::server) fn build_ingest_components(
    cfg: &TeoDBConfig,
    wal: Arc<teodb_storage::wal::WalManager>,
) -> IngestComponents {
    let config = cfg.to_ingest_config();
    let committed_grace = Duration::from_secs(cfg.query.metadata_refresh_secs.saturating_mul(2));
    let buffers = Arc::new(
        teodb_ingest::buffer::BufferRegistry::new(wal, config.buffer_max_bytes, config.buffer_soft_watermark_bytes)
            .with_committed_grace(committed_grace),
    );
    let idempotency = Arc::new(teodb_ingest::idempotency::IdempotencyIndex::new(
        config.idempotency_ttl,
        config.idempotency_max_keys_per_table,
    ));

    IngestComponents {
        config,
        buffers,
        idempotency,
    }
}
