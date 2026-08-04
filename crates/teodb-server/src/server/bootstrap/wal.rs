use std::sync::Arc;

use tracing::info;

use crate::config::TeoDBConfig;

pub(in crate::server) async fn open_wal(cfg: &TeoDBConfig) -> eyre::Result<Arc<teodb_storage::wal::WalManager>> {
    let wal_dir = cfg.wal_dir();
    let wal_cfg = teodb_storage::wal::WalConfig {
        root_dir: wal_dir.clone(),
        max_segment_bytes: cfg.wal.max_segment_bytes,
        fsync_on_append: cfg.wal.fsync_on_append,
        soft_watermark_bytes: cfg.wal.soft_watermark_bytes,
        hard_cap_bytes: cfg.wal.hard_cap_bytes,
        recovery_mode: cfg.wal.recovery_mode,
        identity: cfg
            .wal_identity_config()
            .map_err(|error| eyre::eyre!("invalid writer identity config: {error}"))?,
        max_prepared_files: cfg.wal.max_prepared_files,
        max_prepared_bytes: cfg.wal.max_prepared_bytes,
    };
    let wal = teodb_storage::wal::WalManager::open(wal_cfg)
        .await
        .map_err(|error| eyre::eyre!("failed to open WAL at {}: {error}", wal_dir.display()))?;
    let identity = wal.writer_identity();
    info!(
        dir = %wal_dir.display(),
        cluster_id = %identity.cluster_id,
        node_id = %identity.node_id,
        writer_slot = %identity.writer_slot,
        writer_id = %identity.writer_id,
        writer_epoch = %identity.writer_epoch,
        "WAL ready"
    );
    Ok(Arc::new(wal))
}
