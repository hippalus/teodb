//! Background compaction, orphan sweeping, and cache persistence.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::info;

use teodb_core::snapshot_pin::ActiveSnapshotRegistry;
use teodb_core::snapshot_retention::SnapshotRetention;
use teodb_core::traits::catalog::Catalog;
use teodb_core::traits::storage::StorageFactory;
use teodb_distributed::compactor::CompactorBuilder;
use teodb_distributed::coordination::{CompactionLock, CompactionLockConfig};
use teodb_distributed::orphan::OrphanSweeper;
use teodb_distributed::selection::SelectionConfig;

use super::{compaction, sweep};
use crate::config::MaintenanceConfig;
use crate::metrics::Metrics;

/// All dependencies needed by the maintenance loop.
pub struct MaintenanceContext {
    pub cfg: MaintenanceConfig,
    /// Stable node identity resolved from the WAL identity record.
    pub node_id: String,
    pub catalog: Arc<dyn Catalog>,
    pub storage: Arc<dyn StorageFactory>,
    pub cache_index: Option<Arc<teodb_storage::cache::index::CacheIndex>>,
    pub metrics: Arc<Metrics>,
    /// Snapshot pins held by running queries — shared with the query engine
    /// so sweeps never expire a snapshot a query still reads. Data-node-local:
    /// pins on other data nodes are invisible (the retention window is the
    /// cross-node safety margin).
    pub snapshot_registry: Arc<dyn ActiveSnapshotRegistry>,
    pub object_store: teodb_query::ObjectStoreRegistration,
    /// Spill directory for compaction sorts that exceed the memory ceiling.
    pub spill_dir: std::path::PathBuf,
}

/// Background maintenance component: compaction, orphan sweeping, and cache
/// index persistence.
pub struct Maintenance {
    ctx: MaintenanceContext,
}

impl Maintenance {
    pub fn new(ctx: MaintenanceContext) -> teodb_core::TeoDBResult<Self> {
        Self::validate(&ctx)?;
        Ok(Self { ctx })
    }

    pub fn validate(ctx: &MaintenanceContext) -> teodb_core::TeoDBResult<()> {
        if ctx.cfg.enabled && ctx.cfg.compaction_enabled {
            let _ = build_compactor(ctx)?;
        }
        Ok(())
    }

    /// Persist cache index with contextual logging.
    ///
    /// Runs blocking I/O on a dedicated threadpool to avoid stalling the Tokio
    /// runtime. `only_if_dirty` skips the rewrite when nothing changed (periodic
    /// ticks); shutdown forces a final persist.
    async fn persist_cache_index(
        ci: &Arc<teodb_storage::cache::index::CacheIndex>,
        context: &'static str,
        only_if_dirty: bool,
    ) {
        let ci = Arc::clone(ci);
        let result = tokio::task::spawn_blocking(move || {
            if only_if_dirty {
                ci.persist_if_dirty().map(|_| ())
            } else {
                ci.persist()
            }
        })
        .await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!(error = %e, context, "failed to persist cache index"),
            Err(e) => tracing::warn!(error = %e, context, "cache persist task panicked"),
        }
    }

    /// Run until shutdown is signalled.
    #[tracing::instrument(name = "maintenance.loop", skip_all)]
    pub async fn run(self, mut shutdown_rx: watch::Receiver<bool>) {
        let ctx = self.ctx;
        if !ctx.cfg.enabled {
            info!("maintenance loop disabled by config");
            return;
        }

        let compaction_interval = Duration::from_secs(ctx.cfg.compaction_interval_secs);
        let sweep_interval = Duration::from_secs(ctx.cfg.orphan_sweep_interval_secs);

        let min_age = Duration::from_secs(ctx.cfg.orphan_retention_secs);
        let mut sweeper = OrphanSweeper::new(ctx.catalog.clone(), ctx.storage.clone(), min_age)
            .with_snapshot_registry(ctx.snapshot_registry.clone());
        if ctx.cfg.snapshot_retention_secs > 0 {
            info!(
                snapshot_retention_secs = ctx.cfg.snapshot_retention_secs,
                snapshot_keep_last = ctx.cfg.snapshot_keep_last,
                "snapshot expiration enabled"
            );
            sweeper = sweeper.with_retention(SnapshotRetention {
                max_age: Duration::from_secs(ctx.cfg.snapshot_retention_secs),
                keep_last: ctx.cfg.snapshot_keep_last.max(1),
            });
        }

        let node_id = ctx.node_id.clone();
        let compaction = if ctx.cfg.compaction_enabled {
            let compression = teodb_storage::parquet::CompressionCodec::from_str_config(&ctx.cfg.compression)
                .unwrap_or_else(|e| {
                    tracing::warn!(error = %e, "invalid compression config, falling back to zstd(3)");
                    teodb_storage::parquet::CompressionCodec::default()
                });

            let compactor = build_compactor_with_compression(&ctx, compression)
                .expect("maintenance compactor was validated before startup");
            let selection_cfg = SelectionConfig {
                target_file_bytes: ctx.cfg.target_file_bytes,
                min_files_per_compaction: ctx.cfg.min_files_per_compaction,
                max_files_per_compaction: ctx.cfg.max_files_per_compaction,
                max_bytes_per_compaction: ctx.cfg.max_bytes_per_compaction,
                ..Default::default()
            };
            // Compaction coordination lock — prevents concurrent compaction across nodes.
            let lock = CompactionLock::new(
                ctx.catalog.clone(),
                node_id.clone(),
                CompactionLockConfig {
                    lock_ttl: Duration::from_secs(ctx.cfg.lock_ttl_secs),
                },
            );
            Some((compactor, selection_cfg, lock))
        } else {
            info!("background compaction disabled by config");
            None
        };

        // Jittered deadlines (±15%) instead of fixed intervals: nodes deployed
        // together would otherwise run compaction/sweep in lockstep, hammering
        // the catalog and object store simultaneously (F-33). The first
        // deadline is one full (jittered) period out — let the system
        // stabilize after startup.
        let cache_persist_interval = Duration::from_secs(60);
        let mut next_compaction = tokio::time::Instant::now() + jittered(compaction_interval);
        let mut next_sweep = tokio::time::Instant::now() + jittered(sweep_interval);
        // Cache index is persisted every 60 seconds to survive crashes.
        let mut next_cache_persist = tokio::time::Instant::now() + cache_persist_interval;

        info!(
            compaction_enabled = ctx.cfg.compaction_enabled,
            compaction_secs = ctx.cfg.compaction_interval_secs,
            sweep_secs = ctx.cfg.orphan_sweep_interval_secs,
            %node_id,
            "maintenance loop started"
        );

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if let Some(ref ci) = ctx.cache_index {
                        Self::persist_cache_index(ci, "shutdown", false).await;
                    }
                    info!("maintenance loop stopping on shutdown signal");
                    return;
                }
                _ = tokio::time::sleep_until(next_compaction), if compaction.is_some() => {
                    next_compaction = tokio::time::Instant::now() + jittered(compaction_interval);
                    if let Some((compactor, selection_cfg, lock)) = &compaction {
                        compaction::CompactionCycle::new(
                            &ctx.catalog,
                            compactor,
                            selection_cfg,
                            lock,
                            &ctx.metrics,
                        ).run().await;
                    }
                }
                _ = tokio::time::sleep_until(next_sweep) => {
                    next_sweep = tokio::time::Instant::now() + jittered(sweep_interval);
                    sweep::run_orphan_sweep(&ctx.catalog, &sweeper).await;
                }
                _ = tokio::time::sleep_until(next_cache_persist) => {
                    next_cache_persist = tokio::time::Instant::now() + cache_persist_interval;
                    if let Some(ref ci) = ctx.cache_index {
                        Self::persist_cache_index(ci, "periodic", true).await;
                    }
                }
            }
        }
    }
}

fn build_compactor(ctx: &MaintenanceContext) -> teodb_core::TeoDBResult<teodb_distributed::compactor::Compactor> {
    let compression = teodb_storage::parquet::CompressionCodec::from_str_config(&ctx.cfg.compression)
        .unwrap_or_else(|_| teodb_storage::parquet::CompressionCodec::default());
    build_compactor_with_compression(ctx, compression)
}

fn build_compactor_with_compression(
    ctx: &MaintenanceContext,
    compression: teodb_storage::parquet::CompressionCodec,
) -> teodb_core::TeoDBResult<teodb_distributed::compactor::Compactor> {
    CompactorBuilder::new(ctx.catalog.clone(), ctx.storage.clone(), ctx.object_store.clone())
        .compression(compression)
        .memory_limit(ctx.cfg.compaction_memory_bytes, ctx.spill_dir.clone())
        .build()
}

/// Apply ±15% uniform jitter to a maintenance interval.
fn jittered(base: Duration) -> Duration {
    use rand::RngExt;
    base.mul_f64(rand::rng().random_range(0.85..=1.15))
}
