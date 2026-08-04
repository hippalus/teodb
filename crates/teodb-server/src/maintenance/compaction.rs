//! Compaction cycle — selects and executes compaction plans for all tables.

use tracing::{debug, error, info, warn};

use std::sync::Arc;
use teodb_core::traits::catalog::Catalog;
use teodb_distributed::compactor::{CompactionPlan, Compactor};
use teodb_distributed::coordination::{CompactionLock, LockOutcome};
use teodb_distributed::selection::{SelectionConfig, select_compaction_candidates_with_delete_counts};

use crate::metrics::Metrics;

pub struct CompactionCycle<'a> {
    catalog: &'a Arc<dyn Catalog>,
    compactor: &'a Compactor,
    selection_cfg: &'a SelectionConfig,
    lock: &'a CompactionLock,
    metrics: &'a Metrics,
}

impl<'a> CompactionCycle<'a> {
    pub fn new(
        catalog: &'a Arc<dyn Catalog>,
        compactor: &'a Compactor,
        selection_cfg: &'a SelectionConfig,
        lock: &'a CompactionLock,
        metrics: &'a Metrics,
    ) -> Self {
        Self {
            catalog,
            compactor,
            selection_cfg,
            lock,
            metrics,
        }
    }

    #[tracing::instrument(name = "maintenance.compaction_cycle", skip_all)]
    pub async fn run(&self) {
        let namespaces = match self.catalog.list_namespaces().await {
            Ok(ns) => ns,
            Err(e) => {
                warn!(error = %e, "compaction: failed to list namespaces");
                return;
            }
        };

        let mut plans_run = 0u64;
        let mut plans_committed = 0u64;

        for ns in &namespaces {
            let tables = match self.catalog.list_tables(ns).await {
                Ok(t) => t,
                Err(e) => {
                    warn!(namespace = %ns, error = %e, "compaction: failed to list tables");
                    continue;
                }
            };

            for table_ident in &tables {
                // Acquire per-table compaction lock.
                match self.lock.try_acquire(table_ident).await {
                    LockOutcome::Acquired => {}
                    LockOutcome::HeldBy { owner, .. } => {
                        debug!(
                            table = %table_ident,
                            owner = %owner,
                            "compaction: skipping, lock held by another process"
                        );
                        continue;
                    }
                    LockOutcome::Failed(e) => {
                        warn!(
                            table = %table_ident,
                            error = %e,
                            "compaction: failed to acquire lock"
                        );
                        continue;
                    }
                }

                let (table_metadata, live_files) = match tokio::try_join!(
                    self.catalog.load_table(table_ident),
                    self.catalog.load_live_files(table_ident)
                ) {
                    Ok(loaded) => loaded,
                    Err(e) => {
                        warn!(table = %table_ident, error = %e, "compaction: failed to load table metadata/files");
                        self.lock.release(table_ident).await;
                        continue;
                    }
                };

                let metadata = match (*table_metadata)
                    .clone()
                    .with_live_files(live_files)
                {
                    Ok(m) => m,
                    Err(e) => {
                        warn!(table = %table_ident, error = %e, "compaction: invalid catalog metadata");
                        self.lock.release(table_ident).await;
                        continue;
                    }
                };

                let snapshot_id = match metadata.current_snapshot_id {
                    Some(id) => id,
                    None => {
                        self.lock.release(table_ident).await;
                        continue;
                    }
                };

                let snapshot = metadata
                    .current_snapshot
                    .as_ref()
                    .expect("snapshot_id checked above");

                let delete_counts = match self
                    .compactor
                    .count_position_deletes(&snapshot.delete_files, &snapshot.data_files)
                    .await
                {
                    Ok(counts) => counts,
                    Err(e) => {
                        warn!(table = %table_ident, error = %e, "compaction: failed to resolve delete pressure");
                        self.lock.release(table_ident).await;
                        continue;
                    }
                };

                let groups = select_compaction_candidates_with_delete_counts(
                    &snapshot.data_files,
                    &snapshot.delete_files,
                    snapshot_id,
                    self.selection_cfg,
                    &delete_counts,
                );
                for group in groups {
                    let plan =
                        CompactionPlan::from_group(group, table_ident.clone(), self.selection_cfg.target_file_bytes);
                    plans_run += 1;
                    let compact_start = std::time::Instant::now();
                    match self.compactor.compact(plan).await {
                        Ok(teodb_distributed::compactor::CompactionOutcome::Committed { added, removed }) => {
                            plans_committed += 1;
                            self.metrics.compaction.total.inc();
                            self.metrics
                                .compaction
                                .duration_seconds
                                .observe(compact_start.elapsed().as_secs_f64());
                            info!(
                                table = %table_ident,
                                added = added.len(),
                                removed = removed.len(),
                                "compaction: plan committed"
                            );
                        }
                        Ok(teodb_distributed::compactor::CompactionOutcome::ConflictAbandoned { orphan_files }) => {
                            warn!(
                                table = %table_ident,
                                orphans = orphan_files.len(),
                                "compaction: conflict, output files orphaned"
                            );
                        }
                        Ok(teodb_distributed::compactor::CompactionOutcome::NoChange) => {}
                        Err(e) => {
                            self.metrics.compaction.errors_total.inc();
                            error!(table = %table_ident, error = %e, "compaction: plan failed");
                        }
                    }
                }

                // Release per-table lock after all groups for this table are done.
                self.lock.release(table_ident).await;
            }
        }

        if plans_run > 0 {
            info!(plans_run, plans_committed, "compaction cycle complete");
        }
    }
}
