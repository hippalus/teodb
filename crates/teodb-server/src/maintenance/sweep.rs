//! Orphan file sweeper — removes stale data files past the retention window.

use tracing::{info, warn};

use std::sync::Arc;

use teodb_core::traits::catalog::Catalog;
use teodb_distributed::orphan::OrphanSweeper;

#[tracing::instrument(name = "maintenance.orphan_sweep_cycle", skip_all)]
pub async fn run_orphan_sweep(catalog: &Arc<dyn Catalog>, sweeper: &OrphanSweeper) {
    let namespaces = match catalog.list_namespaces().await {
        Ok(ns) => ns,
        Err(e) => {
            warn!(error = %e, "orphan sweep: failed to list namespaces");
            return;
        }
    };

    let mut total_deleted = 0usize;

    for ns in &namespaces {
        let tables = match catalog.list_tables(ns).await {
            Ok(t) => t,
            Err(e) => {
                warn!(namespace = %ns, error = %e, "orphan sweep: failed to list tables");
                continue;
            }
        };

        for table_ident in &tables {
            match sweeper.sweep(table_ident).await {
                Ok(report) => {
                    total_deleted += report.deleted;
                }
                Err(e) => {
                    warn!(table = %table_ident, error = %e, "orphan sweep: table sweep failed");
                }
            }
        }
    }

    if total_deleted > 0 {
        info!(deleted = total_deleted, "orphan sweep complete");
    }
}
