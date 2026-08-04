//! Orphan file sweeper: identifies and removes data files that are not
//! referenced by **any** snapshot in the table's metadata history. These can
//! arise from failed flushes, failed compactions, aborted writes, or catalog
//! conflicts where a Parquet file was uploaded but never committed.
//!
//! Safety properties:
//! - Only the `data/` subtree of the table location is ever listed or
//!   deleted. The `metadata/` subtree (Iceberg metadata JSON, manifest
//!   lists, manifests) belongs to the catalog and is never touched.
//! - Without a retention policy, files referenced by *any* snapshot in
//!   history are protected — older snapshots need them for time travel.
//! - With a retention policy, snapshots outside the policy are expired:
//!   only files referenced by retained snapshots stay protected, which
//!   reclaims the space held by compacted-away files.
//! - Snapshots pinned by running queries are always retained, and a sweep
//!   aborts if a pin lands on a non-retained snapshot mid-sweep.
//! - Only files older than `min_age` are eligible, so in-progress writers
//!   that have not yet committed are never raced.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt;
use tracing::{debug, info, warn};

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::{SnapshotId, TableIdent};
use teodb_core::location::{ObjectLocation, ObjectPath};
use teodb_core::snapshot_pin::{ActiveSnapshotRegistry, InMemorySnapshotRegistry};
use teodb_core::snapshot_retention::SnapshotRetention;
use teodb_core::traits::catalog::Catalog;
use teodb_core::traits::storage::{ObjectMeta, Storage, StorageFactory};

/// Report summarising the outcome of a single sweep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepReport {
    /// Total files scanned under the table's `data/` prefix.
    pub scanned: usize,
    /// Files deleted because no snapshot in the table's history references them.
    pub deleted: usize,
    /// Files retained (either referenced or younger than `min_age`).
    pub retained: usize,
}

/// Sweeps orphaned data files from object storage that are not referenced by
/// any snapshot in the table's Iceberg metadata history.
///
/// Only files older than `min_age` are eligible for deletion. This grace
/// period prevents racing with in-progress writers that have not yet
/// committed their files to the catalog.
pub struct OrphanSweeper {
    catalog: Arc<dyn Catalog>,
    storage_factory: Arc<dyn StorageFactory>,
    snapshot_registry: Arc<dyn ActiveSnapshotRegistry>,
    min_age: Duration,
    /// When set, snapshots outside the policy are expired: their exclusive
    /// files lose protection. `None` protects the full snapshot history.
    retention: Option<SnapshotRetention>,
}

struct ProtectedFiles {
    keys: HashSet<String>,
    retained_snapshots: HashSet<SnapshotId>,
    expired_snapshots: usize,
}

struct SweepListing {
    storage: Arc<dyn Storage>,
    data_prefix: ObjectPath,
    objects: Vec<ObjectMeta>,
}

impl OrphanSweeper {
    pub fn new(catalog: Arc<dyn Catalog>, storage_factory: Arc<dyn StorageFactory>, min_age: Duration) -> Self {
        Self {
            catalog,
            storage_factory,
            // Defaults to an empty in-process registry (no cross-node pins);
            // callers that track query pins override it via
            // `with_snapshot_registry`. An empty registry is equivalent to the
            // previous "no registry" case: it reports no active pins.
            snapshot_registry: Arc::new(InMemorySnapshotRegistry::new()),
            min_age,
            retention: None,
        }
    }

    /// Consult the active snapshot pin registry before deleting files.
    pub fn with_snapshot_registry(mut self, snapshot_registry: Arc<dyn ActiveSnapshotRegistry>) -> Self {
        self.snapshot_registry = snapshot_registry;
        self
    }

    /// Enable snapshot expiration: snapshots outside `retention` no longer
    /// protect their files from sweeping (pinned and current snapshots are
    /// always retained).
    pub fn with_retention(mut self, retention: SnapshotRetention) -> Self {
        self.retention = Some(retention);
        self
    }

    /// Sweep orphaned data files for the given table.
    ///
    /// 1. Reads active snapshot pins — pinned snapshots are always retained.
    /// 2. Collects every file path referenced by retained snapshots (the
    ///    full history when no retention policy is configured).
    /// 3. Lists objects under the table's `data/` prefix only.
    /// 4. Re-checks pins when expiration dropped snapshots — a pin that
    ///    appeared mid-sweep on a non-retained snapshot aborts the sweep.
    /// 5. Deletes objects that are not in the referenced set *and* are older
    ///    than `min_age`.
    #[tracing::instrument(name = "orphan.sweep_table", skip_all, fields(table = %table))]
    pub async fn sweep(&self, table: &TableIdent) -> TeoDBResult<SweepReport> {
        let pinned = self.active_pins(table).await?;
        let metadata = self.catalog.load_table(table).await?;
        let protected = self
            .collect_protected_files(table, &pinned)
            .await?;
        let table_location = metadata.table_location.to_uri();
        let listing = self.list_data_objects(&table_location).await?;
        let scanned = listing.objects.len();

        if self
            .pins_changed_during_sweep(table, &protected)
            .await?
        {
            warn!(
                table = %table,
                "orphan sweep: snapshot pinned outside the retained set mid-sweep, aborting sweep for this table"
            );
            return Ok(SweepReport {
                scanned,
                deleted: 0,
                retained: scanned,
            });
        }

        let deleted = self
            .delete_candidates(&listing, &protected.keys)
            .await;
        let retained = scanned - deleted;
        info!(table = %table, scanned, deleted, retained, "orphan sweep complete");
        Ok(SweepReport {
            scanned,
            deleted,
            retained,
        })
    }

    async fn active_pins(&self, table: &TableIdent) -> TeoDBResult<HashSet<SnapshotId>> {
        // Best-effort GC of leases left by crashed nodes. `active_snapshots`
        // already ignores expired pins, so a failure here is harmless — log and
        // continue rather than aborting the sweep.
        match self.snapshot_registry.expire_stale(table).await {
            Ok(expired) if expired > 0 => {
                debug!(table = %table, expired, "orphan sweep: expired stale snapshot pins")
            }
            Ok(_) => {}
            Err(error) => warn!(table = %table, error = %error, "orphan sweep: failed to expire stale pins"),
        }
        Ok(self
            .snapshot_registry
            .active_snapshots(table)
            .await?
            .into_iter()
            .collect())
    }

    async fn collect_protected_files(
        &self,
        table: &TableIdent,
        pinned: &HashSet<SnapshotId>,
    ) -> TeoDBResult<ProtectedFiles> {
        // Files compacted away from the current snapshot are still needed by
        // older snapshots for time travel, so the reference set covers every
        // retained snapshot, not just the current one.
        let (referenced_uris, retained_snapshots, expired_snapshots) = match &self.retention {
            None => (
                self.catalog
                    .load_all_referenced_file_paths(table)
                    .await?,
                HashSet::new(),
                0,
            ),
            Some(retention) => {
                let retained = self
                    .catalog
                    .load_retained_file_paths(table, retention, pinned)
                    .await?;
                (retained.paths, retained.retained_snapshots, retained.expired_snapshots)
            }
        };

        // Normalize manifest URIs to bucket-relative keys, the same shape
        // that `Storage::list` yields.
        let keys: HashSet<String> = referenced_uris
            .into_iter()
            .map(|uri| {
                ObjectLocation::parse(&uri)
                    .map(|loc| loc.key)
                    .unwrap_or(uri)
            })
            .collect();

        debug!(
            table = %table,
            referenced = keys.len(),
            expired_snapshots,
            "orphan sweep: collected referenced file keys across retained snapshot history"
        );
        Ok(ProtectedFiles {
            keys,
            retained_snapshots,
            expired_snapshots,
        })
    }

    async fn list_data_objects(&self, table_location: &str) -> TeoDBResult<SweepListing> {
        let table_location =
            ObjectLocation::parse(table_location).map_err(|error| TeoDBError::Catalog(error.to_string()))?;
        let (storage, table_prefix) = self
            .storage_factory
            .resolve(&table_location)
            .await?;

        // Only the data/ subtree is ever listed or deleted. The metadata/
        // subtree is owned by the Iceberg catalog and is never present in the
        // data-file reference set, so it must not be visible to the sweep.
        //
        // The trailing slash makes `data/` a segment boundary: the
        // defense-in-depth `starts_with` guard below is a raw string match, so
        // without it `…/data` would also accept a sibling like `…/data2/…`.
        let data_prefix = if table_prefix.as_str().is_empty() {
            ObjectPath::new("data/")
        } else {
            ObjectPath::new(format!("{}/data/", table_prefix.as_str().trim_end_matches('/')))
        };

        let objects = storage
            .list(&data_prefix)
            .await?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| TeoDBError::Internal(format!("orphan sweep list failed: {error}")))?;
        Ok(SweepListing {
            storage,
            data_prefix,
            objects,
        })
    }

    async fn pins_changed_during_sweep(&self, table: &TableIdent, protected: &ProtectedFiles) -> TeoDBResult<bool> {
        // Expiration shrank the protected set, so a query that pinned a
        // snapshot *after* the pin fetch above could be reading files this
        // sweep is about to delete. Re-check: any pin outside the retained
        // set aborts the sweep (it reruns next cycle).
        if protected.expired_snapshots > 0 {
            let pins_now = self
                .snapshot_registry
                .active_snapshots(table)
                .await?;
            return Ok(pins_now
                .iter()
                .any(|id| !protected.retained_snapshots.contains(id)));
        }
        Ok(false)
    }

    async fn delete_candidates(&self, listing: &SweepListing, referenced_keys: &HashSet<String>) -> usize {
        let mut deleted = 0usize;
        let now = chrono::Utc::now();

        for obj in &listing.objects {
            // `Storage::list` yields bucket-relative keys — compare directly
            // against the normalized reference set.
            if referenced_keys.contains(obj.path.as_str()) {
                continue;
            }

            // Defense in depth: never delete outside the data/ subtree even
            // if a backend returns unexpected paths from a scoped list.
            if !obj
                .path
                .as_str()
                .starts_with(listing.data_prefix.as_str())
            {
                warn!(
                    path = %obj.path,
                    prefix = %listing.data_prefix,
                    "orphan sweep: listed object outside data prefix, skipping"
                );
                continue;
            }

            // Check age threshold.
            let age = now
                .signed_duration_since(obj.last_modified)
                .to_std()
                .unwrap_or(Duration::ZERO);

            if age < self.min_age {
                debug!(
                    path = %obj.path,
                    age_secs = age.as_secs(),
                    min_age_secs = self.min_age.as_secs(),
                    "orphan sweep: file too young, skipping"
                );
                continue;
            }

            match listing.storage.delete(&obj.path).await {
                Ok(()) => {
                    debug!(path = %obj.path, "orphan sweep: deleted orphaned file");
                    deleted += 1;
                }
                Err(e) => {
                    warn!(path = %obj.path, error = %e, "orphan sweep: failed to delete file");
                }
            }
        }
        deleted
    }
}
