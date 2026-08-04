//! Manifest walking: data-file listing and referenced/retained file sets.

use std::collections::{HashMap, HashSet};

use tracing::debug;

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::TableIdent;
use teodb_core::snapshot_retention::SnapshotRetention;
use teodb_core::traits::catalog::RetainedFileSet;

use crate::convert::LoadedIcebergDataFile;
use crate::error::map_iceberg_error;

pub(super) const REMOVED_DATA_FILES_PROP: &str = "teodb.removed_data_files";

/// Reads an Iceberg table's manifests: live data-file listing and the
/// referenced/retained file sets used by the orphan sweeper and snapshot
/// expiry. Borrows the loaded `Table` and reads manifests through its
/// `file_io`, so call sites are `ManifestReader::new(&table).method(...)`.
pub(super) struct ManifestReader<'a> {
    table: &'a iceberg::table::Table,
}

impl<'a> ManifestReader<'a> {
    pub(super) fn new(table: &'a iceberg::table::Table) -> Self {
        Self { table }
    }

    /// List the live content files of the table's current snapshot.
    ///
    /// The pinned Iceberg crate does not expose a public overwrite/delete action
    /// for compaction. Until TeoDB can write a real Iceberg replace, compaction
    /// records removed data-file paths in snapshot summary properties. Read paths
    /// must honor those markers or they will double-count compacted inputs.
    /// This is the read side of the interim replace implementation in
    /// `super::commit`; both sides must change together when Iceberg overwrite
    /// support is available.
    pub(super) async fn live_data_files(&self) -> TeoDBResult<Vec<LoadedIcebergDataFile>> {
        let metadata = self.table.metadata();
        let Some(snapshot) = metadata.current_snapshot() else {
            return Ok(Vec::new());
        };
        let removed_paths = removed_data_file_paths(metadata)?;

        let manifest_list = self
            .table
            .manifest_list_reader(snapshot)
            .load()
            .await
            .map_err(map_iceberg_error)?;

        let mut data_files = Vec::new();
        for manifest_file in manifest_list.entries() {
            let manifest = self
                .read_manifest(&manifest_file.manifest_path)
                .await?;
            let manifest_metadata = manifest.metadata();
            let schema_id = manifest_metadata.schema_id();
            let schema = manifest_metadata.schema().clone();
            let partition_spec = manifest_metadata.partition_spec().clone();
            let partition_spec_id = partition_spec.spec_id();
            for entry in manifest.entries() {
                if entry.is_alive() && !removed_paths.contains(entry.data_file().file_path()) {
                    data_files.push(LoadedIcebergDataFile {
                        file: entry.data_file().clone(),
                        schema_id,
                        schema: schema.clone(),
                        partition_spec_id,
                        partition_spec: partition_spec.clone(),
                    });
                }
            }
        }

        if !removed_paths.is_empty() {
            debug!(
                removed = removed_paths.len(),
                live_files = data_files.len(),
                "reconciled TeoDB removed-file markers while listing live manifest files"
            );
        }

        Ok(data_files)
    }

    /// Collect every file path recorded in the manifests of the snapshots
    /// accepted by `include_snapshot`.
    ///
    /// Every manifest entry is included, alive or not: an entry marked deleted in
    /// one snapshot still references a file that earlier retained snapshots need
    /// until they expire.
    pub(super) async fn collect_referenced_paths(
        &self,
        include_snapshot: impl Fn(i64) -> bool,
    ) -> TeoDBResult<HashSet<String>> {
        let metadata = self.table.metadata();
        let mut referenced: HashSet<String> = HashSet::new();
        // Manifests are immutable and shared across snapshots — read each once.
        let mut seen_manifests: HashSet<String> = HashSet::new();

        for snapshot in metadata.snapshots() {
            if !include_snapshot(snapshot.snapshot_id()) {
                continue;
            }
            let manifest_list = self
                .table
                .manifest_list_reader(snapshot)
                .load()
                .await
                .map_err(map_iceberg_error)?;

            for manifest_file in manifest_list.entries() {
                if !seen_manifests.insert(manifest_file.manifest_path.clone()) {
                    continue;
                }
                let manifest = self
                    .read_manifest(&manifest_file.manifest_path)
                    .await?;
                for entry in manifest.entries() {
                    referenced.insert(entry.data_file().file_path().to_string());
                }
            }
        }

        Ok(referenced)
    }

    /// Apply the retention policy to the table's snapshot history and collect
    /// the file paths referenced by the retained snapshots.
    pub(super) async fn retained_file_set(
        &self,
        ident: &TableIdent,
        retention: &SnapshotRetention,
        protected: &HashSet<teodb_core::ident::SnapshotId>,
    ) -> TeoDBResult<RetainedFileSet> {
        let metadata = self.table.metadata();
        let history: Vec<(i64, i64)> = metadata
            .snapshots()
            .map(|s| (s.snapshot_id(), s.timestamp_ms()))
            .collect();

        // A clock before the epoch yields now_ms = 0, which expires nothing.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
            .unwrap_or(0);
        let expired = teodb_core::snapshot_retention::select_expired_snapshots(
            &history,
            metadata.current_snapshot_id(),
            retention,
            protected,
            now_ms,
        );

        let retained_snapshots: HashSet<i64> = history
            .iter()
            .map(|(id, _)| *id)
            .filter(|id| !expired.contains(id))
            .collect();

        if !expired.is_empty() {
            debug!(
                table = %ident,
                expired = expired.len(),
                retained = retained_snapshots.len(),
                "snapshot expiration: dropping expired snapshots from the protected file set"
            );
        }

        let paths = self
            .collect_referenced_paths(|snapshot_id| retained_snapshots.contains(&snapshot_id))
            .await?;

        Ok(RetainedFileSet {
            paths,
            retained_snapshots,
            expired_snapshots: expired.len(),
        })
    }

    /// Read and parse one manifest file.
    async fn read_manifest(&self, path: &str) -> TeoDBResult<iceberg::spec::Manifest> {
        let manifest_bytes = self
            .table
            .file_io()
            .new_input(path)
            .map_err(map_iceberg_error)?
            .read()
            .await
            .map_err(map_iceberg_error)?;

        iceberg::spec::Manifest::parse_avro(&manifest_bytes).map_err(map_iceberg_error)
    }
}

fn removed_data_file_paths(metadata: &iceberg::spec::TableMetadata) -> TeoDBResult<HashSet<String>> {
    removed_data_file_paths_from_snapshots(metadata.current_snapshot_id(), metadata.snapshots().map(|s| s.as_ref()))
}

fn removed_data_file_paths_from_snapshots<'a>(
    current_snapshot_id: Option<i64>,
    snapshots: impl IntoIterator<Item = &'a iceberg::spec::Snapshot>,
) -> TeoDBResult<HashSet<String>> {
    let by_id: HashMap<i64, &iceberg::spec::Snapshot> = snapshots
        .into_iter()
        .map(|snapshot| (snapshot.snapshot_id(), snapshot))
        .collect();
    let mut removed = HashSet::new();
    let mut next = current_snapshot_id;
    while let Some(snapshot_id) = next {
        let snapshot = by_id.get(&snapshot_id).ok_or_else(|| {
            TeoDBError::Catalog(format!(
                "snapshot {snapshot_id} referenced by current lineage is missing from table metadata"
            ))
        })?;
        if let Some(raw) = snapshot
            .summary()
            .additional_properties
            .get(REMOVED_DATA_FILES_PROP)
        {
            let paths: Vec<String> = serde_json::from_str(raw).map_err(|error| {
                TeoDBError::Catalog(format!(
                    "failed to parse {REMOVED_DATA_FILES_PROP} on snapshot {snapshot_id}: {error}"
                ))
            })?;
            removed.extend(paths);
        }
        next = snapshot.parent_snapshot_id();
    }

    Ok(removed)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{Operation, Snapshot, Summary};

    use super::*;

    fn snapshot(id: i64, parent: Option<i64>, removed: &[&str]) -> Snapshot {
        let mut additional_properties = HashMap::new();
        if !removed.is_empty() {
            additional_properties.insert(
                REMOVED_DATA_FILES_PROP.to_string(),
                serde_json::to_string(&removed).unwrap(),
            );
        }

        Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(id)
            .with_timestamp_ms(id * 1000)
            .with_manifest_list(format!("s3://bucket/metadata/{id}.avro"))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties,
            })
            .build()
    }

    #[test]
    fn removed_file_markers_follow_only_current_snapshot_lineage() {
        let root = snapshot(1, None, &[]);
        let compacted = snapshot(2, Some(1), &["s3://bucket/table/data/old.parquet"]);
        let side_branch = snapshot(3, Some(1), &["s3://bucket/table/data/side.parquet"]);

        let removed = removed_data_file_paths_from_snapshots(Some(2), [&root, &compacted, &side_branch]).unwrap();

        assert!(removed.contains("s3://bucket/table/data/old.parquet"));
        assert!(
            !removed.contains("s3://bucket/table/data/side.parquet"),
            "markers outside the current lineage must not hide files after rollback/branch changes"
        );

        let rolled_back = removed_data_file_paths_from_snapshots(Some(1), [&root, &compacted]).unwrap();
        assert!(rolled_back.is_empty());
    }
}
