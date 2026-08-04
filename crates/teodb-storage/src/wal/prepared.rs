//! Durable prepared-flush intent sidecars.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::file::{DataContent, DataFile, FileFormat};
use teodb_core::ident::{SnapshotId, TableIdent};
use teodb_core::write_protocol::{CommitId, GenerationRange, WRITE_PROTOCOL_VERSION, WriterEpoch, WriterId};
use uuid::Uuid;

const PREPARED_DIR: &str = "prepared";
const DEFAULT_MAX_PREPARED_FILES: usize = 10_000;
const DEFAULT_MAX_PREPARED_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedFlush {
    pub version: u16,
    pub table: TableIdent,
    pub table_uuid: Uuid,
    pub writer_id: WriterId,
    pub writer_epoch: WriterEpoch,
    pub commit_id: CommitId,
    pub generations: GenerationRange,
    pub record_count: u64,
    pub prepared_at_ms: i64,
    pub data_files: Vec<DataFile>,
    pub base_snapshot_id: Option<SnapshotId>,
}

impl PreparedFlush {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table: TableIdent,
        table_uuid: Uuid,
        writer_id: WriterId,
        writer_epoch: WriterEpoch,
        commit_id: CommitId,
        generations: GenerationRange,
        record_count: u64,
        prepared_at_ms: i64,
        data_files: Vec<DataFile>,
        base_snapshot_id: Option<SnapshotId>,
    ) -> Self {
        Self {
            version: WRITE_PROTOCOL_VERSION,
            table,
            table_uuid,
            writer_id,
            writer_epoch,
            commit_id,
            generations,
            record_count,
            prepared_at_ms,
            data_files,
            base_snapshot_id,
        }
    }
}

pub(super) fn persist(
    root: &Path,
    prepared: &PreparedFlush,
    expected_writer: WriterId,
    max_files: usize,
    max_bytes: u64,
) -> TeoDBResult<()> {
    validate(prepared, expected_writer, max_files, max_bytes)?;
    let directory = root.join(PREPARED_DIR);
    std::fs::create_dir_all(&directory)
        .map_err(|error| TeoDBError::wal_source("create prepared intent directory", error))?;
    let path = path_for(&directory, prepared.table_uuid);

    if path.exists() {
        let existing = read_one(&path, expected_writer, max_files, max_bytes)?;
        if existing == *prepared {
            return Ok(());
        }
        return Err(TeoDBError::wal(format!(
            "different prepared intent already exists for table {} ({})",
            prepared.table, prepared.table_uuid
        )));
    }

    let payload = serde_json::to_vec_pretty(prepared)
        .map_err(|error| TeoDBError::wal_source("serialize prepared intent", error))?;
    if payload.len() as u64 > max_bytes {
        return Err(TeoDBError::wal(format!(
            "prepared intent is {} bytes, exceeding limit {max_bytes}",
            payload.len()
        )));
    }

    let temp = directory.join(format!(".{}.{}.tmp", prepared.table_uuid, Uuid::now_v7()));
    let result = (|| -> TeoDBResult<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)
            .map_err(|error| TeoDBError::wal_source("create prepared intent temp file", error))?;
        file.write_all(&payload)
            .map_err(|error| TeoDBError::wal_source("write prepared intent", error))?;
        file.sync_all()
            .map_err(|error| TeoDBError::wal_source("fsync prepared intent", error))?;
        std::fs::rename(&temp, &path).map_err(|error| TeoDBError::wal_source("rename prepared intent", error))?;
        fsync_directory(&directory)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temp);
    }
    result
}

pub(super) fn list(
    root: &Path,
    expected_writer: WriterId,
    max_files: usize,
    max_bytes: u64,
) -> TeoDBResult<Vec<PreparedFlush>> {
    let directory = root.join(PREPARED_DIR);
    let entries = match std::fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Vec::new());
        }
        Err(error) => {
            return Err(TeoDBError::wal_source("read prepared intent directory", error));
        }
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| TeoDBError::wal_source("read prepared intent entry", error))?;
        let path = entry.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            paths.push(path);
        }
    }
    paths.sort();
    paths
        .iter()
        .map(|path| read_one(path, expected_writer, max_files, max_bytes))
        .collect()
}

pub(super) fn remove(root: &Path, table_uuid: Uuid) -> TeoDBResult<()> {
    let directory = root.join(PREPARED_DIR);
    let path = path_for(&directory, table_uuid);
    match std::fs::remove_file(&path) {
        Ok(()) => fsync_directory(&directory),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(TeoDBError::wal_source(
            format!("remove prepared intent {}", path.display()),
            error,
        )),
    }
}

fn read_one(path: &Path, expected_writer: WriterId, max_files: usize, max_bytes: u64) -> TeoDBResult<PreparedFlush> {
    let metadata = std::fs::metadata(path).map_err(|error| TeoDBError::wal_source("stat prepared intent", error))?;
    if metadata.len() == 0 || metadata.len() > max_bytes {
        return Err(TeoDBError::wal(format!(
            "prepared intent {} has invalid size {} (limit {max_bytes})",
            path.display(),
            metadata.len()
        )));
    }
    let mut payload = Vec::with_capacity(metadata.len() as usize);
    File::open(path)
        .and_then(|mut file| file.read_to_end(&mut payload))
        .map_err(|error| TeoDBError::wal_source("read prepared intent", error))?;
    let prepared: PreparedFlush =
        serde_json::from_slice(&payload).map_err(|error| TeoDBError::wal_source("parse prepared intent", error))?;
    validate(&prepared, expected_writer, max_files, max_bytes)?;
    let expected_name = format!("{}.json", prepared.table_uuid);
    if path.file_name().and_then(|name| name.to_str()) != Some(&expected_name) {
        return Err(TeoDBError::wal(format!(
            "prepared intent filename does not match table UUID {}",
            prepared.table_uuid
        )));
    }
    Ok(prepared)
}

fn validate(prepared: &PreparedFlush, expected_writer: WriterId, max_files: usize, _max_bytes: u64) -> TeoDBResult<()> {
    if prepared.version != WRITE_PROTOCOL_VERSION {
        return Err(TeoDBError::wal(format!(
            "unsupported prepared intent version {}",
            prepared.version
        )));
    }
    if prepared.table_uuid.is_nil() {
        return Err(TeoDBError::wal("prepared intent has nil table UUID"));
    }
    if prepared.writer_id.as_uuid().is_nil() {
        return Err(TeoDBError::wal("prepared intent has nil writer ID"));
    }
    if prepared.writer_id != expected_writer {
        return Err(TeoDBError::wal(format!(
            "prepared intent writer {} does not match WAL writer {expected_writer}",
            prepared.writer_id
        )));
    }
    if prepared.data_files.len() > max_files {
        return Err(TeoDBError::wal(format!(
            "prepared intent has {} files, exceeding limit {max_files}",
            prepared.data_files.len()
        )));
    }
    if prepared.writer_epoch == WriterEpoch::ZERO {
        return Err(TeoDBError::wal("prepared intent has zero writer epoch"));
    }
    if prepared.commit_id.as_uuid().is_nil() {
        return Err(TeoDBError::wal("prepared intent has nil commit ID"));
    }
    if prepared.generations.lo == 0 || prepared.generations.lo > prepared.generations.hi {
        return Err(TeoDBError::wal("prepared intent has an invalid generation range"));
    }
    if prepared.record_count == 0 {
        return Err(TeoDBError::wal("prepared intent has zero records"));
    }
    if prepared.prepared_at_ms < 0 {
        return Err(TeoDBError::wal("prepared intent has a negative preparation timestamp"));
    }
    if prepared.data_files.is_empty() {
        return Err(TeoDBError::wal("prepared intent has no data files"));
    }

    // The persisted table location is not duplicated in the sidecar. Validate
    // structural path safety here; the flusher additionally validates the
    // exact table-location prefix before persistence.
    let writer_component = prepared.writer_id.to_string();
    let commit_prefix = format!("{}-", prepared.commit_id);
    let mut seen_paths = std::collections::HashSet::new();
    let mut file_records = 0u64;
    for data_file in &prepared.data_files {
        let key = &data_file.path.key;
        let components: Vec<_> = key.split('/').collect();
        let writer_index = components
            .iter()
            .rposition(|component| *component == writer_component);
        let data_index = components
            .iter()
            .position(|component| *component == "data");
        let path_is_writer_owned = writer_index
            .is_some_and(|index| index + 2 == components.len() && data_index.is_some_and(|data| data < index));
        if key.starts_with('/')
            || components
                .iter()
                .any(|component| component.is_empty() || *component == "..")
            || !path_is_writer_owned
        {
            return Err(TeoDBError::wal(format!(
                "prepared data file path is outside the writer data subtree: {key}"
            )));
        }
        let file_name = key.rsplit('/').next().unwrap_or_default();
        if !file_name.starts_with(&commit_prefix) || !file_name.ends_with(".parquet") {
            return Err(TeoDBError::wal(format!(
                "prepared data file '{key}' is not owned by commit {}",
                prepared.commit_id
            )));
        }
        if data_file.content != DataContent::Data || data_file.format != FileFormat::Parquet {
            return Err(TeoDBError::wal(format!(
                "prepared file '{key}' is not a Parquet data file"
            )));
        }
        let unique_path = format!(
            "{:?}|{}|{}",
            data_file.path.scheme,
            data_file
                .path
                .bucket
                .as_deref()
                .unwrap_or_default(),
            key
        );
        if !seen_paths.insert(unique_path) {
            return Err(TeoDBError::wal(format!("prepared intent repeats data file '{key}'")));
        }
        file_records = file_records
            .checked_add(data_file.record_count)
            .ok_or_else(|| TeoDBError::wal("prepared file record count overflow"))?;
    }
    if file_records != prepared.record_count {
        return Err(TeoDBError::wal(format!(
            "prepared file record count {file_records} does not match intent count {}",
            prepared.record_count
        )));
    }
    Ok(())
}

fn path_for(directory: &Path, table_uuid: Uuid) -> PathBuf {
    directory.join(format!("{table_uuid}.json"))
}

fn fsync_directory(directory: &Path) -> TeoDBResult<()> {
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|error| TeoDBError::wal_source("fsync prepared intent directory", error))
}

pub(super) const fn default_max_files() -> usize {
    DEFAULT_MAX_PREPARED_FILES
}

pub(super) const fn default_max_bytes() -> u64 {
    DEFAULT_MAX_PREPARED_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use teodb_core::file::{DataContent, FileFormat};
    use teodb_core::location::{ObjectLocation, StorageScheme};

    fn intent(writer_id: WriterId) -> PreparedFlush {
        let table_uuid = Uuid::now_v7();
        let commit_id = CommitId::now_v7();
        PreparedFlush::new(
            TableIdent::new("analytics", "events"),
            table_uuid,
            writer_id,
            WriterEpoch::new(7),
            commit_id,
            GenerationRange::new(1, 2).unwrap(),
            10,
            1,
            vec![DataFile {
                content: DataContent::Data,
                path: ObjectLocation {
                    scheme: StorageScheme::S3,
                    bucket: Some("warehouse".into()),
                    key: format!("analytics/events/data/{writer_id}/{commit_id}-f0000.parquet"),
                },
                format: FileFormat::Parquet,
                partition_spec_id: 0,
                sort_order_id: None,
                schema_id: 0,
                partition_values: HashMap::new(),
                record_count: 10,
                file_size_bytes: 100,
                column_sizes: HashMap::new(),
                value_counts: HashMap::new(),
                null_value_counts: HashMap::new(),
                nan_value_counts: HashMap::new(),
                lower_bounds: HashMap::new(),
                upper_bounds: HashMap::new(),
                split_offsets: Vec::new(),
                equality_ids: Vec::new(),
                key_metadata: None,
            }],
            None,
        )
    }

    #[test]
    fn sidecar_roundtrips_and_is_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let writer_id = WriterId::from_uuid(Uuid::now_v7());
        let prepared = intent(writer_id);
        persist(
            root.path(),
            &prepared,
            writer_id,
            default_max_files(),
            default_max_bytes(),
        )
        .unwrap();
        persist(
            root.path(),
            &prepared,
            writer_id,
            default_max_files(),
            default_max_bytes(),
        )
        .unwrap();
        assert_eq!(
            list(root.path(), writer_id, default_max_files(), default_max_bytes()).unwrap(),
            vec![prepared.clone()]
        );
        remove(root.path(), prepared.table_uuid).unwrap();
        assert!(
            list(root.path(), writer_id, default_max_files(), default_max_bytes())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn sidecar_accepts_partition_path_before_writer_subtree() {
        let root = tempfile::tempdir().unwrap();
        let writer_id = WriterId::from_uuid(Uuid::now_v7());
        let mut prepared = intent(writer_id);
        prepared.data_files[0].path.key = format!(
            "analytics/events/data/region=eu/{writer_id}/{}-p0000-f0000.parquet",
            prepared.commit_id
        );

        persist(
            root.path(),
            &prepared,
            writer_id,
            default_max_files(),
            default_max_bytes(),
        )
        .unwrap();
        assert_eq!(
            list(root.path(), writer_id, default_max_files(), default_max_bytes()).unwrap(),
            vec![prepared]
        );
    }

    #[test]
    fn sidecar_rejects_empty_files_wrong_commit_and_wrong_writer() {
        let root = tempfile::tempdir().unwrap();
        let writer_id = WriterId::from_uuid(Uuid::now_v7());

        let mut empty = intent(writer_id);
        empty.data_files.clear();
        assert!(persist(root.path(), &empty, writer_id, default_max_files(), default_max_bytes(),).is_err());

        let mut wrong_commit = intent(writer_id);
        wrong_commit.data_files[0].path.key = format!("analytics/events/data/{writer_id}/other-f0000.parquet");
        assert!(
            persist(
                root.path(),
                &wrong_commit,
                writer_id,
                default_max_files(),
                default_max_bytes(),
            )
            .is_err()
        );

        let mut ambiguous_prefix = intent(writer_id);
        ambiguous_prefix.data_files[0].path.key = format!(
            "analytics/events/data/{writer_id}/{}x-f0000.parquet",
            ambiguous_prefix.commit_id
        );
        assert!(
            persist(
                root.path(),
                &ambiguous_prefix,
                writer_id,
                default_max_files(),
                default_max_bytes(),
            )
            .is_err()
        );

        let other_writer = WriterId::from_uuid(Uuid::now_v7());
        assert!(
            persist(
                root.path(),
                &intent(writer_id),
                other_writer,
                default_max_files(),
                default_max_bytes(),
            )
            .is_err()
        );
    }

    #[test]
    fn listing_rejects_corrupt_and_oversized_sidecars() {
        let root = tempfile::tempdir().unwrap();
        let writer_id = WriterId::from_uuid(Uuid::now_v7());
        let table_uuid = Uuid::now_v7();
        let directory = root.path().join(PREPARED_DIR);
        std::fs::create_dir_all(&directory).unwrap();
        let path = path_for(&directory, table_uuid);
        std::fs::write(&path, b"{not-json").unwrap();
        assert!(list(root.path(), writer_id, default_max_files(), default_max_bytes(),).is_err());

        std::fs::write(&path, vec![b'x'; 32]).unwrap();
        assert!(list(root.path(), writer_id, default_max_files(), 16).is_err());
    }
}
