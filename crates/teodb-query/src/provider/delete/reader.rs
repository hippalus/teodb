//! Delete file reader: applies Iceberg position-delete files during scan.
//!
//! Position-delete files are Parquet files with two columns:
//! - `file_path` (Utf8): the data file path the delete applies to
//! - `pos` (Int64): the zero-based row position within that data file
//!
//! This module loads one or more position-delete files into an in-memory
//! set for efficient lookup during scan execution.

use std::collections::{HashMap, HashSet};

use arrow::array::{Array, Int64Array, StringArray};
use object_store::ObjectStoreExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::debug;

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::file::{DataContent, DataFile};
use teodb_core::location::ObjectPath;
use teodb_core::traits::storage::Storage;

/// An in-memory index of position deletes, keyed by data file path.
///
/// After loading, callers can efficiently check whether a specific
/// `(file_path, row_position)` pair has been deleted.
pub struct PositionDeleteSet {
    /// Maps data-file path → set of deleted row positions within that file.
    deletes: HashMap<String, HashSet<i64>>,
}

impl PositionDeleteSet {
    /// Create an empty `PositionDeleteSet`.
    pub fn new() -> Self {
        Self {
            deletes: HashMap::new(),
        }
    }

    /// Load position-delete entries from one or more delete files.
    ///
    /// Only files with `content == DataContent::PositionDelete` are processed;
    /// others are silently skipped. Each delete file is fetched from storage
    /// as raw bytes and parsed as Parquet with the expected schema
    /// (`file_path: Utf8`, `pos: Int64`).
    pub async fn load(storage: &dyn Storage, delete_files: &[DataFile], base_path: &ObjectPath) -> TeoDBResult<Self> {
        let mut set = Self::new();

        let position_deletes: Vec<&DataFile> = delete_files
            .iter()
            .filter(|f| f.content == DataContent::PositionDelete)
            .collect();

        if position_deletes.is_empty() {
            return Ok(set);
        }

        debug!(count = position_deletes.len(), "loading position-delete files");

        for df in position_deletes {
            let file_key = &df.path.key;
            let object_path = if base_path.as_str().is_empty() {
                ObjectPath::new(file_key)
            } else {
                ObjectPath::new(format!("{}/{}", base_path.as_str().trim_end_matches('/'), file_key))
            };

            let data = storage.get(&object_path).await?;

            set.parse_position_delete_bytes(&data, &object_path)?;
        }

        debug!(
            total_deletes = set.deleted_count(),
            files_affected = set.deletes.len(),
            "position-delete loading complete"
        );

        Ok(set)
    }

    /// Load position-delete entries from the DataFusion object store registered
    /// for an executor-side scan.
    pub async fn load_from_object_store(
        store: &dyn object_store::ObjectStore,
        delete_files: &[DataFile],
    ) -> TeoDBResult<Self> {
        let mut set = Self::new();

        let position_deletes: Vec<&DataFile> = delete_files
            .iter()
            .filter(|f| f.content == DataContent::PositionDelete)
            .collect();

        if position_deletes.is_empty() {
            return Ok(set);
        }

        debug!(
            count = position_deletes.len(),
            "loading position-delete files from object store"
        );

        for df in position_deletes {
            let object_path = object_store::path::Path::from(df.path.key.clone());
            let data = store
                .get(&object_path)
                .await
                .map_err(|e| TeoDBError::ObjectStore(Box::new(e)))?
                .bytes()
                .await
                .map_err(|e| TeoDBError::ObjectStore(Box::new(e)))?;

            set.parse_position_delete_bytes(&data, &ObjectPath::new(object_path.to_string()))?;
        }

        debug!(
            total_deletes = set.deleted_count(),
            files_affected = set.deletes.len(),
            "position-delete loading complete"
        );

        Ok(set)
    }

    /// Returns `true` if the given `(file_path, row_pos)` has been deleted.
    #[cfg(test)]
    pub fn is_deleted(&self, file_path: &str, row_pos: i64) -> bool {
        self.deletes
            .get(file_path)
            .is_some_and(|positions| positions.contains(&row_pos))
    }

    /// All deleted positions recorded for a data file.
    ///
    /// The Iceberg spec records `file_path` as the data file's full URI,
    /// while TeoDB tracks files by table-relative keys — so entries match
    /// either exactly or when a full-URI entry ends with `/{key}`
    /// (externally-written delete files use absolute paths).
    pub fn positions_for_file(&self, key: &str) -> HashSet<i64> {
        let mut merged = HashSet::new();
        for (entry_path, positions) in &self.deletes {
            let matches = entry_path == key
                || entry_path
                    .strip_suffix(key)
                    .is_some_and(|prefix| prefix.ends_with('/'));
            if matches {
                merged.extend(positions.iter().copied());
            }
        }
        merged
    }

    /// Total number of individual position-delete entries across all files.
    pub fn deleted_count(&self) -> usize {
        self.deletes.values().map(|s| s.len()).sum()
    }

    /// Insert a single delete entry directly (test support).
    #[cfg(test)]
    pub(crate) fn insert_for_test(&mut self, file_path: &str, pos: i64) {
        self.deletes
            .entry(file_path.to_owned())
            .or_default()
            .insert(pos);
    }

    /// Parse a single position-delete Parquet file from raw bytes.
    fn parse_position_delete_bytes(&mut self, data: &[u8], source_path: &ObjectPath) -> TeoDBResult<()> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(data))
            .map_err(|e| TeoDBError::Parquet(format!("open position-delete {source_path}: {e}")))?
            .build()
            .map_err(|e| TeoDBError::Parquet(format!("build reader {source_path}: {e}")))?;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| TeoDBError::Parquet(format!("read batch {source_path}: {e}")))?;

            let file_path_col = batch
                .column_by_name("file_path")
                .ok_or_else(|| TeoDBError::Parquet(format!("missing 'file_path' column in {source_path}")))?;

            let pos_col = batch
                .column_by_name("pos")
                .ok_or_else(|| TeoDBError::Parquet(format!("missing 'pos' column in {source_path}")))?;

            let file_paths = file_path_col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    TeoDBError::Parquet(format!(
                        "'file_path' in {source_path} must be Utf8, found {}",
                        file_path_col.data_type()
                    ))
                })?;
            let positions = pos_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    TeoDBError::Parquet(format!(
                        "'pos' in {source_path} must be Int64, found {}",
                        pos_col.data_type()
                    ))
                })?;

            for row in 0..batch.num_rows() {
                if file_paths.is_null(row) || positions.is_null(row) {
                    return Err(TeoDBError::Parquet(format!(
                        "null position-delete entry at row {row} in {source_path}"
                    )));
                }
                let path = file_paths.value(row);
                let pos = positions.value(row);
                if path.is_empty() {
                    return Err(TeoDBError::Parquet(format!(
                        "empty position-delete file path at row {row} in {source_path}"
                    )));
                }
                if pos < 0 {
                    return Err(TeoDBError::Parquet(format!(
                        "negative position-delete row position {pos} at row {row} in {source_path}"
                    )));
                }
                self.deletes
                    .entry(path.to_owned())
                    .or_default()
                    .insert(pos);
            }
        }

        Ok(())
    }
}

impl Default for PositionDeleteSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
        PutOptions, PutPayload, PutResult, RenameOptions,
    };

    use super::*;
    use teodb_core::file::FileFormat;
    use teodb_core::location::{ObjectLocation, StorageScheme};

    #[derive(Debug, Clone, Copy)]
    enum GetFailure {
        PermissionDenied,
        TimedOut,
    }

    #[derive(Debug)]
    struct FailingGetStore {
        inner: object_store::memory::InMemory,
        failure: GetFailure,
    }

    impl FailingGetStore {
        fn new(failure: GetFailure) -> Self {
            Self {
                inner: object_store::memory::InMemory::new(),
                failure,
            }
        }
    }

    impl std::fmt::Display for FailingGetStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "failing-get")
        }
    }

    #[async_trait]
    #[deny(clippy::missing_trait_methods)]
    impl ObjectStore for FailingGetStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner
                .put_opts(location, payload, options)
                .await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner
                .put_multipart_opts(location, options)
                .await
        }

        async fn get_opts(&self, location: &Path, _options: GetOptions) -> object_store::Result<GetResult> {
            let source = || -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(
                    match self.failure {
                        GetFailure::PermissionDenied => std::io::ErrorKind::PermissionDenied,
                        GetFailure::TimedOut => std::io::ErrorKind::TimedOut,
                    },
                    match self.failure {
                        GetFailure::PermissionDenied => "HTTP 403",
                        GetFailure::TimedOut => "request timed out",
                    },
                ))
            };

            Err(match self.failure {
                GetFailure::PermissionDenied => object_store::Error::PermissionDenied {
                    path: location.to_string(),
                    source: source(),
                },
                GetFailure::TimedOut => object_store::Error::Generic {
                    store: "timeout-test",
                    source: source(),
                },
            })
        }

        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> object_store::Result<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }

    fn position_delete_file(path: &str) -> DataFile {
        DataFile {
            content: DataContent::PositionDelete,
            path: ObjectLocation {
                scheme: StorageScheme::Local,
                bucket: None,
                key: path.into(),
            },
            format: FileFormat::Parquet,
            partition_spec_id: 0,
            sort_order_id: None,
            schema_id: 0,
            partition_values: HashMap::new(),
            record_count: 1,
            file_size_bytes: 16,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            split_offsets: Vec::new(),
            equality_ids: Vec::new(),
            key_metadata: None,
        }
    }

    #[test]
    fn empty_set_reports_no_deletes() {
        let set = PositionDeleteSet::new();
        assert_eq!(set.deleted_count(), 0);
        assert!(!set.is_deleted("any_file.parquet", 0));
    }

    #[test]
    fn default_is_empty() {
        let set = PositionDeleteSet::default();
        assert_eq!(set.deleted_count(), 0);
    }

    #[test]
    fn positions_for_file_matches_relative_and_absolute_entries() {
        let mut set = PositionDeleteSet::new();
        // Entry recorded with the table-relative key.
        set.deletes
            .entry("data/file1.parquet".to_owned())
            .or_default()
            .insert(1);
        // Entry recorded with the full URI (externally-written delete file).
        set.deletes
            .entry("s3://warehouse/ns/tbl/data/file1.parquet".to_owned())
            .or_default()
            .insert(7);
        // Different file with a name that merely *contains* the key —
        // must not match (suffix must start at a path separator).
        set.deletes
            .entry("data/xdata/file1.parquet".to_owned())
            .or_default()
            .insert(9);

        let positions = set.positions_for_file("data/file1.parquet");
        assert!(positions.contains(&1), "relative entry matches");
        assert!(positions.contains(&7), "absolute-URI entry matches");
        assert_eq!(positions.len(), 2);

        assert!(
            set.positions_for_file("file1.parquet")
                .contains(&9),
            "the other file's own key still resolves"
        );
        assert!(
            set.positions_for_file("data/file2.parquet")
                .is_empty()
        );
    }

    #[test]
    fn manual_insert_and_lookup() {
        let mut set = PositionDeleteSet::new();
        set.deletes
            .entry("data/file1.parquet".to_owned())
            .or_default()
            .insert(42);
        set.deletes
            .entry("data/file1.parquet".to_owned())
            .or_default()
            .insert(99);
        set.deletes
            .entry("data/file2.parquet".to_owned())
            .or_default()
            .insert(0);

        assert!(set.is_deleted("data/file1.parquet", 42));
        assert!(set.is_deleted("data/file1.parquet", 99));
        assert!(!set.is_deleted("data/file1.parquet", 50));
        assert!(set.is_deleted("data/file2.parquet", 0));
        assert!(!set.is_deleted("data/file3.parquet", 0));
        assert_eq!(set.deleted_count(), 3);
    }

    #[tokio::test]
    async fn missing_position_delete_file_fails_closed() {
        let store = object_store::memory::InMemory::new();
        let error =
            match PositionDeleteSet::load_from_object_store(&store, &[position_delete_file("delete/missing.parquet")])
                .await
            {
                Ok(_) => panic!("a missing delete file must fail the scan"),
                Err(error) => error,
            };
        assert!(matches!(error, TeoDBError::ObjectStore(_)));
    }

    #[tokio::test]
    async fn missing_position_delete_file_fails_closed_through_storage_adapter() {
        let storage = teodb_test_support::in_memory_backend();
        let error = match PositionDeleteSet::load(
            storage.as_ref(),
            &[position_delete_file("delete/missing.parquet")],
            &ObjectPath::new(""),
        )
        .await
        {
            Ok(_) => panic!("a missing local delete file must fail the scan"),
            Err(error) => error,
        };
        assert!(matches!(error, TeoDBError::ObjectStore(_)));
    }

    #[tokio::test]
    async fn permission_and_timeout_failures_from_executor_store_fail_closed() {
        for (failure, expected_message) in [
            (GetFailure::PermissionDenied, "HTTP 403"),
            (GetFailure::TimedOut, "request timed out"),
        ] {
            let store = FailingGetStore::new(failure);
            let error = match PositionDeleteSet::load_from_object_store(
                &store,
                &[position_delete_file("delete/unavailable.parquet")],
            )
            .await
            {
                Ok(_) => panic!("{failure:?} must fail the scan"),
                Err(error) => error,
            };

            assert!(matches!(error, TeoDBError::ObjectStore(_)));
            assert!(
                error.to_string().contains(expected_message),
                "error must preserve the underlying storage failure: {error}"
            );
        }
    }

    #[tokio::test]
    async fn corrupt_position_delete_file_fails_closed() {
        let store = object_store::memory::InMemory::new();
        let path = object_store::path::Path::from("delete/corrupt.parquet");
        store
            .put(&path, bytes::Bytes::from_static(b"not parquet").into())
            .await
            .unwrap();

        let error =
            match PositionDeleteSet::load_from_object_store(&store, &[position_delete_file(path.as_ref())]).await {
                Ok(_) => panic!("a corrupt delete file must fail the scan"),
                Err(error) => error,
            };
        assert!(matches!(error, TeoDBError::Parquet(_)));
    }
}
