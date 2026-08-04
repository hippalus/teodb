//! Compaction file selection policy.
//!
//! Determines which files in a partition are candidates for compaction
//! based on size, count, and delete-file pressure thresholds.

use std::collections::HashMap;
use teodb_core::file::{DataContent, DataFile};
use teodb_core::ident::{FieldId, SnapshotId};
use teodb_core::scalar::TeoScalar;

/// Configuration for the compaction selection policy.
#[derive(Debug, Clone)]
pub struct SelectionConfig {
    /// Target output file size in bytes (default: 128 MiB).
    pub target_file_bytes: u64,
    /// Minimum number of mid-size files to trigger compaction (default: 8).
    pub min_files_per_compaction: usize,
    /// Maximum files per compaction run (default: 64).
    pub max_files_per_compaction: usize,
    /// Maximum total input bytes per compaction run (default: 1 GiB).
    /// Bounds the memory and I/O of a single run — a file-count cap alone
    /// admits runs of max_files × mid-size files (multiple GiB). 0 disables
    /// the budget.
    pub max_bytes_per_compaction: u64,
    /// Delete pressure threshold: fraction of rows covered by delete files (default: 0.25).
    pub delete_pressure_threshold: f64,
}

impl Default for SelectionConfig {
    fn default() -> Self {
        Self {
            target_file_bytes: 128 * 1024 * 1024, // 128 MiB
            min_files_per_compaction: 8,
            max_files_per_compaction: 64,
            max_bytes_per_compaction: 1024 * 1024 * 1024, // 1 GiB
            delete_pressure_threshold: 0.25,
        }
    }
}

/// A group of files within a single partition selected for compaction.
#[derive(Debug, Clone)]
pub struct CompactionGroup {
    pub partition_spec_id: i32,
    pub partition_values: std::collections::HashMap<FieldId, TeoScalar>,
    pub files: Vec<DataFile>,
    /// Position-delete files in this partition. The compaction read path
    /// applies these to the input files so soft-deleted rows are not
    /// resurrected; fully-resolved delete files are removed at commit.
    pub delete_files: Vec<DataFile>,
    pub base_snapshot_id: SnapshotId,
}

/// Select files for compaction from a snapshot's data files.
///
/// Groups files by partition, then within each partition selects files
/// that are too small or under heavy delete pressure.
pub fn select_compaction_candidates(
    data_files: &[DataFile],
    delete_files: &[DataFile],
    base_snapshot_id: SnapshotId,
    config: &SelectionConfig,
) -> Vec<CompactionGroup> {
    select_compaction_candidates_with_delete_counts(data_files, delete_files, base_snapshot_id, config, &HashMap::new())
}

/// Select files using exact delete counts keyed by target data-file path.
pub fn select_compaction_candidates_with_delete_counts(
    data_files: &[DataFile],
    delete_files: &[DataFile],
    base_snapshot_id: SnapshotId,
    config: &SelectionConfig,
    delete_counts: &HashMap<String, u64>,
) -> Vec<CompactionGroup> {
    let small_threshold = config.target_file_bytes / 4;
    let mid_threshold = config.target_file_bytes / 2;

    // Group position-delete files by partition so each candidate group can
    // carry the deletes that apply to its partition's data files.
    let mut deletes_by_partition: HashMap<PartitionKey, Vec<DataFile>> = HashMap::new();
    for d in delete_files {
        if d.content != DataContent::PositionDelete {
            continue;
        }
        let key = PartitionKey {
            spec_id: d.partition_spec_id,
            values: d.partition_values.clone(),
        };
        deletes_by_partition
            .entry(key)
            .or_default()
            .push(d.clone());
    }

    // Group data files by (partition_spec_id, partition_values).
    let mut partitions: HashMap<PartitionKey, Vec<&DataFile>> = HashMap::new();
    for f in data_files {
        if f.content != DataContent::Data {
            continue;
        }
        let key = PartitionKey {
            spec_id: f.partition_spec_id,
            values: f.partition_values.clone(),
        };
        partitions.entry(key).or_default().push(f);
    }

    let mut groups = Vec::new();
    for (key, files) in partitions {
        let candidates = select_within_partition(&files, small_threshold, mid_threshold, delete_counts, config);
        if !candidates.is_empty() {
            let partition_deletes = deletes_by_partition
                .get(&key)
                .cloned()
                .unwrap_or_default();
            groups.push(CompactionGroup {
                partition_spec_id: key.spec_id,
                partition_values: key.values,
                files: candidates,
                delete_files: partition_deletes,
                base_snapshot_id,
            });
        }
    }

    groups
}

#[derive(Debug, Clone)]
struct PartitionKey {
    spec_id: i32,
    values: HashMap<FieldId, TeoScalar>,
}

impl PartialEq for PartitionKey {
    fn eq(&self, other: &Self) -> bool {
        self.spec_id == other.spec_id && self.values == other.values
    }
}

impl Eq for PartitionKey {}

impl std::hash::Hash for PartitionKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.spec_id.hash(state);
        // Hash deterministically by sorting keys
        let mut keys: Vec<_> = self.values.keys().collect();
        keys.sort();
        for k in keys {
            k.hash(state);
            // Use debug representation for hashing since TeoScalar doesn't impl Hash
            format!("{:?}", self.values[k]).hash(state);
        }
    }
}

/// Select candidate files within a single partition.
fn select_within_partition(
    files: &[&DataFile],
    small_threshold: u64,
    mid_threshold: u64,
    delete_counts: &HashMap<String, u64>,
    config: &SelectionConfig,
) -> Vec<DataFile> {
    let mut candidates = Vec::new();

    // Small files are always candidates.
    let small_files: Vec<&DataFile> = files
        .iter()
        .filter(|f| f.file_size_bytes < small_threshold)
        .copied()
        .collect();

    // Mid-size files are candidates only when there are enough of them.
    let mid_files: Vec<&DataFile> = files
        .iter()
        .filter(|f| f.file_size_bytes >= small_threshold && f.file_size_bytes < mid_threshold)
        .copied()
        .collect();

    // Large files under delete pressure are candidates.
    let pressured_files: Vec<&DataFile> = files
        .iter()
        .filter(|f| {
            f.file_size_bytes >= mid_threshold
                && has_delete_pressure(f, delete_counts, config.delete_pressure_threshold)
        })
        .copied()
        .collect();

    candidates.extend(small_files.into_iter().cloned());

    if mid_files.len() >= config.min_files_per_compaction {
        candidates.extend(mid_files.into_iter().cloned());
    }

    candidates.extend(pressured_files.into_iter().cloned());

    // Sort by ascending size so smallest come first, cap at max count and
    // at the bytes budget. Ascending order keeps the most files (the best
    // small-file reduction) inside the budget.
    candidates.sort_by_key(|f| f.file_size_bytes);
    candidates.truncate(config.max_files_per_compaction);
    if config.max_bytes_per_compaction > 0 {
        let mut total = 0u64;
        candidates.retain(|f| {
            total = total.saturating_add(f.file_size_bytes);
            total <= config.max_bytes_per_compaction
        });
    }

    candidates
}

/// Check whether a data file has delete pressure exceeding the threshold.
fn has_delete_pressure(file: &DataFile, delete_counts: &HashMap<String, u64>, threshold: f64) -> bool {
    if file.record_count == 0 {
        return false;
    }
    let count = delete_count_for_file(file, delete_counts);
    (count as f64 / file.record_count as f64) > threshold
}

fn delete_count_for_file(file: &DataFile, delete_counts: &HashMap<String, u64>) -> u64 {
    let key = file.path.key.as_str();
    delete_counts
        .get(&file.path.to_uri())
        .or_else(|| delete_counts.get(key))
        .copied()
        .or_else(|| {
            delete_counts
                .iter()
                .find_map(|(recorded, count)| {
                    (recorded == key
                        || recorded == &file.path.to_uri()
                        || recorded
                            .strip_suffix(key)
                            .is_some_and(|prefix| prefix.ends_with('/')))
                    .then_some(*count)
                })
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use teodb_core::file::FileFormat;
    use teodb_core::location::ObjectLocation;

    fn data_file(path: &str, size: u64, rows: u64) -> DataFile {
        DataFile {
            content: DataContent::Data,
            path: ObjectLocation::parse(path).unwrap(),
            format: FileFormat::Parquet,
            partition_spec_id: 0,
            sort_order_id: Some(1),
            schema_id: 0,
            partition_values: HashMap::new(),
            record_count: rows,
            file_size_bytes: size,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            split_offsets: vec![],
            equality_ids: vec![],
            key_metadata: None,
        }
    }

    fn position_delete_file(path: &str, rows: u64) -> DataFile {
        DataFile {
            content: DataContent::PositionDelete,
            path: ObjectLocation::parse(path).unwrap(),
            format: FileFormat::Parquet,
            partition_spec_id: 0,
            sort_order_id: None,
            schema_id: 0,
            partition_values: HashMap::new(),
            record_count: rows,
            file_size_bytes: 1024,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            split_offsets: vec![],
            equality_ids: vec![],
            key_metadata: None,
        }
    }

    #[test]
    fn small_files_are_always_selected() {
        let config = SelectionConfig::default();
        let files = vec![
            data_file("s3://b/data/a.parquet", 1024, 100),
            data_file("s3://b/data/b.parquet", 2048, 200),
        ];
        let groups = select_compaction_candidates(&files, &[], 42, &config);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].files.len(), 2);
    }

    #[test]
    fn mid_files_need_minimum_count() {
        let config = SelectionConfig {
            min_files_per_compaction: 3,
            ..SelectionConfig::default()
        };
        let mid_size = config.target_file_bytes / 3; // Between small and mid thresholds

        // Only 2 mid files — not enough
        let files = vec![
            data_file("s3://b/data/a.parquet", mid_size, 1000),
            data_file("s3://b/data/b.parquet", mid_size, 1000),
        ];
        let groups = select_compaction_candidates(&files, &[], 42, &config);
        assert!(groups.is_empty());

        // 3 mid files — enough
        let files = vec![
            data_file("s3://b/data/a.parquet", mid_size, 1000),
            data_file("s3://b/data/b.parquet", mid_size, 1000),
            data_file("s3://b/data/c.parquet", mid_size, 1000),
        ];
        let groups = select_compaction_candidates(&files, &[], 42, &config);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].files.len(), 3);
    }

    #[test]
    fn respects_max_files_cap() {
        let config = SelectionConfig {
            max_files_per_compaction: 3,
            ..SelectionConfig::default()
        };
        let files: Vec<DataFile> = (0..10)
            .map(|i| data_file(&format!("s3://b/data/{i}.parquet"), 1024, 100))
            .collect();

        let groups = select_compaction_candidates(&files, &[], 42, &config);
        assert_eq!(groups[0].files.len(), 3);
    }

    #[test]
    fn respects_bytes_budget() {
        let config = SelectionConfig {
            max_bytes_per_compaction: 10 * 1024,
            ..SelectionConfig::default()
        };
        // Five small files of 4 KiB each: only the first two fit the 10 KiB
        // budget (ascending order keeps the smallest).
        let files: Vec<DataFile> = (0..5)
            .map(|i| data_file(&format!("s3://b/data/{i}.parquet"), 4 * 1024, 100))
            .collect();

        let groups = select_compaction_candidates(&files, &[], 42, &config);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].files.len(), 2);
        let total: u64 = groups[0]
            .files
            .iter()
            .map(|f| f.file_size_bytes)
            .sum();
        assert!(total <= config.max_bytes_per_compaction);
    }

    #[test]
    fn zero_bytes_budget_disables_cap() {
        let config = SelectionConfig {
            max_bytes_per_compaction: 0,
            ..SelectionConfig::default()
        };
        let files: Vec<DataFile> = (0..5)
            .map(|i| data_file(&format!("s3://b/data/{i}.parquet"), 4 * 1024, 100))
            .collect();

        let groups = select_compaction_candidates(&files, &[], 42, &config);
        assert_eq!(groups[0].files.len(), 5);
    }

    #[test]
    fn empty_input() {
        let config = SelectionConfig::default();
        let groups = select_compaction_candidates(&[], &[], 42, &config);
        assert!(groups.is_empty());
    }

    #[test]
    fn large_files_without_pressure_skipped() {
        let config = SelectionConfig::default();
        let files = vec![data_file("s3://b/data/a.parquet", config.target_file_bytes, 100_000)];
        let groups = select_compaction_candidates(&files, &[], 42, &config);
        assert!(groups.is_empty());
    }

    #[test]
    fn large_file_selected_with_exact_delete_pressure() {
        let config = SelectionConfig {
            delete_pressure_threshold: 0.25,
            ..SelectionConfig::default()
        };
        let files = vec![data_file("s3://b/data/a.parquet", config.target_file_bytes, 100)];
        let delete_files = vec![position_delete_file("s3://b/data/deletes-a.parquet", 30)];
        let delete_counts = HashMap::from([("data/a.parquet".to_string(), 30)]);

        let groups =
            select_compaction_candidates_with_delete_counts(&files, &delete_files, 42, &config, &delete_counts);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].files[0].path.to_uri(), "s3://b/data/a.parquet");
        assert_eq!(groups[0].delete_files.len(), 1);
    }

    #[test]
    fn delete_file_uri_does_not_create_false_pressure() {
        let config = SelectionConfig {
            delete_pressure_threshold: 0.25,
            ..SelectionConfig::default()
        };
        let files = vec![data_file(
            "s3://b/data/deletes-a.parquet",
            config.target_file_bytes,
            100,
        )];
        let delete_files = vec![position_delete_file("s3://b/data/deletes-a.parquet", 30)];
        let delete_counts = HashMap::from([("data/other.parquet".to_string(), 30)]);

        let groups =
            select_compaction_candidates_with_delete_counts(&files, &delete_files, 42, &config, &delete_counts);

        assert!(groups.is_empty());
    }
}
