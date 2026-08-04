use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub cache_dir: PathBuf,
    pub spill_dir: PathBuf,
    /// Maximum total cache size in bytes. Set to 0 to disable caching.
    pub cache_max_bytes: u64,
    /// Maximum size of a single cached object in bytes.
    pub cache_max_per_object_bytes: u64,
    /// S3 endpoint URL (e.g. `http://localhost:19000` for RustFS).
    /// Falls back to `AWS_ENDPOINT_URL` env var if not set.
    pub s3_endpoint: Option<String>,
    /// S3 access key ID. Falls back to `AWS_ACCESS_KEY_ID` env var if not set.
    pub s3_access_key: Option<String>,
    /// S3 secret access key. Falls back to `AWS_SECRET_ACCESS_KEY` env var if not set.
    pub s3_secret_key: Option<String>,
    /// S3 region (e.g. "us-east-1"). Falls back to `AWS_REGION` env var if not set.
    pub s3_region: Option<String>,
    /// Allow plain HTTP for S3 (needed for a local RustFS/S3 endpoint). Default: false.
    #[serde(default)]
    pub s3_allow_http: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WalConfig {
    pub max_segment_bytes: u64,
    pub fsync_on_append: bool,
    pub soft_watermark_bytes: u64,
    pub hard_cap_bytes: u64,
    /// How WAL replay responds to a structurally corrupt segment:
    /// `"fail"` (default) aborts startup so an operator can decide;
    /// `"salvage"` quarantines the segment as `*.wal.corrupt`, keeps the
    /// records decoded before the corruption, and continues.
    pub recovery_mode: teodb_storage::wal::WalRecoveryMode,
    /// Maximum immutable data-file descriptors in one prepared sidecar.
    pub max_prepared_files: usize,
    /// Maximum serialized prepared sidecar size.
    pub max_prepared_bytes: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            cache_dir: PathBuf::from("./data/cache"),
            spill_dir: PathBuf::from("./data/spill"),
            cache_max_bytes: 10 * 1024 * 1024 * 1024,      // 10 GiB
            cache_max_per_object_bytes: 512 * 1024 * 1024, // 512 MiB
            s3_endpoint: None,
            s3_access_key: None,
            s3_secret_key: None,
            s3_region: None,
            s3_allow_http: false,
        }
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: 256 * 1024 * 1024,
            fsync_on_append: true,
            soft_watermark_bytes: 4 * 1024 * 1024 * 1024,
            hard_cap_bytes: 8 * 1024 * 1024 * 1024,
            recovery_mode: teodb_storage::wal::WalRecoveryMode::Fail,
            max_prepared_files: 10_000,
            max_prepared_bytes: 16 * 1024 * 1024,
        }
    }
}
