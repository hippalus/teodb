/// Configuration for per-request DataFusion sessions.
#[derive(Debug, Clone)]
pub struct DataFusionSessionConfig {
    pub batch_size: usize,
    pub target_partitions: usize,
    pub metadata_refresh: std::time::Duration,
}

impl Default for DataFusionSessionConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            target_partitions: std::thread::available_parallelism().map_or(4, |count| count.get()),
            metadata_refresh: std::time::Duration::from_secs(10),
        }
    }
}

/// Process-level resources shared by DataFusion sessions.
#[derive(Debug, Clone)]
pub struct DataFusionRuntimeConfig {
    pub memory_pool_bytes: u64,
    pub spill_dir: std::path::PathBuf,
}

impl Default for DataFusionRuntimeConfig {
    fn default() -> Self {
        Self {
            memory_pool_bytes: 512 * 1024 * 1024,
            spill_dir: std::env::temp_dir().join("teodb-spill"),
        }
    }
}
