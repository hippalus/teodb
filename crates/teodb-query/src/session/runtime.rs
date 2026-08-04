use std::sync::Arc;

use datafusion::execution::runtime_env::RuntimeEnv;
use teodb_core::error::{TeoDBError, TeoDBResult};
use url::Url;

use super::DataFusionRuntimeConfig;

#[derive(Clone)]
pub struct ObjectStoreRegistration {
    url: Url,
    store: Arc<dyn object_store::ObjectStore>,
}

impl std::fmt::Debug for ObjectStoreRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreRegistration")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreRegistration {
    pub fn new(url: impl AsRef<str>, store: Arc<dyn object_store::ObjectStore>) -> TeoDBResult<Self> {
        let raw = url.as_ref();
        let url = Url::parse(raw)
            .map_err(|error| TeoDBError::Config(format!("invalid object store URL '{raw}': {error}")))?;
        Ok(Self { url, store })
    }

    pub fn url(&self) -> &str {
        self.url.as_str()
    }

    pub fn parsed_url(&self) -> &Url {
        &self.url
    }

    pub fn store(&self) -> Arc<dyn object_store::ObjectStore> {
        self.store.clone()
    }
}

/// Shared DataFusion runtime resources owned by the composition root.
pub struct DataFusionRuntime {
    pub(super) env: Arc<RuntimeEnv>,
}

impl DataFusionRuntime {
    pub fn try_new(config: &DataFusionRuntimeConfig) -> TeoDBResult<Self> {
        build_runtime_env(config).map(|env| Self { env })
    }

    pub fn register_object_store(&self, url: &str, store: Arc<dyn object_store::ObjectStore>) -> TeoDBResult<()> {
        let parsed = datafusion_execution::object_store::ObjectStoreUrl::parse(url)
            .map_err(|error| TeoDBError::Config(format!("invalid object store URL '{url}': {error}")))?;
        self.env
            .register_object_store(parsed.as_ref(), store);
        Ok(())
    }

    pub fn register_object_store_registration(&self, registration: &ObjectStoreRegistration) -> TeoDBResult<()> {
        self.register_object_store(registration.url(), registration.store())
    }
}

fn build_runtime_env(config: &DataFusionRuntimeConfig) -> TeoDBResult<Arc<RuntimeEnv>> {
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::memory_pool::FairSpillPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    std::fs::create_dir_all(&config.spill_dir)
        .map_err(|error| TeoDBError::Internal(format!("failed to create spill dir: {error}")))?;

    let disk_manager =
        DiskManagerBuilder::default().with_mode(DiskManagerMode::Directories(vec![config.spill_dir.clone()]));

    RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(config.memory_pool_bytes as usize)))
        .with_disk_manager_builder(disk_manager)
        .build_arc()
        .map_err(|error| TeoDBError::Internal(format!("RuntimeEnv build failed: {error}")))
}
