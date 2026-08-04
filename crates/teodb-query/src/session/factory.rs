use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionState;
use teodb_core::error::TeoDBResult;
use teodb_core::traits::authz::Principal;
use teodb_core::traits::catalog::Catalog;
use teodb_core::traits::storage::StorageFactory;

use super::{DataFusionRuntime, DataFusionSessionConfig};
use crate::provider::TeoCatalogProvider;

/// Creates request-scoped DataFusion sessions over shared runtime resources.
pub struct DataFusionSessionFactory {
    runtime_env: Arc<RuntimeEnv>,
    config: DataFusionSessionConfig,
    teo_catalog: Arc<TeoCatalogProvider>,
}

impl DataFusionSessionFactory {
    pub fn new(
        catalog: Arc<dyn Catalog>,
        storage_factory: Arc<dyn StorageFactory>,
        runtime: DataFusionRuntime,
        config: DataFusionSessionConfig,
    ) -> TeoDBResult<Self> {
        let teo_catalog =
            Arc::new(TeoCatalogProvider::new(catalog, storage_factory).with_metadata_ttl(config.metadata_refresh));
        Ok(Self {
            runtime_env: runtime.env,
            config,
            teo_catalog,
        })
    }

    pub fn session_state_for_principal(&self, _principal: &Principal) -> TeoDBResult<SessionState> {
        let session_config = datafusion::execution::context::SessionConfig::new()
            .with_batch_size(self.config.batch_size)
            .with_target_partitions(self.config.target_partitions)
            .with_create_default_catalog_and_schema(false);

        let context = SessionContext::new_with_config_rt(session_config, self.runtime_env.clone());
        self.register_teodb_bindings(&context);
        Ok(context.state())
    }

    fn register_teodb_bindings(&self, context: &SessionContext) {
        context.register_udf(crate::udf::url_path_hash_udf());
        context.register_catalog("datafusion", self.teo_catalog.clone());
    }
}
