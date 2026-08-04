use std::path::Path;
use std::sync::Arc;

use teodb_api::http::{AppLifecycle, AppReadiness, AppSecurity, AppServices, AppState, router};
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::TableIdent;
use teodb_core::traits::catalog::Catalog;
use teodb_core::traits::storage::StorageFactory;
use teodb_ingest::buffer::BufferRegistry;
use teodb_ingest::config::IngestConfig;
use teodb_ingest::idempotency::IdempotencyIndex;
use teodb_storage::wal::{WalConfig, WalManager, WalRecoveryMode};

use crate::{MockCatalog, stub_storage_factory, table_metadata};

pub struct StubQueryEngine;

#[async_trait::async_trait]
impl teodb_query::QueryEngine for StubQueryEngine {
    async fn prepare(&self, _req: teodb_query::QueryRequest) -> TeoDBResult<teodb_query::QueryHandle> {
        Err(TeoDBError::Internal("stub query engine".into()))
    }

    async fn execute_stream(&self, _handle: teodb_query::QueryHandle) -> TeoDBResult<teodb_query::QueryResultStream> {
        Err(TeoDBError::Internal("stub query engine".into()))
    }

    async fn cancel(&self, _query_id: &teodb_core::query_id::QueryId) -> TeoDBResult<()> {
        Ok(())
    }

    async fn status(
        &self,
        _query_id: &teodb_core::query_id::QueryId,
    ) -> TeoDBResult<teodb_core::traits::query_engine::QueryStatus> {
        Err(TeoDBError::NotFound {
            resource: "query".into(),
        })
    }
}

pub struct TestApp {
    pub router: axum::Router,
    pub state: Arc<AppState>,
    pub wal_dir: tempfile::TempDir,
}

impl TestApp {
    pub fn into_router_and_wal_dir(self) -> (axum::Router, tempfile::TempDir) {
        (self.router, self.wal_dir)
    }
}

pub struct TestAppBuilder {
    catalog: Arc<dyn Catalog>,
    config: IngestConfig,
    authorizer: Option<Arc<dyn teodb_core::traits::authz::Authorizer>>,
    admin_token: Option<String>,
    storage_factory: Arc<dyn StorageFactory>,
    role: String,
    query_timeout: std::time::Duration,
    slow_query_threshold: std::time::Duration,
    query_engine: Arc<dyn teodb_query::QueryEngine>,
    api_config: teodb_api::ApiConfig,
    observer: Arc<dyn teodb_api::ApiObserver>,
    jwt_validator: Option<Arc<teodb_api::security::JwtValidator>>,
}

impl Default for TestAppBuilder {
    fn default() -> Self {
        Self::rest_api()
    }
}

impl TestAppBuilder {
    pub fn rest_api() -> Self {
        let metadata = table_metadata("file:///data/events");
        let catalog: Arc<dyn Catalog> = Arc::new(
            MockCatalog::builder()
                .namespaces(["default", "analytics"])
                .tables([TableIdent::new("default", "events")])
                .serves("events", metadata.clone())
                .commit_result(metadata)
                .build(),
        );
        Self {
            catalog,
            config: rest_api_config(),
            authorizer: None,
            admin_token: None,
            storage_factory: stub_storage_factory(),
            role: "test".into(),
            query_timeout: std::time::Duration::from_secs(60),
            slow_query_threshold: std::time::Duration::from_millis(5000),
            query_engine: Arc::new(StubQueryEngine),
            api_config: teodb_api::ApiConfig {
                max_body_bytes: 1024 * 1024,
                ..teodb_api::ApiConfig::default()
            },
            observer: Arc::new(teodb_api::NoopApiObserver),
            jwt_validator: None,
        }
    }

    pub fn catalog(mut self, catalog: Arc<dyn Catalog>) -> Self {
        self.catalog = catalog;
        self
    }

    pub fn config(mut self, config: IngestConfig) -> Self {
        self.config = config;
        self
    }

    pub fn authorizer(mut self, authorizer: Option<Arc<dyn teodb_core::traits::authz::Authorizer>>) -> Self {
        self.authorizer = authorizer;
        self
    }

    pub fn admin_token(mut self, admin_token: Option<String>) -> Self {
        self.admin_token = admin_token;
        self
    }

    pub fn storage_factory(mut self, storage_factory: Arc<dyn StorageFactory>) -> Self {
        self.storage_factory = storage_factory;
        self
    }

    pub fn role(mut self, role: impl Into<String>) -> Self {
        self.role = role.into();
        self
    }

    pub fn query_limits(
        mut self,
        query_timeout: std::time::Duration,
        slow_query_threshold: std::time::Duration,
    ) -> Self {
        self.query_timeout = query_timeout;
        self.slow_query_threshold = slow_query_threshold;
        self
    }

    pub fn query_engine(mut self, query_engine: Arc<dyn teodb_query::QueryEngine>) -> Self {
        self.query_engine = query_engine;
        self
    }

    pub fn api_config(mut self, api_config: teodb_api::ApiConfig) -> Self {
        self.api_config = api_config;
        self
    }

    pub fn observer(mut self, observer: Arc<dyn teodb_api::ApiObserver>) -> Self {
        self.observer = observer;
        self
    }

    pub fn jwt_validator(mut self, validator: Arc<teodb_api::security::JwtValidator>) -> Self {
        self.jwt_validator = Some(validator);
        self
    }

    pub async fn build(self) -> TestApp {
        let wal_dir = tempfile::tempdir().expect("wal tempdir");
        let state = self
            .build_state(wal_dir.path(), WalRecoveryMode::Fail)
            .await;
        TestApp {
            router: router(state.clone()),
            state,
            wal_dir,
        }
    }

    async fn build_state(self, wal_dir: &Path, recovery_mode: WalRecoveryMode) -> Arc<AppState> {
        build_state(StateBuild {
            catalog: self.catalog,
            config: self.config,
            authorizer: self.authorizer,
            admin_token: self.admin_token,
            storage_factory: self.storage_factory,
            role: self.role,
            query_timeout: self.query_timeout,
            slow_query_threshold: self.slow_query_threshold,
            query_engine: self.query_engine,
            api_config: self.api_config,
            observer: self.observer,
            jwt_validator: self.jwt_validator,
            wal_dir,
            recovery_mode,
        })
        .await
    }
}

pub struct TestNode {
    pub router: axum::Router,
    pub state: Arc<AppState>,
}

pub struct TestNodeBuilder<'a> {
    wal_dir: &'a Path,
    catalog: Arc<dyn Catalog>,
    config: IngestConfig,
    storage_factory: Arc<dyn StorageFactory>,
    recovery_mode: WalRecoveryMode,
    role: String,
    query_timeout: std::time::Duration,
    slow_query_threshold: std::time::Duration,
    query_engine: Arc<dyn teodb_query::QueryEngine>,
    api_config: teodb_api::ApiConfig,
    observer: Arc<dyn teodb_api::ApiObserver>,
    jwt_validator: Option<Arc<teodb_api::security::JwtValidator>>,
}

impl<'a> TestNodeBuilder<'a> {
    pub fn new(wal_dir: &'a Path) -> Self {
        Self {
            wal_dir,
            catalog: Arc::new(MockCatalog::empty()),
            config: IngestConfig::default(),
            storage_factory: stub_storage_factory(),
            recovery_mode: WalRecoveryMode::Fail,
            role: "test".into(),
            query_timeout: std::time::Duration::from_secs(10),
            slow_query_threshold: std::time::Duration::from_secs(5),
            query_engine: Arc::new(StubQueryEngine),
            api_config: teodb_api::ApiConfig {
                max_body_bytes: 1024 * 1024,
                ..teodb_api::ApiConfig::default()
            },
            observer: Arc::new(teodb_api::NoopApiObserver),
            jwt_validator: None,
        }
    }

    pub fn catalog(mut self, catalog: Arc<dyn Catalog>) -> Self {
        self.catalog = catalog;
        self
    }

    pub fn config(mut self, config: IngestConfig) -> Self {
        self.config = config;
        self
    }

    pub fn storage_factory(mut self, storage_factory: Arc<dyn StorageFactory>) -> Self {
        self.storage_factory = storage_factory;
        self
    }

    pub fn recovery_mode(mut self, recovery_mode: WalRecoveryMode) -> Self {
        self.recovery_mode = recovery_mode;
        self
    }

    pub async fn build(self) -> TestNode {
        let state = build_state(StateBuild {
            catalog: self.catalog,
            config: self.config,
            authorizer: None,
            admin_token: None,
            storage_factory: self.storage_factory,
            role: self.role,
            query_timeout: self.query_timeout,
            slow_query_threshold: self.slow_query_threshold,
            query_engine: self.query_engine,
            api_config: self.api_config,
            observer: self.observer,
            jwt_validator: self.jwt_validator,
            wal_dir: self.wal_dir,
            recovery_mode: self.recovery_mode,
        })
        .await;
        TestNode {
            router: router(state.clone()),
            state,
        }
    }
}

struct StateBuild<'a> {
    catalog: Arc<dyn Catalog>,
    config: IngestConfig,
    authorizer: Option<Arc<dyn teodb_core::traits::authz::Authorizer>>,
    admin_token: Option<String>,
    storage_factory: Arc<dyn StorageFactory>,
    role: String,
    query_timeout: std::time::Duration,
    slow_query_threshold: std::time::Duration,
    query_engine: Arc<dyn teodb_query::QueryEngine>,
    api_config: teodb_api::ApiConfig,
    observer: Arc<dyn teodb_api::ApiObserver>,
    jwt_validator: Option<Arc<teodb_api::security::JwtValidator>>,
    wal_dir: &'a Path,
    recovery_mode: WalRecoveryMode,
}

async fn build_state(args: StateBuild<'_>) -> Arc<AppState> {
    let lifecycle = teodb_core::lifecycle::RoleLifecycle::new();
    lifecycle.mark_ready();

    let wal = Arc::new(
        WalManager::open(WalConfig {
            root_dir: args.wal_dir.to_path_buf(),
            max_segment_bytes: 16 * 1024 * 1024,
            fsync_on_append: false,
            soft_watermark_bytes: 64 * 1024 * 1024,
            hard_cap_bytes: 256 * 1024 * 1024,
            recovery_mode: args.recovery_mode,
            ..Default::default()
        })
        .await
        .expect("open WAL"),
    );
    let buffers = Arc::new(BufferRegistry::new(
        wal.clone(),
        args.config.buffer_max_bytes,
        args.config.buffer_soft_watermark_bytes,
    ));
    let idempotency = Arc::new(IdempotencyIndex::new(
        args.config.idempotency_ttl,
        args.config.idempotency_max_keys_per_table,
    ));
    let warehouse_uri: Arc<str> = Arc::from(args.config.default_warehouse_uri.as_str());
    let ingest = teodb_ingest::service::IngestService::new(
        args.catalog.clone(),
        buffers.clone(),
        wal.clone(),
        idempotency.clone(),
        warehouse_uri.clone(),
    );
    let ddl = teodb_api::service::DdlService::new(
        args.catalog.clone(),
        args.storage_factory.clone(),
        buffers.clone(),
        wal.clone(),
        idempotency.clone(),
        warehouse_uri,
    );
    let flusher = teodb_ingest::flush::Flusher::new(
        buffers.clone(),
        args.catalog.clone(),
        args.storage_factory.clone(),
        wal.clone(),
    );
    let api_config = Arc::new(args.api_config);
    let admission = teodb_api::admission::ApiAdmission::new(&api_config);
    let observer = args.observer;

    Arc::new(AppState {
        services: AppServices {
            catalog: args.catalog,
            buffers,
            config: api_config,
            ingest,
            ddl,
            flusher,
            wal,
            idempotency,
            query_engine: args.query_engine,
        },
        security: AppSecurity {
            authorization: Arc::new(teodb_api::ApiAuthorization::new(args.authorizer, observer.clone())),
            authenticator: Arc::new(teodb_api::security::ApiAuthenticator::new(args.jwt_validator, observer)),
            admin_token: args.admin_token,
        },
        admission,
        readiness: AppReadiness {
            probes: Vec::new(),
            cluster_topology: None,
        },
        lifecycle: AppLifecycle {
            role: args.role,
            role_lifecycle: lifecycle,
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            query_timeout: args.query_timeout,
            slow_query_threshold: args.slow_query_threshold,
            started_at: std::time::Instant::now(),
        },
    })
}

fn rest_api_config() -> IngestConfig {
    IngestConfig {
        buffer_max_bytes: 1024 * 1024,
        buffer_soft_watermark_bytes: 768 * 1024,
        flush_interval: std::time::Duration::from_secs(60),
        default_warehouse_uri: "s3://test-warehouse".into(),
        idempotency_ttl: std::time::Duration::from_secs(60),
        idempotency_max_keys_per_table: 1000,
        commit_status_check: Default::default(),
    }
}
