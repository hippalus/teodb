use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::config::{ProcessRole, TeoDBConfig};
use crate::security::AllowListAuthorizer;

struct AllowAllAuthorizer;

#[async_trait::async_trait]
impl teodb_core::traits::authz::Authorizer for AllowAllAuthorizer {
    async fn authorize(
        &self,
        _principal: &teodb_core::traits::authz::Principal,
        _action: &teodb_core::traits::authz::Action,
        _resource: &teodb_core::traits::authz::Resource,
    ) -> teodb_core::error::TeoDBResult<()> {
        Ok(())
    }
}

pub(in crate::server) struct AppStateDependencies {
    pub(in crate::server) catalog: Arc<dyn teodb_core::traits::catalog::Catalog>,
    pub(in crate::server) buffers: Arc<teodb_ingest::buffer::BufferRegistry>,
    pub(in crate::server) wal: Arc<teodb_storage::wal::WalManager>,
    pub(in crate::server) idempotency: Arc<teodb_ingest::idempotency::IdempotencyIndex>,
    pub(in crate::server) query_engine: Arc<dyn teodb_query::QueryEngine>,
    pub(in crate::server) ingest_config: teodb_ingest::config::IngestConfig,
    pub(in crate::server) storage_factory: Arc<dyn teodb_core::traits::storage::StorageFactory>,
    pub(in crate::server) flusher: teodb_ingest::flush::Flusher,
    pub(in crate::server) lifecycle: teodb_core::lifecycle::RoleLifecycle,
    pub(in crate::server) draining: Arc<std::sync::atomic::AtomicBool>,
    pub(in crate::server) api_observer: Arc<dyn teodb_api::ApiObserver>,
}

struct SchedulerReadinessProbe {
    endpoint: String,
}

#[async_trait::async_trait]
impl teodb_api::http::ReadinessProbe for SchedulerReadinessProbe {
    fn name(&self) -> &'static str {
        "scheduler"
    }

    async fn check(&self) -> (bool, String) {
        match tokio::net::TcpStream::connect(&self.endpoint).await {
            Ok(_) => (true, format!("reachable at {}", self.endpoint)),
            Err(error) => (false, format!("unreachable at {}: {error}", self.endpoint)),
        }
    }
}

struct ExecutorQuorumProbe {
    client: teodb_distributed::scheduler_api::SchedulerApiClient,
    min_executors: usize,
    liveness_window: Duration,
}

#[async_trait::async_trait]
impl teodb_api::http::ReadinessProbe for ExecutorQuorumProbe {
    fn name(&self) -> &'static str {
        "executor-quorum"
    }

    async fn check(&self) -> (bool, String) {
        match self
            .client
            .alive_executor_count(self.liveness_window)
            .await
        {
            Ok(alive) if alive >= self.min_executors => {
                (true, format!("{alive} live executors (min {})", self.min_executors))
            }
            Ok(alive) => (
                false,
                format!("executor quorum not met: {alive} live (min {})", self.min_executors),
            ),
            Err(error) => (false, format!("scheduler executor query failed: {error}")),
        }
    }
}

struct SecurityContext {
    authorizer: Option<Arc<dyn teodb_core::traits::authz::Authorizer>>,
    jwt_validator: Option<Arc<teodb_api::security::JwtValidator>>,
}

struct HttpStateBuilder {
    role: String,
    dependencies: AppStateDependencies,
    security: SecurityContext,
    api_config: teodb_api::ApiConfig,
    admin_token: Option<String>,
    readiness_probes: Vec<Arc<dyn teodb_api::http::ReadinessProbe>>,
    cluster_topology: Option<Arc<dyn teodb_core::traits::cluster::ClusterTopology>>,
    query_timeout: Duration,
    slow_query_threshold: Duration,
}

impl HttpStateBuilder {
    fn new(
        role: String,
        dependencies: AppStateDependencies,
        security: SecurityContext,
        api_config: teodb_api::ApiConfig,
    ) -> Self {
        Self {
            role,
            dependencies,
            security,
            api_config,
            admin_token: None,
            readiness_probes: Vec::new(),
            cluster_topology: None,
            query_timeout: Duration::from_secs(30),
            slow_query_threshold: Duration::from_millis(500),
        }
    }

    fn admin_token(mut self, admin_token: Option<String>) -> Self {
        self.admin_token = admin_token;
        self
    }

    fn readiness_probes(mut self, probes: Vec<Arc<dyn teodb_api::http::ReadinessProbe>>) -> Self {
        self.readiness_probes = probes;
        self
    }

    fn cluster_topology(mut self, topology: Option<Arc<dyn teodb_core::traits::cluster::ClusterTopology>>) -> Self {
        self.cluster_topology = topology;
        self
    }

    fn query_limits(mut self, timeout: Duration, slow_query_threshold: Duration) -> Self {
        self.query_timeout = timeout;
        self.slow_query_threshold = slow_query_threshold;
        self
    }

    fn build(self) -> Arc<teodb_api::http::AppState> {
        let warehouse_uri: Arc<str> = Arc::from(
            self.dependencies
                .ingest_config
                .default_warehouse_uri
                .as_str(),
        );
        let ingest = teodb_ingest::service::IngestService::new(
            self.dependencies.catalog.clone(),
            self.dependencies.buffers.clone(),
            self.dependencies.wal.clone(),
            self.dependencies.idempotency.clone(),
            warehouse_uri.clone(),
        );
        let ddl = teodb_api::service::DdlService::new(
            self.dependencies.catalog.clone(),
            self.dependencies.storage_factory.clone(),
            self.dependencies.buffers.clone(),
            self.dependencies.wal.clone(),
            self.dependencies.idempotency.clone(),
            warehouse_uri,
        );
        let api_config = Arc::new(self.api_config);
        let admission = teodb_api::admission::ApiAdmission::new(&api_config);
        let observer = self.dependencies.api_observer;
        let authorization = Arc::new(teodb_api::ApiAuthorization::new(
            self.security.authorizer,
            observer.clone(),
        ));
        let authenticator = Arc::new(teodb_api::security::ApiAuthenticator::new(
            self.security.jwt_validator,
            observer,
        ));

        Arc::new(teodb_api::http::AppState {
            services: teodb_api::http::AppServices {
                catalog: self.dependencies.catalog,
                buffers: self.dependencies.buffers,
                config: api_config,
                ingest,
                ddl,
                flusher: self.dependencies.flusher,
                wal: self.dependencies.wal,
                idempotency: self.dependencies.idempotency,
                query_engine: self.dependencies.query_engine,
            },
            security: teodb_api::http::AppSecurity {
                authorization,
                authenticator,
                admin_token: self.admin_token,
            },
            admission,
            readiness: teodb_api::http::AppReadiness {
                probes: self.readiness_probes,
                cluster_topology: self.cluster_topology,
            },
            lifecycle: teodb_api::http::AppLifecycle {
                role: self.role,
                role_lifecycle: self.dependencies.lifecycle,
                draining: self.dependencies.draining,
                query_timeout: self.query_timeout,
                slow_query_threshold: self.slow_query_threshold,
                started_at: std::time::Instant::now(),
            },
        })
    }
}

pub(in crate::server) fn build_app_state(
    cfg: &TeoDBConfig,
    dependencies: AppStateDependencies,
) -> eyre::Result<Arc<teodb_api::http::AppState>> {
    let security = build_security_context(cfg)?;
    let readiness_probes = build_readiness_probes(cfg)?;
    let cluster_topology = build_cluster_topology(cfg)?;

    Ok(
        HttpStateBuilder::new(cfg.role.to_string(), dependencies, security, cfg.to_api_config())
            .admin_token(cfg.security.admin_token.clone())
            .readiness_probes(readiness_probes)
            .cluster_topology(cluster_topology)
            .query_limits(
                Duration::from_secs(cfg.query.query_timeout_secs),
                Duration::from_millis(cfg.query.slow_query_threshold_ms),
            )
            .build(),
    )
}

fn build_security_context(cfg: &TeoDBConfig) -> eyre::Result<SecurityContext> {
    let authorizer = if cfg.security.mode.allows_anonymous() {
        None
    } else {
        Some(build_authorizer(cfg)?)
    };
    let jwt_validator = build_jwt_validator(cfg)?.map(Arc::new);

    if cfg.security.admin_token.is_none() {
        tracing::warn!("security.admin_token is not set; admin endpoints and /metrics are unauthenticated");
    }
    Ok(SecurityContext {
        authorizer,
        jwt_validator,
    })
}

fn build_authorizer(cfg: &TeoDBConfig) -> eyre::Result<Arc<dyn teodb_core::traits::authz::Authorizer>> {
    if let Some(path) = &cfg.security.allow_list_path {
        let authorizer = AllowListAuthorizer::from_toml_file(path)
            .map_err(|error| eyre::eyre!("failed to load allow-list from {}: {error}", path.display()))?;
        info!(path = %path.display(), "loaded allow-list authorizer");
        return Ok(Arc::new(authorizer));
    }
    Ok(Arc::new(AllowAllAuthorizer))
}

fn build_jwt_validator(cfg: &TeoDBConfig) -> eyre::Result<Option<teodb_api::security::JwtValidator>> {
    let Some(key_path) = &cfg.security.jwt_signing_key else {
        return Ok(None);
    };
    let key = std::fs::read(key_path).map_err(|error| {
        eyre::eyre!(
            "failed to read security.jwt_signing_key {}: {error}",
            key_path.display()
        )
    })?;
    let algorithm: jsonwebtoken::Algorithm = cfg
        .security
        .jwt_algorithm
        .parse()
        .map_err(|error| {
            eyre::eyre!(
                "invalid security.jwt_algorithm '{}': {error}",
                cfg.security.jwt_algorithm
            )
        })?;
    let validator_config = teodb_api::security::JwtValidatorConfig {
        issuer: cfg.security.jwt_issuer.clone(),
        audience: cfg.security.jwt_audience.clone(),
        algorithms: vec![algorithm],
        require_exp: true,
    };

    use jsonwebtoken::Algorithm;
    let validator = match algorithm {
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => {
            teodb_api::security::JwtValidator::with_secret(&key, validator_config)
        }
        Algorithm::RS256
        | Algorithm::RS384
        | Algorithm::RS512
        | Algorithm::PS256
        | Algorithm::PS384
        | Algorithm::PS512 => teodb_api::security::JwtValidator::with_rsa_pem(&key, validator_config)
            .map_err(|error| eyre::eyre!("invalid JWT RSA key: {error}"))?,
        Algorithm::ES256 | Algorithm::ES384 => teodb_api::security::JwtValidator::with_ec_pem(&key, validator_config)
            .map_err(|error| eyre::eyre!("invalid JWT EC key: {error}"))?,
        // `Algorithm` is `#[non_exhaustive]`: reject EdDSA and any variant added by a
        // later jsonwebtoken release rather than loading the key as the wrong type.
        _ => {
            return Err(eyre::eyre!(
                "security.jwt_algorithm '{}' is not supported",
                cfg.security.jwt_algorithm
            ));
        }
    };
    info!(algorithm = %cfg.security.jwt_algorithm, "JWT validator configured");
    Ok(Some(validator))
}

/// Build the cluster-topology provider for the admin endpoint. Present whenever
/// a Ballista scheduler exists to query: `data-node`/`control-plane` roles
/// always, and `standalone` only when it runs an in-process scheduler. Pure
/// standalone has no scheduler, so the admin UI correctly reports no cluster.
fn build_cluster_topology(
    cfg: &TeoDBConfig,
) -> eyre::Result<Option<Arc<dyn teodb_core::traits::cluster::ClusterTopology>>> {
    let has_scheduler = matches!(cfg.role, ProcessRole::DataNode | ProcessRole::ControlPlane)
        || (matches!(cfg.role, ProcessRole::Standalone) && cfg.cluster.scheduler_enabled);
    if !has_scheduler {
        return Ok(None);
    }

    let liveness_window = Duration::from_secs(
        cfg.cluster
            .heartbeat_interval_secs
            .saturating_mul(u64::from(cfg.cluster.heartbeat_miss_threshold))
            .max(1),
    );
    let topology = teodb_distributed::cluster_topology::SchedulerTopology::new(
        &cfg.cluster.scheduler_addr,
        liveness_window,
        Duration::from_secs(5),
    )
    .map_err(|error| eyre::eyre!("invalid scheduler topology endpoint: {error}"))?;
    Ok(Some(Arc::new(topology)))
}

fn build_readiness_probes(cfg: &TeoDBConfig) -> eyre::Result<Vec<Arc<dyn teodb_api::http::ReadinessProbe>>> {
    let probes: Vec<Arc<dyn teodb_api::http::ReadinessProbe>> = match cfg.role {
        ProcessRole::DataNode if cfg.cluster.min_executors > 0 => {
            let liveness_window = Duration::from_secs(
                cfg.cluster
                    .heartbeat_interval_secs
                    .saturating_mul(u64::from(cfg.cluster.heartbeat_miss_threshold))
                    .max(1),
            );
            let client = teodb_distributed::scheduler_api::SchedulerApiClient::new(
                &cfg.cluster.scheduler_addr,
                Duration::from_secs(5),
            )
            .map_err(|error| eyre::eyre!("invalid scheduler readiness endpoint: {error}"))?;
            vec![Arc::new(ExecutorQuorumProbe {
                client,
                min_executors: cfg.cluster.min_executors,
                liveness_window,
            })]
        }
        ProcessRole::DataNode if !cfg.cluster.scheduler_enabled => {
            let scheduler =
                teodb_distributed::ballista::HostPort::parse(&cfg.cluster.scheduler_addr, "cluster.scheduler_addr")
                    .map_err(|error| eyre::eyre!("invalid scheduler readiness endpoint: {error}"))?;
            vec![Arc::new(SchedulerReadinessProbe {
                endpoint: scheduler.authority(),
            })]
        }
        _ => Vec::new(),
    };
    Ok(probes)
}
