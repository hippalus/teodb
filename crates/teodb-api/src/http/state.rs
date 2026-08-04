//! Shared application state for all HTTP handlers.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use teodb_core::traits::catalog::Catalog;
use teodb_ingest::buffer::BufferRegistry;
use teodb_ingest::idempotency::IdempotencyIndex;
use teodb_query::QueryEngine;

use crate::authorization::ApiAuthorization;
use crate::config::ApiConfig;

/// Async component probe included in the readiness endpoint.
#[async_trait]
pub trait ReadinessProbe: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    async fn check(&self) -> (bool, String);
}

pub struct AppState {
    pub services: AppServices,
    pub security: AppSecurity,
    pub admission: crate::admission::ApiAdmission,
    pub readiness: AppReadiness,
    pub lifecycle: AppLifecycle,
}

pub struct AppServices {
    pub catalog: Arc<dyn Catalog>,
    pub buffers: Arc<BufferRegistry>,
    pub config: Arc<ApiConfig>,
    pub ingest: teodb_ingest::service::IngestService,
    pub ddl: crate::service::DdlService,
    pub flusher: teodb_ingest::flush::Flusher,
    pub wal: Arc<teodb_storage::wal::WalManager>,
    pub idempotency: Arc<IdempotencyIndex>,
    pub query_engine: Arc<dyn QueryEngine>,
}

pub struct AppSecurity {
    pub authorization: Arc<ApiAuthorization>,
    pub authenticator: Arc<crate::security::ApiAuthenticator>,
    pub admin_token: Option<String>,
}

pub struct AppReadiness {
    pub probes: Vec<Arc<dyn ReadinessProbe>>,
    pub cluster_topology: Option<Arc<dyn teodb_core::traits::cluster::ClusterTopology>>,
}

pub struct AppLifecycle {
    pub role: String,
    pub role_lifecycle: teodb_core::lifecycle::RoleLifecycle,
    pub draining: Arc<AtomicBool>,
    pub query_timeout: Duration,
    pub slow_query_threshold: Duration,
    pub started_at: Instant,
}
