//! HTTP API modules and shared request state.

mod admin;
pub(crate) mod common;
mod health;
mod ingest_api;
pub mod middleware;
mod namespace;
mod query;
mod router;
pub mod state;
mod table;

pub use common::error::ApiError;
pub use common::extract::{ApiJson, RequestContext};
pub use common::security::admin_guard;
pub use router::router;
pub use state::{AppLifecycle, AppReadiness, AppSecurity, AppServices, AppState, ReadinessProbe};

pub mod handlers {
    pub use super::admin::{all_tables, cluster, status};
    pub use super::health::{liveness, readiness};
    pub use super::ingest_api::{flush_table, ingest_json};
    pub use super::namespace::{create_namespace, drop_namespace, get_namespace, list_namespaces};
    pub use super::query::{explain_sql, query_sql};
    pub use super::table::{create_table, drop_table, get_table, list_tables};
}
