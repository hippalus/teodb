//! Namespace route definitions.

use axum::Router;
use axum::routing::get;
use std::sync::Arc;

use crate::http::state::AppState;

use super::handler;

/// Namespace CRUD routes under `/api/v1/namespaces`.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/api/v1/namespaces",
            get(handler::list_namespaces).post(handler::create_namespace),
        )
        .route(
            "/api/v1/namespaces/{ns}",
            get(handler::get_namespace).delete(handler::drop_namespace),
        )
}
