//! Admin route definitions.

use axum::Router;
use axum::routing::{get, post};
use std::sync::Arc;

use crate::http::common::security::admin_guard;
use crate::http::state::AppState;

use super::handler;

/// Admin routes under `/api/v1/admin/...`, uniformly guarded by
/// [`admin_guard`] (admin bearer token + `Action::Admin` authorization) —
/// handlers contain no auth code.
pub fn routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/v1/admin/status", get(handler::status))
        .route("/api/v1/admin/tables", get(handler::all_tables))
        .route("/api/v1/admin/cluster", get(handler::cluster))
        .route("/api/v1/admin/flush-blocked", get(handler::flush_blocked))
        .route(
            "/api/v1/admin/flush-blocked/{namespace}/{table}/recheck",
            post(handler::recheck_flush_blocked),
        )
        .route_layer(axum::middleware::from_fn_with_state(state, admin_guard))
}
