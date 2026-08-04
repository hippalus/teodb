//! Health probe route definitions.

use axum::Router;
use axum::routing::get;
use std::sync::Arc;

use crate::http::state::AppState;

use super::handler;

/// Health probe routes at the root level (no `/api/v1` prefix).
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/live", get(handler::liveness))
        .route("/ready", get(handler::readiness))
}
