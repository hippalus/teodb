//! Query route definitions.

use axum::Router;
use axum::routing::post;
use std::sync::Arc;

use crate::http::state::AppState;

use super::{execute, explain};

/// SQL query routes under `/api/v1/query/...`.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/v1/query", post(execute::query_sql))
        .route("/api/v1/query/explain", post(explain::explain_sql))
}
