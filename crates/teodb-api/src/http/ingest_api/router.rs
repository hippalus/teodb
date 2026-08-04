//! Ingest route definitions.

use axum::Router;
use axum::routing::post;
use std::sync::Arc;

use crate::http::state::AppState;

use super::handler;

/// Ingestion routes under `/api/v1/tables/{ns}/{tbl}/...`.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/v1/tables/{ns}/{tbl}/ingest", post(handler::ingest_json))
        .route("/api/v1/tables/{ns}/{tbl}/flush", post(handler::flush_table))
}
