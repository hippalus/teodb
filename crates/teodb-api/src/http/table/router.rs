//! Table route definitions.

use axum::Router;
use axum::routing::get;
use std::sync::Arc;

use crate::http::state::AppState;

use super::handler;

/// Table CRUD routes under `/api/v1/namespaces/{ns}/tables`.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/api/v1/namespaces/{ns}/tables",
            get(handler::list_tables).post(handler::create_table),
        )
        .route(
            "/api/v1/namespaces/{ns}/tables/{tbl}",
            get(handler::get_table).delete(handler::drop_table),
        )
}
