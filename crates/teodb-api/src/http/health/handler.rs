//! Liveness and readiness probe endpoint handlers.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use teodb_core::problem::Link;

use crate::http::common::hateoas::HateoasResponse;
use crate::http::state::AppState;

use super::model::HealthResponse;

/// GET /live — Liveness probe (Richardson Level 2: proper verb + status).
pub async fn liveness() -> Response {
    let resp = HateoasResponse::new(HealthResponse { status: "alive".into() })
        .with_link("self", Link::new("/live").with_method("GET"))
        .with_link(
            "readiness",
            Link::new("/ready")
                .with_method("GET")
                .with_title("Readiness probe"),
        );

    (StatusCode::OK, Json(resp)).into_response()
}

/// GET /ready — Readiness probe: checks lifecycle state and dependencies.
pub async fn readiness(State(state): State<Arc<AppState>>) -> Response {
    let lifecycle_state = state.lifecycle.role_lifecycle.state();

    // If draining or in a terminal state, return 503.
    if !lifecycle_state.is_serving()
        || state
            .lifecycle
            .draining
            .load(std::sync::atomic::Ordering::Relaxed)
    {
        let body = serde_json::json!({
            "status": lifecycle_state.to_string(),
            "checks": [],
            "_links": {
                "self": { "href": "/ready", "method": "GET" },
                "liveness": { "href": "/live", "method": "GET", "title": "Liveness probe" }
            }
        });
        return (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
    }

    let mut checks: Vec<(String, bool, String)> = Vec::with_capacity(4 + state.readiness.probes.len());

    let catalog_ok = match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        state.services.catalog.list_namespaces(),
    )
    .await
    {
        Ok(Ok(_)) => {
            checks.push(("catalog".into(), true, "connected".into()));
            true
        }
        Ok(Err(e)) => {
            checks.push(("catalog".into(), false, format!("error: {e}")));
            false
        }
        Err(_) => {
            checks.push(("catalog".into(), false, "timeout after 5s".into()));
            false
        }
    };

    let wal_ok = match state.services.wal.segment_count() {
        Ok(count) => {
            checks.push(("wal".into(), true, format!("healthy ({count} segments)")));
            true
        }
        Err(e) => {
            checks.push(("wal".into(), false, format!("error: {e}")));
            false
        }
    };

    checks.push(("query_engine".into(), true, "available".into()));
    checks.push(("storage".into(), true, "available".into()));

    let mut extra_ok = true;
    for probe in &state.readiness.probes {
        match tokio::time::timeout(std::time::Duration::from_secs(2), probe.check()).await {
            Ok((ok, detail)) => {
                checks.push((probe.name().into(), ok, detail));
                extra_ok &= ok;
            }
            Err(_) => {
                checks.push((probe.name().into(), false, "timeout after 2s".into()));
                extra_ok = false;
            }
        }
    }

    // Buffer backlog check — informational, doesn't fail readiness.
    let table_count = state.services.buffers.table_count();
    checks.push(("buffers".into(), true, format!("{table_count} tables tracked")));

    let all_ok = catalog_ok && wal_ok && extra_ok;
    let status_code = if all_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let status_str = if all_ok { "ready" } else { "not_ready" };

    let check_details: Vec<serde_json::Value> = checks
        .iter()
        .map(|(name, ok, detail)| {
            serde_json::json!({
                "name": name,
                "status": if *ok { "pass" } else { "fail" },
                "detail": detail,
            })
        })
        .collect();

    let body = serde_json::json!({
        "status": status_str,
        "checks": check_details,
        "_links": {
            "self": { "href": "/ready", "method": "GET" },
            "liveness": { "href": "/live", "method": "GET", "title": "Liveness probe" }
        }
    });

    (status_code, Json(body)).into_response()
}
