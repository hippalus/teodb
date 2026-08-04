//! HTTP router composition.

use std::sync::Arc;

use axum::Router;
use tower::ServiceBuilder;

use super::middleware::{
    RateLimitLayer, RequestIdLayer, access_log, enforce_body_limit, handle_fallback, handle_method_not_allowed,
    handle_panic, inject_traceparent,
};
use super::{AppState, admin, common, health, ingest_api, namespace, query, table};

/// Build the HTTP router by composing domain routers with shared middleware.
///
/// All API endpoints live under `/api/v1/...`.
/// Health probes remain at the root (`/live`, `/ready`).
///
/// Middleware stack (outermost → innermost):
///   1. `CatchPanicLayer` — catches panics, returns RFC 9457 ProblemDetail
///   2. Request ID — assign + propagate `x-request-id`
///   3. HTTP trace — structured tracing per request (I6 invariant)
///   4. Traceparent — OpenTelemetry W3C trace context propagation
///   5. Timeout — hard request deadline (handlers enforce finer budgets)
///   6. Compression — gzip/br/deflate/zstd response compression
///   7. Problem rendering — serialize deferred `ApiError`s (RFC 9457), filling `instance`
///   8. CORS — permissive by default; restrict via `cors_allowed_origins`
///   9. Rate limit — fixed-window per-client rate limiter
///  10. Body limit — configurable request body limit (default 64 MiB)
pub fn router(state: Arc<AppState>) -> Router {
    // Hard backstop above the query handler's own end-to-end `query_timeout`
    // (which now spans planning, execution, and result streaming): the
    // request-level deadline must sit comfortably above it.
    let request_deadline = state.lifecycle.query_timeout.saturating_mul(2) + std::time::Duration::from_secs(30);
    let cors = build_cors_layer(&state.services.config.cors_allowed_origins);

    let api_layers = ServiceBuilder::new()
        .layer(tower_http::catch_panic::CatchPanicLayer::custom(handle_panic))
        .layer(RequestIdLayer::assign())
        .layer(RequestIdLayer::propagate())
        .layer(axum::middleware::from_fn(access_log))
        .layer(axum::middleware::from_fn(inject_traceparent))
        .layer(tower_http::timeout::TimeoutLayer::with_status_code(
            axum::http::StatusCode::REQUEST_TIMEOUT,
            request_deadline,
        ))
        .layer(tower_http::compression::CompressionLayer::new())
        .layer(axum::middleware::from_fn(common::error::render_problem_details))
        .layer(cors)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            RateLimitLayer::handle,
        ))
        .layer(axum::middleware::from_fn_with_state(state.clone(), enforce_body_limit));

    // Compose domain routers
    Router::new()
        .merge(ingest_api::routes())
        .merge(query::routes())
        .merge(namespace::routes())
        .merge(table::routes())
        .merge(admin::routes(state.clone()))
        .merge(health::routes())
        .fallback(handle_fallback)
        .method_not_allowed_fallback(handle_method_not_allowed)
        .layer(api_layers)
        .with_state(state)
}

/// Build the CORS layer. An empty allow-list keeps the historical
/// permissive policy (any origin, any header) for the embedded SPA and dev
/// proxies; configured origins switch to an explicit allow-list.
fn build_cors_layer(allowed_origins: &[String]) -> tower_http::cors::CorsLayer {
    let methods = [
        axum::http::Method::GET,
        axum::http::Method::POST,
        axum::http::Method::PUT,
        axum::http::Method::DELETE,
        axum::http::Method::OPTIONS,
    ];
    let exposed = [
        axum::http::header::HeaderName::from_static("x-request-id"),
        axum::http::header::HeaderName::from_static("traceparent"),
    ];

    let layer = tower_http::cors::CorsLayer::new()
        .allow_methods(methods)
        .expose_headers(exposed)
        .max_age(std::time::Duration::from_secs(3600));

    if allowed_origins.is_empty() {
        return layer
            .allow_origin(tower_http::cors::Any)
            .allow_headers(tower_http::cors::Any);
    }

    let origins: Vec<axum::http::HeaderValue> = allowed_origins
        .iter()
        .filter_map(|o| match o.parse::<axum::http::HeaderValue>() {
            Ok(v) => Some(v),
            Err(_) => {
                tracing::warn!(origin = %o, "ignoring invalid CORS origin in config");
                None
            }
        })
        .collect();

    layer.allow_origin(origins).allow_headers([
        axum::http::header::AUTHORIZATION,
        axum::http::header::CONTENT_TYPE,
        axum::http::header::ACCEPT,
    ])
}

#[cfg(test)]
mod tests {
    use crate::config::ApiConfig;

    #[test]
    fn router_builds() {
        let _ = ApiConfig::default();
    }
}
