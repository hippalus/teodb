//! HTTP router construction.

use std::sync::Arc;

use crate::config::TeoDBConfig;
use crate::metrics::Metrics;

use super::collectors;

pub fn build_http_router(
    app_state: &Arc<teodb_api::http::AppState>,
    metrics: &Arc<Metrics>,
    cache_index: Option<Arc<teodb_storage::cache::index::CacheIndex>>,
    cfg: &TeoDBConfig,
) -> axum::Router {
    let m = metrics.clone();
    let metrics_handler = {
        let m = metrics.clone();
        move || {
            let m = m.clone();
            async move { m.encode() }
        }
    };

    // `/metrics` shares the admin guard (admin bearer token + Admin authz),
    // applied at the router level like the /api/v1/admin routes.
    let metrics_routes = axum::Router::new()
        .route("/metrics", axum::routing::get(metrics_handler))
        .route_layer(axum::middleware::from_fn_with_state(
            app_state.clone(),
            teodb_api::http::admin_guard,
        ));

    let api = teodb_api::http::router(app_state.clone()).merge(metrics_routes);

    let query_total = metrics.query.total.clone();
    let query_duration = metrics.query.duration_seconds.clone();
    let ingest_batches = metrics.ingest.batches_total.clone();
    let ingest_errors = metrics.ingest.errors_total.clone();
    let ingest_bytes = metrics.ingest.bytes_total.clone();

    let api = api.layer(axum::middleware::from_fn(
        move |req: axum::http::Request<axum::body::Body>, next: axum::middleware::Next| {
            let qt = query_total.clone();
            let qd = query_duration.clone();
            let ib = ingest_batches.clone();
            let ie = ingest_errors.clone();
            let ibytes = ingest_bytes.clone();
            async move {
                let path = req.uri().path().to_string();
                let content_len = req
                    .headers()
                    .get(axum::http::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
                let start = std::time::Instant::now();
                let resp: axum::response::Response = next.run(req).await;
                let elapsed = start.elapsed().as_secs_f64();

                if path.contains("/query") {
                    let status = if resp.status().is_success() { "ok" } else { "error" };
                    qt.with_label_values(&[status]).inc();
                    qd.observe(elapsed);
                } else if path.contains("/ingest") {
                    if resp.status().is_success() {
                        ib.inc();
                        ibytes.inc_by(content_len);
                    } else {
                        ie.inc();
                    }
                }

                resp
            }
        },
    ));

    let max_requests = cfg.server.max_http_in_flight_requests;
    let admission_observer = app_state.security.authorization.clone();
    let api = api.layer(
        tower::ServiceBuilder::new()
            .layer(axum::error_handling::HandleErrorLayer::new(
                move |_error: tower::BoxError| {
                    let admission_observer = admission_observer.clone();
                    async move {
                        admission_observer.admission_rejection(teodb_api::ApiTransport::Rest, "global_concurrency");
                        (
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            "HTTP request concurrency limit reached",
                        )
                    }
                },
            ))
            .load_shed()
            .concurrency_limit(max_requests),
    );

    collectors::spawn_gauge_collector(m, app_state.services.buffers.clone(), cache_index);
    collectors::spawn_uptime_ticker(metrics.clone());

    api.fallback(axum::routing::get(super::ui::fallback_handler))
}
