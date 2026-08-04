//! HTTP request/response access logging.
//!
//! A single `from_fn` middleware that wraps each request in an `http` span.
//! The span carries a stable task name and request ID. Parameterized and
//! unmatched routes also carry the actual path. Every error response logs its
//! full cause chain and application call site when typed diagnostics exist.
//!
//! High-frequency probe/poll endpoints (`/live`, `/ready`, `/metrics`, and the
//! admin `status`/`cluster` polls the SPA drives) are silenced on success to
//! keep logs readable; their client/server errors are still logged.
//!
//! SQL is never logged here — that happens in the handler layer at DEBUG.

use axum::body::Body;
use axum::extract::MatchedPath;
use axum::http::{Method, Request};
use axum::middleware::Next;
use axum::response::Response;
use tracing::Instrument;

use crate::http::common::error::ErrorLogContext;

use super::request_id::REQUEST_ID_HEADER;

const ACCESS_LOG_TARGET: &str = "teodb_api::access";
const UNKNOWN_TASK: &str = "http.request";

/// Emit one access-log line per request inside a task/route/request span.
///
/// The span instruments the inner service, so `inject_traceparent` and handlers
/// observe it as `Span::current()`. Successful responses on silenced probe paths
/// produce no log line.
pub async fn access_log(req: Request<Body>, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let matched_route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_owned());
    let route = matched_route.as_deref().unwrap_or(&path);
    let task = request_task_name(&method, route);
    let request_id = req
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-")
        .to_owned();

    let span = tracing::info_span!("http", task, request_id = %request_id, path = tracing::field::Empty);
    if task == UNKNOWN_TASK || route != path {
        span.record("path", path.as_str());
    }

    let start = std::time::Instant::now();
    let response = next.run(req).instrument(span.clone()).await;
    let latency_ms = start.elapsed().as_millis();
    let status = response.status().as_u16();

    let _entered = span.enter();
    match status {
        200..=399 if is_silenced(&path) => {}
        200..=399 => tracing::info!(target: ACCESS_LOG_TARGET, status, latency_ms, "response"),
        _ => log_response_error(&response, status, latency_ms),
    }

    response
}

fn log_response_error(response: &Response, status: u16, latency_ms: u128) {
    let Some(context) = response.extensions().get::<ErrorLogContext>() else {
        log_untyped_error(status, latency_ms);
        return;
    };
    let diagnostics = &context.diagnostics;
    let error_location = format!(
        "{}:{}:{}",
        diagnostics.origin_file, diagnostics.origin_line, diagnostics.origin_column
    );
    let trace_id = context.trace_id.as_deref().unwrap_or("-");

    if status >= 500 {
        tracing::error!(
            target: ACCESS_LOG_TARGET,
            status,
            latency_ms,
            error_code = diagnostics.error_code,
            error_chain = %diagnostics.chain,
            error_location = %error_location,
            trace_id,
            "request failed"
        );
    } else {
        tracing::warn!(
            target: ACCESS_LOG_TARGET,
            status,
            latency_ms,
            error_code = diagnostics.error_code,
            error_chain = %diagnostics.chain,
            error_location = %error_location,
            trace_id,
            "request failed"
        );
    }
}

fn log_untyped_error(status: u16, latency_ms: u128) {
    if status >= 500 {
        tracing::error!(
            target: ACCESS_LOG_TARGET,
            status,
            latency_ms,
            "request failed without typed diagnostic context"
        );
    } else {
        tracing::warn!(
            target: ACCESS_LOG_TARGET,
            status,
            latency_ms,
            "client error"
        );
    }
}

fn request_task_name(method: &Method, route: &str) -> &'static str {
    match (method.as_str(), route) {
        ("POST", "/api/v1/query") => "query.execute",
        ("POST", "/api/v1/query/explain") => "query.explain",
        ("GET", "/live") => "health.live",
        ("GET", "/ready") => "health.ready",
        ("GET", "/metrics") => "metrics.scrape",
        ("GET", "/api/v1/namespaces") => "namespace.list",
        ("POST", "/api/v1/namespaces") => "namespace.create",
        ("GET", "/api/v1/namespaces/{ns}") => "namespace.get",
        ("DELETE", "/api/v1/namespaces/{ns}") => "namespace.drop",
        ("GET", "/api/v1/namespaces/{ns}/tables") => "table.list",
        ("POST", "/api/v1/namespaces/{ns}/tables") => "table.create",
        ("GET", "/api/v1/namespaces/{ns}/tables/{tbl}") => "table.get",
        ("DELETE", "/api/v1/namespaces/{ns}/tables/{tbl}") => "table.drop",
        ("POST", "/api/v1/tables/{ns}/{tbl}/ingest") => "ingest.write",
        ("POST", "/api/v1/tables/{ns}/{tbl}/flush") => "ingest.flush",
        ("GET", "/api/v1/admin/status") => "admin.status.get",
        ("GET", "/api/v1/admin/tables") => "admin.tables.list",
        ("GET", "/api/v1/admin/cluster") => "admin.cluster.get",
        ("GET", "/api/v1/admin/flush-blocked") => "admin.flush_blocked.list",
        ("POST", "/api/v1/admin/flush-blocked/{namespace}/{table}/recheck") => "admin.flush_blocked.recheck",
        _ => UNKNOWN_TASK,
    }
}

/// High-frequency probe/poll endpoints whose successful responses are not worth
/// logging. Health probes and Prometheus scrapes hit these continuously, and the
/// admin UI polls `status`/`cluster` on a timer.
fn is_silenced(path: &str) -> bool {
    matches!(
        path,
        "/live" | "/ready" | "/metrics" | "/api/v1/admin/status" | "/api/v1/admin/cluster"
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone, Default)]
    struct LogBuffer(Arc<Mutex<Vec<u8>>>);

    struct LogWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for LogWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("log buffer lock")
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for LogBuffer {
        type Writer = LogWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            LogWriter(self.0.clone())
        }
    }

    impl LogBuffer {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().expect("log buffer lock").clone()).expect("UTF-8 log output")
        }
    }

    #[test]
    fn probe_and_poll_paths_are_silenced() {
        for path in [
            "/live",
            "/ready",
            "/metrics",
            "/api/v1/admin/status",
            "/api/v1/admin/cluster",
        ] {
            assert!(is_silenced(path), "{path} should be silenced");
        }
    }

    #[test]
    fn domain_paths_are_not_silenced() {
        for path in ["/api/v1/query", "/api/v1/tables/ns/t/ingest", "/api/v1/admin/tables"] {
            assert!(!is_silenced(path), "{path} should be logged");
        }
    }

    #[test]
    fn task_name_is_semantic_and_stable() {
        assert_eq!(
            request_task_name(&Method::POST, "/api/v1/tables/{ns}/{tbl}/ingest"),
            "ingest.write"
        );
        assert_eq!(
            request_task_name(&Method::GET, "/api/v1/admin/tables"),
            "admin.tables.list"
        );
        assert_eq!(request_task_name(&Method::PATCH, "/unknown"), UNKNOWN_TASK);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn access_log_contains_matched_task_chain_and_location() {
        use tower::ServiceExt;

        let logs = LogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_target(false)
            .with_writer(logs.clone())
            .finish();
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);
        let app = axum::Router::new()
            .route(
                "/api/v1/tables/{ns}/{tbl}/ingest",
                axum::routing::post(|| async {
                    let mut response = Response::builder()
                        .status(500)
                        .body(Body::empty())
                        .expect("500 response");
                    response.extensions_mut().insert(ErrorLogContext {
                        diagnostics: crate::http::common::error::ErrorDiagnostics {
                            error_code: "Wal",
                            chain: Arc::from("wal: checkpoint persistence failed -> caused by: disk is read-only"),
                            origin_file: "crates/teodb-ingest/src/flush/commit.rs",
                            origin_line: 142,
                            origin_column: 17,
                        },
                        trace_id: Some("01HTESTTRACE".into()),
                    });
                    response
                }),
            )
            .layer(axum::middleware::from_fn(access_log));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/tables/acme/orders/ingest")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), 500);

        let output = logs.contents();
        assert!(output.contains(r#""task":"ingest.write""#));
        assert!(output.contains(r#""path":"/api/v1/tables/acme/orders/ingest""#));
        assert!(!output.contains(r#""method""#));
        assert!(!output.contains(r#""route""#));
        assert!(
            output.contains(r#""error_chain":"wal: checkpoint persistence failed -> caused by: disk is read-only""#)
        );
        assert!(output.contains(r#""error_location":"crates/teodb-ingest/src/flush/commit.rs:142:17""#));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn static_success_log_has_no_duplicate_route_fields() {
        use tower::ServiceExt;

        let logs = LogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_target(false)
            .with_writer(logs.clone())
            .finish();
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);
        let app = axum::Router::new()
            .route("/api/v1/admin/tables", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(access_log));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/admin/tables")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), 200);

        let output = logs.contents();
        assert!(output.contains(r#""task":"admin.tables.list""#));
        assert!(!output.contains(r#""method""#));
        assert!(!output.contains(r#""route""#));
        assert!(!output.contains(r#""path""#));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn untyped_client_error_does_not_invent_diagnostics() {
        use tower::ServiceExt;

        let logs = LogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_target(false)
            .with_writer(logs.clone())
            .finish();
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);
        let app = axum::Router::new()
            .route(
                "/api/v1/query",
                axum::routing::post(|| async {
                    Response::builder()
                        .status(400)
                        .body(Body::empty())
                        .expect("400 response")
                }),
            )
            .layer(axum::middleware::from_fn(access_log));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/query")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), 400);

        let output = logs.contents();
        assert!(output.contains(r#""task":"query.execute""#));
        assert!(output.contains("client error"));
        assert!(!output.contains(r#""error_code""#));
        assert!(!output.contains(r#""error_chain""#));
        assert!(!output.contains(r#""error_location""#));
    }
}
