//! RFC 9457 Problem Details response adapter.

use axum::Json;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use tracing::Span;

use teodb_core::error::TeoDBError;
use teodb_core::problem::ProblemDetail;

const PROBLEM_JSON: &str = "application/problem+json";

/// Wraps a [`ProblemDetail`] so it implements axum's `IntoResponse`
/// with the correct `Content-Type: application/problem+json` header.
///
/// Note: The `x-request-id` header is injected by the `RequestIdLayer` middleware
/// on all responses (including errors), so it does not need to be set here.
pub struct ProblemResponse {
    pub detail: ProblemDetail,
}

impl ProblemResponse {
    pub fn new(detail: ProblemDetail) -> Self {
        Self { detail }
    }
}

impl IntoResponse for ProblemResponse {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.detail.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut resp = (status, Json(&self.detail)).into_response();
        // "application/problem+json" is a valid header value — parse cannot fail.
        if let Ok(value) = PROBLEM_JSON.parse() {
            resp.headers_mut()
                .insert(header::CONTENT_TYPE, value);
        }
        resp
    }
}

/// Extract the OpenTelemetry trace ID from the current span, if available.
pub(crate) fn current_trace_id() -> Option<String> {
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let span = Span::current();
    let ctx = span.context();
    let span_ref = ctx.span();
    let trace_id = span_ref.span_context().trace_id();
    if trace_id == opentelemetry::trace::TraceId::INVALID {
        None
    } else {
        Some(trace_id.to_string())
    }
}

/// Build a `ProblemResponse` from a `TeoDBError`, attaching the request path
/// as the `instance` field and the current trace ID.
pub fn problem_from_error(e: TeoDBError, instance: &str) -> ProblemResponse {
    let mut pd = e.to_problem_detail().with_instance(instance);
    if let Some(tid) = current_trace_id() {
        pd = pd.with_trace_id(tid);
    }
    ProblemResponse::new(pd)
}
