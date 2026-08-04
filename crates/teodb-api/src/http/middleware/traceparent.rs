//! W3C Trace Context propagation middleware.
//!
//! - **Inbound**: Extracts the `traceparent` request header and sets it as the parent
//!   context on the current tracing span, enabling distributed trace continuation.
//! - **Outbound**: Adds a `traceparent` response header with the current span's trace
//!   context per [W3C Trace Context](https://www.w3.org/TR/trace-context/).

use axum::body::Body;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

const TRACEPARENT_HEADER: &str = "traceparent";

/// Middleware that propagates W3C trace context:
/// 1. Extracts inbound `traceparent` header → sets parent span context
/// 2. Injects outbound `traceparent` header on the response
pub async fn inject_traceparent(req: Request<Body>, next: Next) -> Response {
    // Inbound: extract parent context from request headers.
    extract_parent_context(&req);

    let mut resp = next.run(req).await;

    // Outbound: inject current span's trace context into response.
    inject_response_traceparent(&mut resp);

    resp
}

/// Extract the W3C traceparent from request headers and set it as the parent
/// context on the current tracing span.
fn extract_parent_context(req: &Request<Body>) {
    let propagator = TraceContextPropagator::new();
    let extractor = HeaderExtractor(req.headers());
    let parent_ctx = propagator.extract(&extractor);

    // Only set parent if the extracted context has a valid span.
    if parent_ctx.span().span_context().is_valid() {
        let _ = Span::current().set_parent(parent_ctx);
    }
}

/// Inject the current span's trace context as a W3C traceparent response header.
fn inject_response_traceparent(resp: &mut Response) {
    let span = Span::current();
    let ctx = span.context();
    let span_ref = ctx.span();
    let sc = span_ref.span_context();

    if sc.is_valid() {
        let traceparent = format!("00-{}-{}-{:02x}", sc.trace_id(), sc.span_id(), sc.trace_flags().to_u8());

        if let Ok(val) = axum::http::HeaderValue::from_str(&traceparent) {
            resp.headers_mut().insert(TRACEPARENT_HEADER, val);
        }
    }
}

/// Adapter to extract headers from an `axum::http::HeaderMap` for OTel propagation.
struct HeaderExtractor<'a>(&'a axum::http::HeaderMap);

impl opentelemetry::propagation::Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}
