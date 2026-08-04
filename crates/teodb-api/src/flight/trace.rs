//! W3C Trace Context propagation for Arrow Flight gRPC metadata.
//!
//! Extracts `traceparent` and `tracestate` headers from gRPC request
//! metadata and creates tracing spans that are linked to the parent trace.
//! This ensures distributed traces flow across Flight client→server boundaries.

use opentelemetry::propagation::Extractor;
use tonic::metadata::MetadataMap;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Extractor that reads W3C trace context headers from tonic `MetadataMap`.
struct MetadataExtractor<'a>(&'a MetadataMap);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|k| match k {
                tonic::metadata::KeyRef::Ascii(key) => Some(key.as_str()),
                tonic::metadata::KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

/// Extract W3C trace context from Flight gRPC metadata and attach it to
/// the current tracing span as a parent context.
///
/// Call this at the start of Flight RPC handlers to propagate distributed traces.
pub fn extract_trace_context(metadata: &MetadataMap, span: &Span) {
    let extractor = MetadataExtractor(metadata);
    let parent_cx = opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(&extractor));
    let _ = span.set_parent(parent_cx);
}

/// Extract the `authorization` header value from Flight gRPC metadata.
/// Returns the bearer token if present, or `None`.
pub fn extract_bearer_token(metadata: &MetadataMap) -> Option<String> {
    metadata
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(String::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataMap;

    #[test]
    fn extract_bearer_token_present() {
        let mut map = MetadataMap::new();
        map.insert("authorization", "Bearer my-token-123".parse().unwrap());
        assert_eq!(extract_bearer_token(&map), Some("my-token-123".into()));
    }

    #[test]
    fn extract_bearer_token_missing() {
        let map = MetadataMap::new();
        assert_eq!(extract_bearer_token(&map), None);
    }

    #[test]
    fn extract_bearer_token_non_bearer() {
        let mut map = MetadataMap::new();
        map.insert("authorization", "Basic dXNlcjpwYXNz".parse().unwrap());
        assert_eq!(extract_bearer_token(&map), None);
    }

    #[test]
    fn metadata_extractor_reads_ascii_keys() {
        let mut map = MetadataMap::new();
        map.insert("traceparent", "00-abc-def-01".parse().unwrap());
        let extractor = MetadataExtractor(&map);
        assert_eq!(extractor.get("traceparent"), Some("00-abc-def-01"));
        assert!(extractor.keys().contains(&"traceparent"));
    }
}
