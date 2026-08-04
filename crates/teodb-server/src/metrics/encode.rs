//! Prometheus text encoding.

use prometheus::Registry;

/// Encode all metrics in a registry as Prometheus text format.
///
/// Returns an empty string if encoding somehow fails (should never happen
/// because `Vec<u8>` I/O is infallible and Prometheus text is valid UTF-8).
pub fn encode_prometheus(registry: &Registry) -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let families = registry.gather();
    let mut buf = Vec::with_capacity(4096);
    if let Err(e) = encoder.encode(&families, &mut buf) {
        tracing::error!("prometheus encoding failed (should be infallible): {e}");
        return String::new();
    }
    String::from_utf8(buf).unwrap_or_else(|e| {
        tracing::error!("prometheus output was not valid UTF-8: {e}");
        String::new()
    })
}
