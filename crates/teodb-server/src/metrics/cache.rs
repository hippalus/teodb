//! Cache subsystem metrics.

use prometheus::{IntCounter, IntGauge, Registry};

use super::registry::{counter, gauge, register};

/// Metrics for the SSD read-through cache.
pub struct CacheMetrics {
    pub hits_total: IntCounter,
    pub misses_total: IntCounter,
    pub bytes: IntGauge,
}

impl CacheMetrics {
    pub fn new(registry: &Registry) -> Self {
        let hits_total = counter("teodb_cache_hits_total", "Total cache hits");
        let misses_total = counter("teodb_cache_misses_total", "Total cache misses");
        let bytes = gauge("teodb_cache_bytes", "Current cache size in bytes");

        register(registry, &hits_total);
        register(registry, &misses_total);
        register(registry, &bytes);

        Self {
            hits_total,
            misses_total,
            bytes,
        }
    }
}
