//! Metric creation and registration helpers.

use prometheus::{Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry};

/// Creates an IntCounter. Prometheus creation only fails on invalid names;
/// all names here are compile-time constants, so this is safe.
pub fn counter(name: &str, help: &str) -> IntCounter {
    IntCounter::with_opts(Opts::new(name, help)).expect("compile-time metric name is always valid")
}

pub fn gauge(name: &str, help: &str) -> IntGauge {
    IntGauge::with_opts(Opts::new(name, help)).expect("compile-time metric name is always valid")
}

pub fn gauge_vec(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(name, help), labels).expect("compile-time metric name is always valid")
}

pub fn histogram(name: &str, help: &str, buckets: Vec<f64>) -> Histogram {
    Histogram::with_opts(HistogramOpts::new(name, help).buckets(buckets))
        .expect("compile-time metric name is always valid")
}

pub fn counter_vec(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    IntCounterVec::new(Opts::new(name, help), labels).expect("compile-time metric name is always valid")
}

/// Register a metric with the registry.
/// Registration of unique compile-time names cannot fail.
pub fn register<C: prometheus::core::Collector + Clone + 'static>(registry: &Registry, metric: &C) {
    registry
        .register(Box::new(metric.clone()))
        .expect("metric registration must succeed for unique names");
}
