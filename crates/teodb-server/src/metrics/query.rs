//! Query subsystem metrics.

use prometheus::{Histogram, IntCounter, IntCounterVec, IntGauge, Registry};

use super::registry::{counter, counter_vec, gauge, histogram, register};

/// Metrics for the query execution engine.
#[allow(dead_code)]
pub struct QueryMetrics {
    pub total: IntCounterVec,
    pub duration_seconds: Histogram,
    /// Total rows returned across all queries.
    pub rows_returned_total: IntCounter,
    /// Number of queries currently in-flight.
    pub active_queries: IntGauge,
    /// Query errors by error category.
    pub errors: IntCounterVec,
    /// Query planning duration.
    pub plan_duration_seconds: Histogram,
    /// Queries that fell back to data-node-local execution because the remote
    /// scheduler was unreachable.
    pub local_fallback_total: IntCounter,
}

impl QueryMetrics {
    pub fn new(registry: &Registry) -> Self {
        let total = counter_vec("teodb_query_total", "Total queries by status", &["status"]);
        let duration_seconds = histogram(
            "teodb_query_duration_seconds",
            "Query execution duration",
            vec![0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0],
        );
        let rows_returned_total = counter("teodb_query_rows_returned_total", "Total rows returned by queries");
        let active_queries = gauge("teodb_query_active", "Number of currently executing queries");
        let errors = counter_vec("teodb_query_errors_total", "Query errors by category", &["category"]);
        let plan_duration_seconds = histogram(
            "teodb_query_plan_duration_seconds",
            "Query planning duration",
            vec![0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
        );
        let local_fallback_total = counter(
            "teodb_query_local_fallback_total",
            "Queries that fell back to data-node-local execution because the scheduler was unreachable",
        );

        register(registry, &total);
        register(registry, &duration_seconds);
        register(registry, &rows_returned_total);
        register(registry, &active_queries);
        register(registry, &errors);
        register(registry, &plan_duration_seconds);
        register(registry, &local_fallback_total);

        Self {
            total,
            duration_seconds,
            rows_returned_total,
            active_queries,
            errors,
            plan_duration_seconds,
            local_fallback_total,
        }
    }
}
