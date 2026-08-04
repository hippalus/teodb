//! Periodic background tasks for metrics collection.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::Instrument;

use crate::metrics::Metrics;

/// Bridges Iceberg commit protocol events to Prometheus metrics.
pub struct MetricsCatalogObserver {
    pub metrics: Arc<Metrics>,
}

impl teodb_catalog::CatalogObserver for MetricsCatalogObserver {
    fn on_append_commit(&self, outcome: teodb_catalog::CatalogCommitOutcome, duration: Duration) {
        self.metrics
            .catalog
            .commit_total
            .with_label_values(&[outcome.as_str()])
            .inc();
        self.metrics
            .catalog
            .commit_duration_seconds
            .observe(duration.as_secs_f64());
    }

    fn on_append_rebase(&self, rebases: u32) {
        self.metrics
            .catalog
            .commit_rebase_total
            .inc_by(u64::from(rebases));
    }

    fn on_status_check(&self, outcome: teodb_catalog::CatalogStatusCheckOutcome, duration: Duration) {
        self.metrics
            .catalog
            .status_check_total
            .with_label_values(&[outcome.as_str()])
            .inc();
        self.metrics
            .catalog
            .status_check_duration_seconds
            .observe(duration.as_secs_f64());
    }

    fn on_writer_checkpoint_parse_failure(&self) {
        self.metrics
            .catalog
            .writer_checkpoint_parse_failure_total
            .inc();
    }

    fn on_writer_checkpoint_count(&self, count: usize) {
        self.metrics
            .catalog
            .writer_checkpoint_count
            .set(i64::try_from(count).unwrap_or(i64::MAX));
    }
}

/// Bridges flush events to Prometheus metrics.
pub struct MetricsFlushObserver {
    pub metrics: Arc<Metrics>,
}

impl teodb_ingest::flush::FlushObserver for MetricsFlushObserver {
    fn on_flush_complete(
        &self,
        table: &teodb_core::ident::TableIdent,
        rows: u64,
        oldest_committed_created_at_ms: Option<i64>,
        duration: Duration,
    ) {
        self.metrics.flush.total.inc();
        self.metrics.flush.rows_total.inc_by(rows);
        self.metrics
            .flush
            .inflight
            .with_label_values(&["committed"])
            .inc();
        self.metrics
            .flush
            .duration_seconds
            .observe(duration.as_secs_f64());
        if let Some(created_at_ms) = oldest_committed_created_at_ms {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(i64::MAX as u128) as i64;
            self.metrics
                .flush
                .visibility_lag_seconds
                .with_label_values(&[&table.namespace, &table.name])
                .set(now_ms.saturating_sub(created_at_ms) / 1_000);
        }
    }

    fn on_flush_empty(&self, duration: Duration) {
        self.metrics.flush.total.inc();
        self.metrics
            .flush
            .inflight
            .with_label_values(&["empty"])
            .inc();
        self.metrics
            .flush
            .duration_seconds
            .observe(duration.as_secs_f64());
    }

    fn on_flush_error(&self) {
        self.metrics.flush.total.inc();
        self.metrics.flush.errors_total.inc();
        self.metrics
            .flush
            .inflight
            .with_label_values(&["error"])
            .inc();
    }

    fn on_data_file_write(&self, duration: Duration) {
        self.metrics
            .flush
            .data_file_write_duration_seconds
            .observe(duration.as_secs_f64());
    }

    fn on_flush_lock_wait(&self, duration: Duration) {
        self.metrics
            .flush
            .lock_wait_seconds
            .observe(duration.as_secs_f64());
    }

    fn on_flush_blocked(&self, reason: &'static str) {
        self.metrics
            .flush
            .blocked_total
            .with_label_values(&[reason])
            .inc();
    }

    fn on_blocked_resolution(&self, outcome: &'static str) {
        self.metrics
            .flush
            .blocked_resolution_total
            .with_label_values(&[outcome])
            .inc();
    }
}

pub struct MetricsApiObserver {
    pub metrics: Arc<Metrics>,
}

impl teodb_api::ApiObserver for MetricsApiObserver {
    fn on_authentication(&self, transport: teodb_api::ApiTransport, outcome: &'static str, reason: &'static str) {
        self.metrics
            .security
            .auth_total
            .with_label_values(&[transport.as_str(), outcome, reason])
            .inc();
    }

    fn on_authorization(
        &self,
        transport: teodb_api::ApiTransport,
        outcome: &'static str,
        action: &teodb_core::traits::authz::Action,
        resource: &teodb_core::traits::authz::Resource,
    ) {
        use teodb_core::traits::authz::{Action, Resource};
        let action = match action {
            Action::CreateTable => "create_table",
            Action::DropTable => "drop_table",
            Action::AlterTable => "alter_table",
            Action::Ingest => "ingest",
            Action::Query => "query",
            Action::Compact => "compact",
            Action::Admin => "admin",
        };
        let resource_kind = match resource {
            Resource::Cluster => "cluster",
            Resource::Namespace(_) => "namespace",
            Resource::Table(_) => "table",
        };
        self.metrics
            .security
            .authz_total
            .with_label_values(&[transport.as_str(), outcome, action, resource_kind])
            .inc();
    }

    fn on_result_bytes(&self, transport: teodb_api::ApiTransport, operation: &'static str, bytes: u64) {
        self.metrics
            .transport
            .result_bytes_total
            .with_label_values(&[transport.as_str(), operation])
            .inc_by(bytes);
    }

    fn on_admission_rejection(&self, transport: teodb_api::ApiTransport, reason: &'static str) {
        self.metrics
            .transport
            .admission_rejections_total
            .with_label_values(&[transport.as_str(), reason])
            .inc();
    }

    fn on_write_rejection(&self, reason: &'static str) {
        self.metrics
            .ingest
            .rejected_writes_total
            .with_label_values(&[reason])
            .inc();
    }
}

/// Bridges query-engine events to Prometheus metrics.
pub struct MetricsEngineEventObserver {
    pub metrics: Arc<Metrics>,
}

impl teodb_distributed::EngineEventObserver for MetricsEngineEventObserver {
    fn on_local_fallback(&self, _query_id: &teodb_core::query_id::QueryId, _error: &str) {
        self.metrics.query.local_fallback_total.inc();
    }
}

/// Bridges WAL-replay events to Prometheus metrics.
pub struct MetricsReplayObserver {
    pub metrics: Arc<Metrics>,
}

impl teodb_ingest::replay::ReplayObserver for MetricsReplayObserver {
    fn on_batch_replayed(&self, rows: u64) {
        self.metrics.ingest.rows_total.inc_by(rows);
    }

    fn on_record(&self, outcome: &'static str) {
        self.metrics
            .wal
            .replay_records_total
            .with_label_values(&[outcome])
            .inc();
    }

    fn on_recovery_failure(&self, reason: &'static str) {
        self.metrics
            .wal
            .recovery_failure_total
            .with_label_values(&[reason])
            .inc();
    }

    fn on_flush_blocked(&self, reason: &'static str) {
        self.metrics
            .flush
            .blocked_total
            .with_label_values(&[reason])
            .inc();
    }

    fn on_replay_complete(&self, records: usize, duration: Duration) {
        let _ = records;
        self.metrics
            .wal
            .replay_duration_seconds
            .observe(duration.as_secs_f64());
    }
}

/// Spawn a background task that periodically collects buffer and cache gauges.
pub fn spawn_gauge_collector(
    metrics: Arc<Metrics>,
    buffers: Arc<teodb_ingest::buffer::BufferRegistry>,
    cache_index: Option<Arc<teodb_storage::cache::index::CacheIndex>>,
) {
    tokio::spawn(
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(15));
            let mut previous_tables = std::collections::HashSet::new();
            loop {
                interval.tick().await;
                collect_gauges_once(&metrics, &buffers, cache_index.as_deref(), &mut previous_tables);
            }
        }
        .instrument(tracing::info_span!("gauge_collector")),
    );
}

pub(super) fn collect_gauges_once(
    metrics: &Metrics,
    buffers: &teodb_ingest::buffer::BufferRegistry,
    cache_index: Option<&teodb_storage::cache::index::CacheIndex>,
    previous_tables: &mut std::collections::HashSet<teodb_core::ident::TableIdent>,
) {
    let tables = buffers.tables();
    metrics
        .buffer
        .tables
        .set(i64::try_from(tables.len()).unwrap_or(i64::MAX));
    let mut total_bytes = 0_u64;
    let mut total_entries = 0_usize;
    let mut total_reserved_bytes = 0_u64;
    let mut prepared_flushes = 0_usize;
    let mut blocked_tables = 0_usize;
    let mut oldest_prepared_age_seconds = 0_i64;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64;
    for ident in &tables {
        if let Some(buffer) = buffers.get(ident) {
            let stats = buffer.buffer_stats();
            total_bytes = total_bytes
                .saturating_add(stats.pending_bytes)
                .saturating_add(stats.in_flight_bytes)
                .saturating_add(stats.recently_committed_bytes);
            total_reserved_bytes = total_reserved_bytes.saturating_add(stats.reserved_bytes);
            total_entries = total_entries
                .saturating_add(stats.pending_entries)
                .saturating_add(stats.in_flight_entries);
            if let Some(created_at_ms) = stats.oldest_uncommitted_created_at_ms {
                metrics
                    .buffer
                    .oldest_pending_age_seconds
                    .with_label_values(&[&ident.namespace, &ident.name])
                    .set(now_ms.saturating_sub(created_at_ms) / 1_000);
            } else {
                let _ = metrics
                    .buffer
                    .oldest_pending_age_seconds
                    .remove_label_values(&[&ident.namespace, &ident.name]);
            }
            if let Some(prepared) = buffer.prepared_flush() {
                prepared_flushes = prepared_flushes.saturating_add(1);
                oldest_prepared_age_seconds =
                    oldest_prepared_age_seconds.max(now_ms.saturating_sub(prepared.prepared_at_ms) / 1_000);
            }
            blocked_tables = blocked_tables.saturating_add(usize::from(buffer.blocked_flush().is_some()));
        }
    }
    metrics
        .buffer
        .bytes
        .set(i64::try_from(total_bytes).unwrap_or(i64::MAX));
    metrics
        .buffer
        .reserved_bytes
        .set(i64::try_from(total_reserved_bytes).unwrap_or(i64::MAX));
    metrics
        .buffer
        .entries
        .set(i64::try_from(total_entries).unwrap_or(i64::MAX));
    let current_tables = tables
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
    for stale in previous_tables.difference(&current_tables) {
        let labels = [stale.namespace.as_str(), stale.name.as_str()];
        let _ = metrics
            .buffer
            .oldest_pending_age_seconds
            .remove_label_values(&labels);
        let _ = metrics
            .flush
            .visibility_lag_seconds
            .remove_label_values(&labels);
    }
    *previous_tables = current_tables;
    metrics
        .flush
        .prepared_flushes
        .set(i64::try_from(prepared_flushes).unwrap_or(i64::MAX));
    metrics
        .flush
        .blocked_tables
        .set(i64::try_from(blocked_tables).unwrap_or(i64::MAX));
    metrics
        .flush
        .prepared_oldest_age_seconds
        .set(oldest_prepared_age_seconds);
    metrics.buffer.evicted_rows_total.inc_by(
        buffers
            .evicted_rows_total()
            .saturating_sub(metrics.buffer.evicted_rows_total.get()),
    );

    if let Some(cache_index) = cache_index {
        metrics
            .cache
            .bytes
            .set(i64::try_from(cache_index.total_bytes()).unwrap_or(i64::MAX));
        metrics.cache.hits_total.inc_by(
            cache_index
                .hits()
                .saturating_sub(metrics.cache.hits_total.get()),
        );
        metrics.cache.misses_total.inc_by(
            cache_index
                .misses()
                .saturating_sub(metrics.cache.misses_total.get()),
        );
    }
}

/// Spawn an uptime ticker that increments the uptime gauge every second.
pub fn spawn_uptime_ticker(metrics: Arc<Metrics>) {
    tokio::spawn(
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                metrics.uptime_seconds.inc();
            }
        }
        .instrument(tracing::info_span!("uptime_ticker")),
    );
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use teodb_api::ApiObserver as _;
    use teodb_ingest::flush::FlushObserver as _;

    use super::*;

    #[test]
    fn api_metrics_use_only_bounded_non_secret_labels() {
        let metrics = Arc::new(Metrics::new());
        let observer = MetricsApiObserver {
            metrics: metrics.clone(),
        };
        observer.on_authentication(teodb_api::ApiTransport::Rest, "failed", "invalid");
        observer.on_authorization(
            teodb_api::ApiTransport::Flight,
            "denied",
            &teodb_core::traits::authz::Action::Query,
            &teodb_core::traits::authz::Resource::Table(teodb_core::ident::TableIdent::new(
                "secret_namespace",
                "secret_table",
            )),
        );
        observer.on_result_bytes(teodb_api::ApiTransport::Rest, "query", 123);
        observer.on_result_bytes(teodb_api::ApiTransport::Flight, "query", 456);
        observer.on_admission_rejection(teodb_api::ApiTransport::Rest, "request_body");
        observer.on_write_rejection("buffer_capacity");
        metrics
            .transport
            .active_connections
            .with_label_values(&["flight"])
            .set(2);

        let encoded = metrics.encode();
        for expected in [
            "teodb_auth_total",
            "reason=\"invalid\"",
            "teodb_authz_total",
            "action=\"query\"",
            "resource_kind=\"table\"",
            "teodb_transport_result_bytes_total",
            "operation=\"query\"",
            "teodb_transport_admission_rejections_total",
            "reason=\"request_body\"",
            "teodb_ingest_rejected_writes_total",
            "reason=\"buffer_capacity\"",
            "teodb_transport_active_connections",
            "transport=\"flight\"",
        ] {
            assert!(encoded.contains(expected), "missing metric fragment {expected}");
        }
        for forbidden in [
            "secret_namespace",
            "secret_table",
            "super-secret-token",
            "subject=",
            "request_id=",
        ] {
            assert!(
                !encoded.contains(forbidden),
                "secret or unbounded label leaked: {forbidden}"
            );
        }
    }

    #[tokio::test]
    async fn collector_removes_dropped_table_series() {
        let metrics = Arc::new(Metrics::new());
        let wal_dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(
            teodb_storage::wal::WalManager::open(teodb_storage::wal::WalConfig {
                root_dir: wal_dir.path().to_path_buf(),
                fsync_on_append: false,
                ..Default::default()
            })
            .await
            .unwrap(),
        );
        let buffers = teodb_ingest::buffer::BufferRegistry::new(wal, 1024 * 1024, 768 * 1024);
        let catalog = teodb_test_support::MockCatalog::builder()
            .serves_any(teodb_test_support::table_metadata("s3://warehouse/metrics/events"))
            .build();
        let table = teodb_core::ident::TableIdent::new("metrics", "events");
        let buffer = buffers
            .get_or_load(&table, &catalog)
            .await
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap();
        let created_at_ms = chrono::Utc::now().timestamp_millis() - 2_000;
        let insert_reservation = buffer.reserve(&batch).unwrap();
        buffer.insert_reserved_at(uuid::Uuid::now_v7(), insert_reservation, created_at_ms, batch.clone());
        let held_reservation = buffer.reserve(&batch).unwrap();

        MetricsFlushObserver {
            metrics: metrics.clone(),
        }
        .on_flush_complete(&table, 1, Some(created_at_ms), Duration::from_millis(10));
        let mut previous = HashSet::new();
        collect_gauges_once(&metrics, &buffers, None, &mut previous);
        assert!(metrics.buffer.reserved_bytes.get() > 0);
        let before = metrics.encode();
        assert!(before.contains("teodb_buffer_oldest_pending_age_seconds"));
        assert!(before.contains("teodb_flush_visibility_lag_seconds"));
        assert!(before.contains("namespace=\"metrics\""));
        assert!(before.contains("table=\"events\""));

        buffer.release_reservation(held_reservation);
        buffers.remove(&table);
        collect_gauges_once(&metrics, &buffers, None, &mut previous);
        let after = metrics.encode();
        assert!(
            !after.contains("namespace=\"metrics\""),
            "dropped table metric labels must be removed"
        );
        assert_eq!(metrics.buffer.reserved_bytes.get(), 0);
    }
}
