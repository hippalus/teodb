//! Admin status and cluster endpoint handlers.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::http::common::error::ApiError;
use crate::http::state::AppState;

use super::model::*;

/// GET /api/v1/admin/status — Aggregate server status for the admin dashboard.
pub async fn status(State(state): State<Arc<AppState>>) -> Response {
    let mut tables_count = 0usize;

    if let Ok(namespaces) = state.services.catalog.list_namespaces().await {
        for ns in &namespaces {
            if let Ok(tables) = state.services.catalog.list_tables(ns).await {
                tables_count += tables.len();
            }
        }
    }

    let buffer_tables = state.services.buffers.tables();
    let mut buffer_bytes = 0u64;
    for ident in &buffer_tables {
        if let Some(buf) = state.services.buffers.get(ident) {
            let stats = buf.buffer_stats();
            buffer_bytes += stats
                .pending_bytes
                .saturating_add(stats.in_flight_bytes)
                .saturating_add(stats.recently_committed_bytes)
                .saturating_add(stats.reserved_bytes);
        }
    }

    let mut components = Vec::with_capacity(4);

    let catalog_healthy = matches!(
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            state.services.catalog.list_namespaces(),
        )
        .await,
        Ok(Ok(_))
    );
    components.push(ComponentHealth {
        name: "catalog".into(),
        status: if catalog_healthy { "healthy" } else { "unhealthy" },
        message: None,
    });

    let (wal_status, wal_message) = match state.services.wal.segment_count() {
        Ok(count) => ("healthy", Some(format!("{count} segments"))),
        Err(e) => ("unhealthy", Some(format!("WAL error: {e}"))),
    };
    components.push(ComponentHealth {
        name: "wal".into(),
        status: wal_status,
        message: wal_message,
    });

    components.push(ComponentHealth {
        name: "query_engine".into(),
        status: "healthy",
        message: None,
    });

    let blocked = state.services.flusher.blocked_tables();
    components.push(ComponentHealth {
        name: "flush".into(),
        status: if blocked.is_empty() { "healthy" } else { "degraded" },
        message: (!blocked.is_empty()).then(|| format!("{} table(s) have unresolved exact commits", blocked.len())),
    });

    let resp = StatusResponse {
        server_version: env!("CARGO_PKG_VERSION").into(),
        uptime_seconds: state.lifecycle.started_at.elapsed().as_secs(),
        tables_count,
        total_rows: 0,
        memory_usage_bytes: buffer_bytes,
        components,
    };

    (StatusCode::OK, Json(resp)).into_response()
}

/// GET /api/v1/admin/tables — List all tables across all namespaces.
pub async fn all_tables(State(state): State<Arc<AppState>>) -> Result<Response, ApiError> {
    let namespaces = state.services.catalog.list_namespaces().await?;

    let mut all_idents = Vec::new();
    for ns in &namespaces {
        if let Ok(tables) = state.services.catalog.list_tables(ns).await {
            all_idents.extend(tables);
        }
    }

    // Load metadata for all tables concurrently.
    let futs: Vec<_> = all_idents
        .iter()
        .map(|ident| state.services.catalog.load_table(ident))
        .collect();
    let metadata_results = futures::future::join_all(futs).await;

    let mut summaries = Vec::with_capacity(all_idents.len());
    for (ident, meta_result) in all_idents.iter().zip(metadata_results) {
        let stats = state
            .services
            .buffers
            .get(ident)
            .map(|b| b.buffer_stats())
            .unwrap_or_default();
        let buffer_bytes = stats
            .pending_bytes
            .saturating_add(stats.in_flight_bytes)
            .saturating_add(stats.recently_committed_bytes);
        let buffer_rows = stats
            .pending_entries
            .saturating_add(stats.in_flight_entries);

        let (column_count, iceberg_rows, iceberg_bytes, partitioned, partition_fields) = match meta_result {
            Ok(meta) => {
                let cols = meta.current_schema()?.columns.len();

                // The Iceberg Rust library's fast_append doesn't compute
                // cumulative summary stats — `total-records` only reflects
                // the current commit's files. Walk the snapshot chain and
                // sum `added-records` / `added-files-size` for correct totals.
                let (rows, bytes) = {
                    let mut total_rows = 0u64;
                    let mut total_bytes = 0u64;
                    let mut snap_id = meta.current_snapshot_id;
                    while let Some(id) = snap_id {
                        if let Some(snap) = meta.snapshot_by_id(id) {
                            let props = &snap.summary;
                            total_rows += props
                                .get("added-records")
                                .and_then(|v| v.parse::<u64>().ok())
                                .unwrap_or(0);
                            total_bytes += props
                                .get("added-files-size")
                                .and_then(|v| v.parse::<u64>().ok())
                                .unwrap_or(0);
                            snap_id = snap.parent_snapshot_id;
                        } else {
                            break;
                        }
                    }
                    (total_rows, total_bytes)
                };

                let spec = meta.current_partition_spec()?;
                let is_partitioned = !spec.fields.is_empty();

                let pfields: Vec<String> = spec
                    .fields
                    .iter()
                    .map(|f| {
                        let src_name = meta
                            .current_schema()
                            .ok()
                            .and_then(|schema| schema.by_id(f.source_id))
                            .map(|field| field.name.as_str())
                            .unwrap_or("?");
                        let transform = format!("{}", f.transform);
                        if transform == "identity" {
                            src_name.to_string()
                        } else {
                            format!("{transform}({src_name})")
                        }
                    })
                    .collect();

                (cols, rows, bytes, is_partitioned, pfields)
            }
            Err(_) => (0, 0, 0, false, Vec::new()),
        };

        summaries.push(TableSummary {
            name: ident.name.clone(),
            namespace: ident.namespace.clone(),
            column_count,
            row_count: iceberg_rows + buffer_rows as u64,
            size_bytes: iceberg_bytes + buffer_bytes,
            partitioned,
            partition_fields,
        });
    }

    Ok((StatusCode::OK, Json(summaries)).into_response())
}

/// GET /api/v1/admin/cluster — Cluster status information.
///
/// In distributed deployments the workers/scheduler/active-jobs come from the
/// Ballista scheduler via the injected [`ClusterTopology`]; standalone
/// deployments have no topology source and report empty workers.
///
/// [`ClusterTopology`]: teodb_core::traits::cluster::ClusterTopology
pub async fn cluster(State(state): State<Arc<AppState>>) -> Response {
    let (workers, scheduler, active_jobs) = match &state.readiness.cluster_topology {
        Some(topology) => {
            let snapshot = topology.snapshot().await;
            let workers = snapshot
                .workers
                .into_iter()
                .map(|w| ClusterWorker {
                    id: w.id,
                    host: w.host,
                    flight_port: w.port,
                    status: if w.alive { "active" } else { "offline" },
                    last_heartbeat: w.last_heartbeat_ms.and_then(ms_to_rfc3339),
                })
                .collect();
            let scheduler = SchedulerInfo {
                address: snapshot.scheduler_address,
                reachable: snapshot.scheduler_reachable,
            };
            (workers, Some(scheduler), Some(snapshot.active_jobs))
        }
        None => (Vec::new(), None, None),
    };

    let identity = state.services.wal.writer_identity();
    let pending_tables = state
        .services
        .buffers
        .tables()
        .into_iter()
        .filter_map(|ident| state.services.buffers.get(&ident))
        .filter(|buffer| buffer.has_pending())
        .count();
    let blocked_tables = state.services.flusher.blocked_tables().len();
    let (wal_segments, segment_error) = match state.services.wal.segment_count() {
        Ok(count) => (Some(count), None),
        Err(error) => (None, Some(error.to_string())),
    };
    let (wal_bytes, usage_error) = match state.services.wal.disk_usage_bytes().await {
        Ok(bytes) => (Some(bytes), None),
        Err(error) => (None, Some(error.to_string())),
    };
    let wal_error = segment_error.or(usage_error);

    let resp = ClusterStatusResponse {
        mode: state.lifecycle.role.clone(),
        cluster_id: identity.cluster_id.to_string(),
        node_id: identity.node_id.to_string(),
        writer_id: identity.writer_id.to_string(),
        writer_epoch: identity.writer_epoch.get(),
        // The HTTP server is bound only after startup WAL recovery succeeds.
        recovery_status: "complete",
        uptime_seconds: state.lifecycle.started_at.elapsed().as_secs(),
        pending_tables,
        blocked_tables,
        wal_segments,
        wal_bytes,
        wal_error,
        workers,
        connections: Vec::new(),
        scheduler,
        active_jobs,
    };

    (StatusCode::OK, Json(resp)).into_response()
}

/// GET /api/v1/admin/flush-blocked — Exact table-local ambiguous commits.
pub async fn flush_blocked(State(state): State<Arc<AppState>>) -> Response {
    let blocked = state
        .services
        .flusher
        .blocked_tables()
        .into_iter()
        .map(|blocked| BlockedFlushResponse {
            namespace: blocked.prepared.table.namespace,
            table: blocked.prepared.table.name,
            table_uuid: blocked.prepared.table_uuid.to_string(),
            writer_id: blocked.prepared.writer_id.to_string(),
            writer_epoch: blocked.prepared.writer_epoch.get(),
            commit_id: blocked.prepared.commit_id.to_string(),
            generation_lo: blocked.prepared.generations.lo,
            generation_hi: blocked.prepared.generations.hi,
            blocked_since_ms: blocked.since_ms,
            last_recheck_ms: blocked.last_recheck_ms,
            status_check_attempts: blocked.status_check_attempts,
            last_error_class: blocked.last_error_class,
        })
        .collect::<Vec<_>>();
    (StatusCode::OK, Json(blocked)).into_response()
}

/// POST /api/v1/admin/flush-blocked/{namespace}/{table}/recheck — Trigger
/// one exact status check. There is intentionally no force/discard operation.
pub async fn recheck_flush_blocked(
    State(state): State<Arc<AppState>>,
    axum::extract::Path((namespace, table)): axum::extract::Path<(String, String)>,
) -> Result<Response, ApiError> {
    let ident = teodb_core::ident::TableIdent::new(namespace, table);
    let outcome = state
        .services
        .flusher
        .recheck_blocked(&ident)
        .await?;
    let status = match outcome {
        teodb_ingest::flush::FlushOutcome::Committed { .. } => "committed",
        teodb_ingest::flush::FlushOutcome::Empty => "not_blocked",
    };
    Ok((StatusCode::OK, Json(BlockedFlushRecheckResponse { status })).into_response())
}

/// Convert epoch milliseconds to an RFC 3339 timestamp for the admin UI.
fn ms_to_rfc3339(ms: u64) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(ms as i64).map(|dt| dt.to_rfc3339())
}
