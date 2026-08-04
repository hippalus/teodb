//! POST /api/v1/query — Execute a SQL query and return JSON results.

use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use tracing::{debug, warn};

use teodb_core::error::TeoDBError;
use teodb_core::problem::Link;
use teodb_core::query_id::QueryId;
use teodb_core::traits::authz::{Action, Resource};
use teodb_core::validation::clamp_query_limit;

use crate::http::AppState;
use crate::http::common::error::ApiError;
use crate::http::common::hateoas::HateoasResponse;
use crate::http::common::security::SecurityContext;
use crate::observer::ApiTransport;
use crate::service::SqlRouting;

use super::json_rows::{JsonRowsWriter, rows_from_maps};
use super::types::{ColumnInfo, QueryRequest, QueryResponse};

pub async fn query_sql(
    State(state): State<Arc<AppState>>,
    ctx: SecurityContext,
    Json(body): Json<QueryRequest>,
) -> Result<Response, ApiError> {
    let instance = "/api/v1/query";
    let start = Instant::now();

    ctx.authorize(Action::Query, Resource::Cluster)
        .await?;

    if body.sql.trim().is_empty() {
        return Err(TeoDBError::InvalidArgument {
            field: "sql".into(),
            message: "sql must not be empty".into(),
        }
        .into());
    }

    let limit = clamp_query_limit(body.limit);
    let principal = ctx.principal().clone();

    debug!(sql = %body.sql, limit, "executing SQL query via REST");

    // DDL is routed through the catalog and applies its buffer/WAL side effects
    // in the query service; anything else falls through to the engine.
    match state.services.ddl.route_sql(&body.sql).await? {
        SqlRouting::Ddl(result) => return ddl_response(result, start.elapsed().as_millis() as u64, instance),
        SqlRouting::Engine => {}
    }

    let engine = &state.services.query_engine;
    let query_id = QueryId::new();
    let req = teodb_query::QueryRequest {
        sql: body.sql.clone(),
        principal,
        query_id,
        limit: Some(limit),
    };

    // One end-to-end deadline across planning, execution, and every stream
    // poll — not a fresh timeout per phase. On expiry we drop the stream
    // (releasing snapshot pins) and best-effort cancel the engine job.
    let deadline = tokio::time::Instant::now() + state.lifecycle.query_timeout;
    let timed_out = || {
        ApiError::from(TeoDBError::QueryExecution(format!(
            "query timed out after {} seconds",
            state.lifecycle.query_timeout.as_secs()
        )))
    };

    let handle = match tokio::time::timeout_at(deadline, engine.prepare(req)).await {
        Ok(inner) => inner?,
        Err(_elapsed) => {
            cancel_query(engine, &query_id).await;
            return Err(timed_out());
        }
    };

    // Extract column metadata from the planned schema.
    let columns: Vec<ColumnInfo> = handle
        .schema
        .fields()
        .iter()
        .map(|f| ColumnInfo {
            name: f.name().clone(),
            data_type: format!("{}", f.data_type()),
        })
        .collect();

    let mut stream = match tokio::time::timeout_at(deadline, engine.execute_stream(handle)).await {
        Ok(inner) => inner?,
        Err(_elapsed) => {
            cancel_query(engine, &query_id).await;
            return Err(timed_out());
        }
    };

    // Serialize streaming batches straight into the response `rows` JSON,
    // enforcing the same deadline on each poll so a slow stream cannot outlive
    // the configured timeout.
    let mut rows_writer = JsonRowsWriter::new(limit, state.services.config.max_result_bytes);
    loop {
        let next = match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(next) => next,
            Err(_elapsed) => {
                // Drop the stream first so pins/resources release, then cancel.
                drop(stream);
                cancel_query(engine, &query_id).await;
                return Err(timed_out());
            }
        };
        let Some(result) = next else { break };
        let batch = match result {
            Ok(b) => b,
            Err(e) => {
                return Err(TeoDBError::QueryExecution(format!("streaming error: {e}")).into());
            }
        };
        // `write` returns false once the row limit is reached.
        match rows_writer.write(&batch) {
            Ok(true) => {}
            Ok(false) => break,
            Err(error @ TeoDBError::ResultTooLarge { .. }) => {
                drop(stream);
                cancel_query(engine, &query_id).await;
                state
                    .security
                    .authorization
                    .admission_rejection(ApiTransport::Rest, "result_size");
                return Err(error.into());
            }
            Err(error) => return Err(error.into()),
        }
    }
    let (rows, row_count) = rows_writer.finish()?;
    state
        .security
        .authorization
        .result_bytes(ApiTransport::Rest, "query", rows.get().len() as u64);

    let elapsed_ms = start.elapsed().as_millis() as u64;

    if elapsed_ms > state.lifecycle.slow_query_threshold.as_millis() as u64 {
        warn!(
            elapsed_ms,
            sql = %body.sql,
            "slow query detected"
        );
    }

    let resp = HateoasResponse::new(QueryResponse {
        columns,
        rows,
        row_count,
        elapsed_ms,
    })
    .with_link(
        "self",
        Link::new(instance)
            .with_method("POST")
            .with_title("Execute SQL query"),
    )
    .with_link(
        "explain",
        Link::new("/api/v1/query/explain")
            .with_method("POST")
            .with_title("Explain query plan"),
    );

    Ok((StatusCode::OK, Json(resp)).into_response())
}

/// Best-effort cancellation of a timed-out query. The deadline already fired,
/// so failures here are logged, not surfaced to the caller.
async fn cancel_query(engine: &Arc<dyn teodb_query::QueryEngine>, query_id: &QueryId) {
    if let Err(e) = engine.cancel(query_id).await {
        warn!(query_id = %query_id, error = %e, "failed to cancel timed-out query");
    }
}

/// Shape an executed DDL statement's result into the standard query response.
fn ddl_response(result: teodb_query::ddl::DdlResult, elapsed_ms: u64, instance: &str) -> Result<Response, ApiError> {
    let columns = if result.rows.is_empty() {
        vec![ColumnInfo {
            name: "status".into(),
            data_type: "Utf8".into(),
        }]
    } else {
        result
            .rows
            .first()
            .map(|r| {
                r.keys()
                    .map(|k| ColumnInfo {
                        name: k.clone(),
                        data_type: "Utf8".into(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    let row_maps: Vec<serde_json::Map<String, serde_json::Value>> = if result.rows.is_empty() {
        vec![{
            let mut m = serde_json::Map::new();
            m.insert("status".into(), serde_json::Value::String(result.status));
            m
        }]
    } else {
        result
            .rows
            .into_iter()
            .map(|r| r.into_iter().collect())
            .collect()
    };
    let row_count = row_maps.len();
    let rows = rows_from_maps(&row_maps)?;

    let resp = HateoasResponse::new(QueryResponse {
        columns,
        rows,
        row_count,
        elapsed_ms,
    })
    .with_link(
        "self",
        Link::new(instance)
            .with_method("POST")
            .with_title("Execute SQL query"),
    );
    Ok((StatusCode::OK, Json(resp)).into_response())
}
