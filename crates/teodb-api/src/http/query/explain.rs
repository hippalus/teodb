//! POST /api/v1/query/explain — Return the logical/physical plan for a SQL query.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::Array;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use teodb_core::error::TeoDBError;
use teodb_core::query_id::QueryId;
use teodb_core::traits::authz::{Action, Resource};

use crate::http::AppState;
use crate::http::common::error::ApiError;
use crate::http::common::security::SecurityContext;

use super::types::{ExplainResponse, QueryRequest};

pub async fn explain_sql(
    State(state): State<Arc<AppState>>,
    ctx: SecurityContext,
    Json(body): Json<QueryRequest>,
) -> Result<Response, ApiError> {
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

    let engine = &state.services.query_engine;
    let principal = ctx.principal().clone();
    let query_id = QueryId::new();
    let explain_sql = format!("EXPLAIN {}", body.sql);

    let req = teodb_query::QueryRequest {
        sql: explain_sql,
        principal,
        query_id,
        limit: None,
    };

    let handle = engine.prepare(req).await?;

    // The plan text absorbs execution/timeout failures rather than surfacing
    // them as problem responses — an EXPLAIN that can't run still answers with
    // a diagnostic body.
    let plan = match tokio::time::timeout(state.lifecycle.query_timeout, engine.execute_stream(handle)).await {
        Ok(Ok(stream)) => {
            use futures::StreamExt;
            let mut plan_text = String::new();
            let mut stream = stream;
            while let Some(result) = stream.next().await {
                match result {
                    Ok(batch) => {
                        let Some(plan_col) = batch.column_by_name("plan") else {
                            continue;
                        };
                        for i in 0..plan_col.len() {
                            if plan_col.is_null(i) {
                                continue;
                            }
                            if let Ok(p) = arrow::util::display::array_value_to_string(plan_col, i) {
                                plan_text.push_str(&p);
                                plan_text.push('\n');
                            }
                        }
                    }
                    Err(e) => {
                        plan_text = format!("explain failed: {e}");
                        break;
                    }
                }
            }
            plan_text
        }
        Ok(Err(e)) => format!("explain failed: {e}"),
        Err(_) => "explain timed out".into(),
    };

    let elapsed_ms = start.elapsed().as_millis() as u64;

    Ok((StatusCode::OK, Json(ExplainResponse { plan, elapsed_ms })).into_response())
}
