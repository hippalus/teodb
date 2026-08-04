//! JSON row ingestion and buffer flush endpoint handlers.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use teodb_core::ident::TableIdent;
use teodb_core::problem::Link;
use teodb_core::traits::authz::{Action, Resource};

use crate::http::common::error::ApiError;
use crate::http::common::hateoas::HateoasResponse;
use crate::http::common::security::SecurityContext;
use crate::http::state::AppState;
use teodb_ingest::service::IngestOutcome;

use super::model::*;

/// POST /api/v1/tables/{ns}/{tbl}/ingest — Ingest JSON into a table's hot buffer.
///
/// Accepts any JSON shape:
///   - `[{...}, {...}]` — array of row objects
///   - `{...}`          — single row object
///   - `{"rows": [...]}` — explicit wrapper (with optional `idempotency_key`)
///
/// Nested objects are flattened with dot-notation (`a.b.c`).
/// If the table doesn't exist, it is auto-created from the inferred schema.
pub async fn ingest_json(
    State(state): State<Arc<AppState>>,
    ctx: SecurityContext,
    Path((ns, tbl)): Path<(String, String)>,
    Json(body): Json<IngestRequest>,
) -> Result<Response, ApiError> {
    let ident = TableIdent::new(&ns, &tbl);

    ctx.authorize(Action::Ingest, Resource::Table(ident.clone()))
        .await?;

    let outcome = state
        .services
        .ingest
        .ingest_rows(&ident, &body.rows, body.idempotency_key.as_deref())
        .await
        .inspect_err(|error| {
            if let Some(reason) = write_rejection_reason(error) {
                state
                    .security
                    .authorization
                    .write_rejection(reason);
            }
        })?;

    let instance = format!("/api/v1/tables/{ns}/{tbl}/ingest");
    let (status, deduplicated, receipt) = match outcome {
        // 200 OK: the rows already landed under this idempotency key.
        IngestOutcome::Deduplicated(receipt) => (StatusCode::OK, true, receipt),
        // 202 Accepted: the rows were durably appended on this request.
        IngestOutcome::Accepted(receipt) => (StatusCode::ACCEPTED, false, receipt),
    };

    let response = IngestResponse {
        accepted_rows: receipt.accepted_rows,
        batch_id: receipt.batch_id.to_string(),
        writer_id: receipt.writer_id.to_string(),
        generation: receipt.generation,
        deduplicated,
    };
    Ok((status, Json(ingest_response_links(response, &instance, &ns, &tbl))).into_response())
}

/// Attach the standard HATEOAS links to an ingest response.
fn ingest_response_links(
    response: IngestResponse,
    instance: &str,
    ns: &str,
    tbl: &str,
) -> HateoasResponse<IngestResponse> {
    let table_uri = format!("/api/v1/tables/{ns}/{tbl}");
    HateoasResponse::new(response)
        .with_link(
            "self",
            Link::new(instance)
                .with_method("POST")
                .with_title("Ingest rows"),
        )
        .with_link(
            "flush",
            Link::new(format!("{table_uri}/flush"))
                .with_method("POST")
                .with_title("Flush table buffer"),
        )
        .with_link(
            "table",
            Link::new(&table_uri)
                .with_method("GET")
                .with_title("Table metadata"),
        )
}

/// POST /api/v1/tables/{ns}/{tbl}/flush — Force-flush the table's hot buffer to Parquet.
pub async fn flush_table(
    State(state): State<Arc<AppState>>,
    ctx: SecurityContext,
    Path((ns, tbl)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let ident = TableIdent::new(&ns, &tbl);

    ctx.authorize(Action::Ingest, Resource::Table(ident.clone()))
        .await?;

    let outcome = state.services.flusher.flush_table(&ident).await?;

    let status = match outcome {
        teodb_ingest::flush::FlushOutcome::Committed { .. } => "flushed",
        // No unflushed data on this node — peers flush their own buffers on
        // the background interval.
        teodb_ingest::flush::FlushOutcome::Empty => "no_pending_data",
    };

    let table_uri = format!("/api/v1/tables/{ns}/{tbl}");
    let instance = format!("{table_uri}/flush");
    let resp = HateoasResponse::new(FlushResponse { status: status.into() })
        .with_link(
            "self",
            Link::new(&instance)
                .with_method("POST")
                .with_title("Flush table"),
        )
        .with_link(
            "ingest",
            Link::new(format!("{table_uri}/ingest"))
                .with_method("POST")
                .with_title("Ingest rows"),
        )
        .with_link(
            "table",
            Link::new(&table_uri)
                .with_method("GET")
                .with_title("Table metadata"),
        );

    Ok((StatusCode::OK, Json(resp)).into_response())
}

fn write_rejection_reason(error: &teodb_core::error::TeoDBError) -> Option<&'static str> {
    use teodb_core::error::TeoDBError;
    match error {
        TeoDBError::Backpressure(_) => Some("buffer_capacity"),
        TeoDBError::Wal { .. } => Some("wal_capacity"),
        TeoDBError::FlushBlocked { .. } => Some("flush_blocked"),
        TeoDBError::WriterRegistryFull { .. } => Some("writer_registry"),
        _ => None,
    }
}
