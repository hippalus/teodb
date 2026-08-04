//! FlightSQL query handlers: get_flight_info, get_schema, do_get.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::{
    Any, CommandGetCatalogs, CommandGetDbSchemas, CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes,
    CommandGetTables, CommandStatementQuery, ProstMessageExt,
};
use arrow_flight::{FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, PollInfo, SchemaResult, Ticket};
use futures::{Stream, StreamExt};
use prost::Message;
use tonic::{Response, Status};
use tracing::debug;

use teodb_core::query_id::QueryId;

use crate::http::AppState;

use super::super::prepared::PreparedStatementStore;

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

enum FlightInfoRequest {
    Statement(String),
    MetadataQuery { sql: String, schema: Arc<Schema> },
    Static { ticket: &'static str, schema: Arc<Schema> },
    SqlInfo(CommandGetSqlInfo),
}

async fn plan_sql_schema(
    state: &Arc<AppState>,
    principal: &teodb_core::traits::authz::Principal,
    sql: &str,
    timeout_operation: &str,
) -> Result<Arc<Schema>, Status> {
    super::super::auth::authorize(
        state,
        principal,
        teodb_core::traits::authz::Action::Query,
        teodb_core::traits::authz::Resource::Cluster,
    )
    .await?;

    let request = teodb_query::QueryRequest {
        sql: sql.to_string(),
        principal: principal.clone(),
        query_id: QueryId::new(),
        limit: None,
    };
    match tokio::time::timeout(
        state.lifecycle.query_timeout,
        state.services.query_engine.prepare(request),
    )
    .await
    {
        Ok(Ok(handle)) => Ok(handle.schema),
        Ok(Err(error)) => Err(crate::flight::error::status(error)),
        Err(_) => Err(Status::deadline_exceeded(format!(
            "{timeout_operation} timed out after {}s",
            state.lifecycle.query_timeout.as_secs()
        ))),
    }
}

/// Build a `FlightInfo` response from a SQL string by planning the query
/// through the QueryEngine to determine the output schema.
async fn flight_info_for_sql(
    state: &Arc<AppState>,
    principal: &teodb_core::traits::authz::Principal,
    sql: &str,
    descriptor: FlightDescriptor,
) -> Result<Response<FlightInfo>, Status> {
    let arrow_schema = plan_sql_schema(state, principal, sql, "query planning").await?;

    // Carry the SQL in a CommandStatementQuery, not a TicketStatementQuery:
    // the latter is reserved for prepared-statement handles, which do_get
    // resolves against the prepared-statement cache. Conflating the two made
    // every direct query look like an unknown prepared handle.
    let ticket_cmd = CommandStatementQuery {
        query: sql.to_string(),
        transaction_id: None,
    };
    let ticket_bytes = ticket_cmd.as_any().encode_to_vec();
    let ticket = Ticket::new(ticket_bytes);

    let info = FlightInfo::new()
        .try_with_schema(&arrow_schema)
        .map_err(|e| Status::internal(format!("schema encoding: {e}")))?
        .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
        .with_descriptor(descriptor);

    Ok(Response::new(info))
}

use super::metadata::*;

fn decode_flight_info_request(any_cmd: &Any) -> Result<FlightInfoRequest, Status> {
    if let Ok(Some(command)) = any_cmd.unpack::<CommandStatementQuery>() {
        if command.query.trim().is_empty() {
            return Err(Status::invalid_argument("query must not be empty"));
        }
        return Ok(FlightInfoRequest::Statement(command.query));
    }
    if any_cmd
        .unpack::<CommandGetCatalogs>()
        .ok()
        .flatten()
        .is_some()
    {
        return Ok(FlightInfoRequest::MetadataQuery {
            sql: "SELECT DISTINCT table_catalog AS catalog_name FROM information_schema.tables ORDER BY catalog_name"
                .into(),
            schema: catalogs_schema(),
        });
    }
    if let Ok(Some(command)) = any_cmd.unpack::<CommandGetDbSchemas>() {
        return Ok(FlightInfoRequest::MetadataQuery {
            sql: build_db_schemas_sql(&command),
            schema: db_schemas_schema(),
        });
    }
    if let Ok(Some(command)) = any_cmd.unpack::<CommandGetTables>() {
        return Ok(FlightInfoRequest::MetadataQuery {
            sql: build_get_tables_sql(&command),
            schema: tables_schema(),
        });
    }
    if any_cmd
        .unpack::<CommandGetTableTypes>()
        .ok()
        .flatten()
        .is_some()
    {
        return Ok(FlightInfoRequest::Static {
            ticket: "__table_types__",
            schema: table_types_schema(),
        });
    }
    if let Ok(Some(command)) = any_cmd.unpack::<CommandGetSqlInfo>() {
        return Ok(FlightInfoRequest::SqlInfo(command));
    }
    if any_cmd
        .unpack::<CommandGetPrimaryKeys>()
        .ok()
        .flatten()
        .is_some()
    {
        return Ok(FlightInfoRequest::Static {
            ticket: "__primary_keys__",
            schema: primary_keys_schema(),
        });
    }
    Err(Status::unimplemented(format!(
        "FlightSQL command not supported: {}",
        any_cmd.type_url
    )))
}

/// Build a `FlightInfo` for a `CommandStatementQuery` or metadata commands.
pub async fn get_flight_info(
    state: &Arc<AppState>,
    principal: &teodb_core::traits::authz::Principal,
    descriptor: FlightDescriptor,
) -> Result<Response<FlightInfo>, Status> {
    let any_cmd = Any::decode(&*descriptor.cmd)
        .map_err(|e| Status::invalid_argument(format!("cannot decode FlightSQL command: {e}")))?;
    match decode_flight_info_request(&any_cmd)? {
        FlightInfoRequest::Statement(sql) => {
            debug!(%sql, "Flight SQL statement info");
            flight_info_for_sql(state, principal, &sql, descriptor).await
        }
        FlightInfoRequest::MetadataQuery { sql, .. } => flight_info_for_sql(state, principal, &sql, descriptor).await,
        FlightInfoRequest::Static { ticket, schema } => flight_info_for_schema(ticket, &schema, descriptor),
        FlightInfoRequest::SqlInfo(command) => flight_info_for_sql_info(&command, descriptor),
    }
}

/// Poll for completion — TeoDB queries are synchronous so this returns immediately.
pub async fn poll_flight_info(
    state: &Arc<AppState>,
    principal: &teodb_core::traits::authz::Principal,
    descriptor: FlightDescriptor,
) -> Result<Response<PollInfo>, Status> {
    let info = get_flight_info(state, principal, descriptor).await?;
    Ok(Response::new(PollInfo {
        info: Some(info.into_inner()),
        flight_descriptor: None,
        progress: Some(1.0),
        expiration_time: None,
    }))
}

/// Return the schema for a `CommandStatementQuery` or metadata commands.
pub async fn get_schema(
    state: &Arc<AppState>,
    principal: &teodb_core::traits::authz::Principal,
    descriptor: FlightDescriptor,
) -> Result<Response<SchemaResult>, Status> {
    let any_cmd =
        Any::decode(&*descriptor.cmd).map_err(|e| Status::invalid_argument(format!("cannot decode command: {e}")))?;

    let arrow_schema = match decode_flight_info_request(&any_cmd)? {
        FlightInfoRequest::Statement(sql) => plan_sql_schema(state, principal, &sql, "schema resolution").await?,
        FlightInfoRequest::MetadataQuery { schema, .. } | FlightInfoRequest::Static { schema, .. } => schema,
        FlightInfoRequest::SqlInfo(_) => build_sql_info_data()?.schema(),
    };

    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let schema_result = SchemaResult::try_from(arrow_flight::SchemaAsIpc::new(&arrow_schema, &options))
        .map_err(|e| Status::internal(format!("schema encoding: {e}")))?;
    Ok(Response::new(schema_result))
}

/// Stream record batches as `FlightData`.
fn batches_to_stream(
    state: &AppState,
    operation: &'static str,
    batches: Vec<RecordBatch>,
) -> Result<Response<BoxStream<FlightData>>, Status> {
    let schema = if let Some(batch) = batches.first() {
        batch.schema()
    } else {
        return Ok(Response::new(
            Box::pin(futures::stream::empty()) as BoxStream<FlightData>
        ));
    };

    let flight_data = arrow_flight::utils::batches_to_flight_data(&schema, batches)
        .map_err(|e| Status::internal(format!("flight encoding: {e}")))?;

    let authorization = state.security.authorization.clone();
    let stream = futures::stream::iter(flight_data.into_iter().map(move |data| {
        authorization.result_bytes(
            crate::observer::ApiTransport::Flight,
            operation,
            flight_data_bytes(&data),
        );
        Ok(data)
    }));
    Ok(Response::new(Box::pin(stream) as BoxStream<FlightData>))
}

/// Execute a SQL query from a ticket and stream results as `FlightData`.
pub async fn do_get(
    state: &Arc<AppState>,
    prepared: &PreparedStatementStore,
    principal: &teodb_core::traits::authz::Principal,
    ticket: Ticket,
) -> Result<Response<BoxStream<FlightData>>, Status> {
    // Try decoding as CommandGetSqlInfo first (not SQL-based).
    if let Ok(any_msg) = Any::decode(&*ticket.ticket)
        && let Ok(Some(cmd)) = any_msg.unpack::<CommandGetSqlInfo>()
    {
        let sql_info_data = build_sql_info_data()?;
        let batch = sql_info_data
            .record_batch(cmd.info.iter().copied())
            .map_err(|e| Status::internal(format!("sql info encoding: {e}")))?;
        return batches_to_stream(state, "sql_info", vec![batch]);
    }

    let sql = prepared.extract_sql_from_ticket(&ticket)?;

    // Handle synthetic tickets for metadata that doesn't need SQL execution.
    if sql == "__table_types__" {
        let schema = table_types_schema();
        let types = StringArray::from(vec!["TABLE", "VIEW"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(types)])
            .map_err(|e| Status::internal(format!("batch creation: {e}")))?;
        return batches_to_stream(state, "table_types", vec![batch]);
    }

    if sql == "__primary_keys__" {
        let schema = primary_keys_schema();
        let batch = RecordBatch::new_empty(schema);
        return batches_to_stream(state, "primary_keys", vec![batch]);
    }

    if sql.trim().is_empty() {
        return Err(Status::invalid_argument("ticket must contain a non-empty SQL query"));
    }

    debug!(sql = %sql, "Flight SQL do_get: executing query");

    super::super::auth::authorize(
        state,
        principal,
        teodb_core::traits::authz::Action::Query,
        teodb_core::traits::authz::Resource::Cluster,
    )
    .await?;

    let engine = &state.services.query_engine;

    let query_id = QueryId::new();
    let req = teodb_query::QueryRequest {
        sql: sql.clone(),
        principal: principal.clone(),
        query_id,
        limit: None,
    };

    // Single deadline budget for the entire prepare → execute → collect cycle.
    let deadline = state.lifecycle.query_timeout;
    let start = Instant::now();

    let handle = match tokio::time::timeout(deadline, engine.prepare(req)).await {
        Ok(Ok(h)) => h,
        Ok(Err(error)) => {
            return Err(crate::flight::error::status(error));
        }
        Err(_) => {
            let _ = engine.cancel(&query_id).await;
            return Err(Status::deadline_exceeded(format!(
                "query planning timed out after {}s",
                deadline.as_secs()
            )));
        }
    };
    // The planned schema is emitted first so empty results still carry it.
    let result_schema = handle.schema.clone();

    let remaining = deadline.saturating_sub(start.elapsed());
    let stream = match tokio::time::timeout(remaining, engine.execute_stream(handle)).await {
        Ok(Ok(s)) => s,
        Ok(Err(error)) => {
            return Err(crate::flight::error::status(error));
        }
        Err(_) => {
            let _ = engine.cancel(&query_id).await;
            return Err(Status::deadline_exceeded(format!(
                "query execution timed out after {}s",
                deadline.as_secs()
            )));
        }
    };

    // Encode FlightData incrementally — schema first, then each batch as it
    // arrives — so the client gets the first batch before the query finishes
    // and large results never materialize in memory. Each poll stays under the
    // remaining end-to-end deadline; on expiry the engine job is cancelled.
    let deadline_at = tokio::time::Instant::now() + deadline.saturating_sub(start.elapsed());
    let deadline_secs = deadline.as_secs();
    let engine = engine.clone();
    let batch_stream = futures::stream::unfold(
        (stream, engine, query_id, deadline_at, deadline_secs, false),
        |(mut stream, engine, query_id, deadline_at, deadline_secs, done)| async move {
            if done {
                return None;
            }
            match tokio::time::timeout_at(deadline_at, stream.next()).await {
                Ok(Some(Ok(batch))) => Some((Ok(batch), (stream, engine, query_id, deadline_at, deadline_secs, false))),
                Ok(Some(Err(e))) => Some((
                    Err(flight_external_error(format!("query streaming: {e}"))),
                    (stream, engine, query_id, deadline_at, deadline_secs, true),
                )),
                Ok(None) => None,
                Err(_) => {
                    let _ = engine.cancel(&query_id).await;
                    let err = flight_external_error(format!("query streaming timed out after {deadline_secs}s"));
                    Some((Err(err), (stream, engine, query_id, deadline_at, deadline_secs, true)))
                }
            }
        },
    );

    let authorization = state.security.authorization.clone();
    let flight_stream = arrow_flight::encode::FlightDataEncoderBuilder::new()
        .with_schema(result_schema)
        .build(batch_stream)
        .map(move |result| {
            result
                .inspect(|data| {
                    authorization.result_bytes(crate::observer::ApiTransport::Flight, "query", flight_data_bytes(data));
                })
                .map_err(|e| Status::internal(format!("flight encoding: {e}")))
        });

    Ok(Response::new(Box::pin(flight_stream) as BoxStream<FlightData>))
}

fn flight_data_bytes(data: &FlightData) -> u64 {
    u64::try_from(
        data.data_header
            .len()
            .saturating_add(data.data_body.len())
            .saturating_add(data.app_metadata.len()),
    )
    .unwrap_or(u64::MAX)
}

/// Wrap a message as a `FlightError` for surfacing through the encoder stream.
fn flight_external_error(message: String) -> arrow_flight::error::FlightError {
    arrow_flight::error::FlightError::ExternalError(Box::new(std::io::Error::other(message)))
}

fn catalogs_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("catalog_name", DataType::Utf8, false)]))
}

fn db_schemas_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("db_schema_name", DataType::Utf8, false),
    ]))
}

fn tables_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("db_schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
    ]))
}

fn table_types_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("table_type", DataType::Utf8, false)]))
}

fn primary_keys_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new("db_schema_name", DataType::Utf8, true),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("key_name", DataType::Utf8, true),
        Field::new("key_sequence", DataType::Int32, false),
    ]))
}
