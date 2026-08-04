//! Prepared statement handlers for FlightSQL.

use std::pin::Pin;

use arrow_flight::Ticket;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult,
    Any, CommandStatementQuery, ProstMessageExt, TicketStatementQuery,
};
use futures::Stream;
use moka::sync::Cache;
use prost::Message;
use tonic::{Response, Status};

use super::codec::encode_schema;

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;
const MAX_PREPARED_STATEMENTS: u64 = 10_000;
const PREPARED_STATEMENT_TTL: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// Manages FlightSQL prepared statements.
pub struct PreparedStatementStore {
    statements: Cache<String, String>,
}

impl PreparedStatementStore {
    pub fn new() -> Self {
        Self {
            statements: Cache::builder()
                .max_capacity(MAX_PREPARED_STATEMENTS)
                .time_to_idle(PREPARED_STATEMENT_TTL)
                .build(),
        }
    }

    /// Extract the SQL query from a canonical FlightSQL ticket.
    pub fn extract_sql_from_ticket(&self, ticket: &Ticket) -> Result<String, Status> {
        let any_msg = Any::decode(&*ticket.ticket)
            .map_err(|error| Status::invalid_argument(format!("ticket must contain a FlightSQL command: {error}")))?;
        // A TicketStatementQuery carries an opaque prepared-statement handle
        // that must be resolved against the store.
        if let Ok(Some(tsq)) = any_msg.unpack::<TicketStatementQuery>() {
            let handle = String::from_utf8(tsq.statement_handle.to_vec())
                .map_err(|_| Status::invalid_argument("statement handle must be valid UTF-8"))?;
            return self
                .statements
                .get(&handle)
                .ok_or_else(|| Status::not_found("prepared statement handle not found or expired"));
        }
        // Direct and metadata queries carry their SQL inline; no cache lookup.
        if let Ok(Some(cmd)) = any_msg.unpack::<CommandStatementQuery>() {
            return Ok(cmd.query);
        }

        Err(Status::invalid_argument(
            "ticket is not a supported FlightSQL statement command",
        ))
    }

    /// Handle a CreatePreparedStatement action.
    pub async fn handle_create(
        &self,
        body: &[u8],
        principal: &teodb_core::traits::authz::Principal,
        state: &crate::http::AppState,
    ) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
        let any_msg =
            Any::decode(body).map_err(|e| Status::invalid_argument(format!("cannot decode action body: {e}")))?;

        let req = any_msg
            .unpack::<ActionCreatePreparedStatementRequest>()
            .map_err(|e| Status::internal(format!("failed to unpack CreatePreparedStatement: {e}")))?
            .ok_or_else(|| Status::invalid_argument("action body is not a CreatePreparedStatementRequest"))?;

        let sql = &req.query;
        if sql.trim().is_empty() {
            return Err(Status::invalid_argument("prepared statement query must not be empty"));
        }

        super::auth::authorize(
            state,
            principal,
            teodb_core::traits::authz::Action::Query,
            teodb_core::traits::authz::Resource::Cluster,
        )
        .await?;

        // Validate the SQL by planning through the QueryEngine.
        let engine = &state.services.query_engine;

        let query_req = teodb_query::QueryRequest {
            sql: sql.clone(),
            principal: principal.clone(),
            query_id: teodb_core::query_id::QueryId::new(),
            limit: None,
        };

        let timeout = state.lifecycle.query_timeout;
        let handle = match tokio::time::timeout(timeout, engine.prepare(query_req)).await {
            Ok(Ok(h)) => h,
            Ok(Err(error)) => {
                return Err(crate::flight::error::status(error));
            }
            Err(_) => {
                return Err(Status::deadline_exceeded(format!(
                    "prepared statement validation timed out after {}s",
                    timeout.as_secs()
                )));
            }
        };

        let arrow_schema = handle.schema.as_ref().clone();

        // Generate a unique handle and store the mapping.
        let handle_id = uuid::Uuid::now_v7().to_string();
        self.statements
            .insert(handle_id.clone(), sql.clone());

        // Encode the schema as IPC bytes for the result.
        let schema_bytes = encode_schema(&arrow_schema)?;

        let result = ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle_id.into_bytes().into(),
            dataset_schema: schema_bytes.into(),
            parameter_schema: prost::bytes::Bytes::new(),
        };

        let any_result = result.as_any();
        let result_bytes = any_result.encode_to_vec();

        let flight_result = arrow_flight::Result {
            body: result_bytes.into(),
        };
        let stream = futures::stream::iter(vec![Ok(flight_result)]);
        Ok(Response::new(Box::pin(stream) as BoxStream<arrow_flight::Result>))
    }

    /// Handle a ClosePreparedStatement action.
    pub fn handle_close(&self, body: &[u8]) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
        let any_msg =
            Any::decode(body).map_err(|e| Status::invalid_argument(format!("cannot decode action body: {e}")))?;

        let req = any_msg
            .unpack::<ActionClosePreparedStatementRequest>()
            .map_err(|e| Status::internal(format!("failed to unpack ClosePreparedStatement: {e}")))?
            .ok_or_else(|| Status::invalid_argument("action body is not a ClosePreparedStatementRequest"))?;

        let handle = String::from_utf8(req.prepared_statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("prepared statement handle must be valid UTF-8"))?;

        self.statements.invalidate(&handle);

        // ClosePreparedStatement returns an empty stream per the FlightSQL spec.
        let stream = futures::stream::empty();
        Ok(Response::new(Box::pin(stream) as BoxStream<arrow_flight::Result>))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepared_ticket(handle: &str) -> Ticket {
        let query = TicketStatementQuery {
            statement_handle: handle.as_bytes().to_vec().into(),
        };
        let any = query.as_any();
        Ticket {
            ticket: any.encode_to_vec().into(),
        }
    }

    #[test]
    fn ticket_statement_query_resolves_prepared_handle() {
        let store = PreparedStatementStore::new();
        store
            .statements
            .insert("handle-1".into(), "SELECT 1".into());

        let sql = store
            .extract_sql_from_ticket(&prepared_ticket("handle-1"))
            .unwrap();

        assert_eq!(sql, "SELECT 1");
    }

    #[test]
    fn unknown_prepared_handle_returns_not_found() {
        let store = PreparedStatementStore::new();

        let err = store
            .extract_sql_from_ticket(&prepared_ticket("missing"))
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    fn statement_query_ticket(sql: &str) -> Ticket {
        let cmd = CommandStatementQuery {
            query: sql.to_string(),
            transaction_id: None,
        };
        Ticket {
            ticket: cmd.as_any().encode_to_vec().into(),
        }
    }

    #[test]
    fn command_statement_query_ticket_resolves_without_cache() {
        // Regression: direct queries carry their SQL in a CommandStatementQuery
        // ticket and must not be misread as prepared-statement handles.
        let store = PreparedStatementStore::new();

        let sql = store
            .extract_sql_from_ticket(&statement_query_ticket("SELECT * FROM tpch.region"))
            .unwrap();

        assert_eq!(sql, "SELECT * FROM tpch.region");
    }

    #[test]
    fn raw_utf8_ticket_is_rejected() {
        let store = PreparedStatementStore::new();
        let ticket = Ticket {
            ticket: "SELECT 1".as_bytes().to_vec().into(),
        };

        let error = store
            .extract_sql_from_ticket(&ticket)
            .unwrap_err();

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
}
