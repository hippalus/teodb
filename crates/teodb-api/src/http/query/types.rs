//! Request/response DTOs for SQL query endpoints.

use serde::{Deserialize, Serialize};

/// SQL query request body.
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
    #[serde(default = "default_query_limit")]
    pub limit: usize,
}

fn default_query_limit() -> usize {
    10_000
}

/// Column metadata in query responses.
#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

/// SQL query response — rows as objects for easy frontend consumption.
///
/// `rows` is pre-serialized JSON (an array of row objects) spliced into the
/// response body as-is, so result batches are serialized exactly once (F-31).
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub columns: Vec<ColumnInfo>,
    pub rows: Box<serde_json::value::RawValue>,
    pub row_count: usize,
    pub elapsed_ms: u64,
}

/// SQL explain response.
#[derive(Debug, Serialize)]
pub struct ExplainResponse {
    pub plan: String,
    pub elapsed_ms: u64,
}
