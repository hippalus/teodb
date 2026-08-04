//! Request/response DTOs for the table domain.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Table list response.
#[derive(Debug, Serialize)]
pub struct TableListResponse {
    pub tables: Vec<TableIdentResponse>,
    pub total: usize,
    pub offset: usize,
    pub limit: usize,
}

/// Table identifier in responses.
#[derive(Debug, Serialize)]
pub struct TableIdentResponse {
    pub namespace: String,
    pub name: String,
}

/// Table metadata response.
#[derive(Debug, Serialize)]
pub struct TableMetadataResponse {
    pub namespace: String,
    pub name: String,
    pub current_schema_id: i32,
    pub current_snapshot_id: Option<i64>,
    pub columns: Vec<ColumnSchemaInfo>,
    pub properties: HashMap<String, String>,
}

/// Column information from the current schema.
#[derive(Debug, Serialize)]
pub struct ColumnSchemaInfo {
    pub field_id: i32,
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Create table request.
#[derive(Debug, Deserialize)]
pub struct CreateTableRestRequest {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    #[serde(default)]
    pub partition_by: Vec<PartitionFieldRequest>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct DropTableParams {
    #[serde(default)]
    pub purge: bool,
}

/// Partition field definition in create-table requests.
#[derive(Debug, Deserialize)]
pub struct PartitionFieldRequest {
    /// Source column name.
    pub column: String,
    /// Partition transform: "identity", "year", "month", "day", "hour",
    /// "bucket(N)", "truncate(W)".
    pub transform: String,
}

/// Column definition in create-table requests.
#[derive(Debug, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: String,
    #[serde(default)]
    pub nullable: bool,
}
