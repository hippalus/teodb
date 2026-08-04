//! Request/response DTOs for the namespace domain.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Namespace list response.
#[derive(Debug, Serialize)]
pub struct NamespaceListResponse {
    pub namespaces: Vec<String>,
    pub total: usize,
    pub offset: usize,
    pub limit: usize,
}

/// Single namespace response.
#[derive(Debug, Serialize)]
pub struct NamespaceResponse {
    pub namespace: String,
}

/// Create namespace request.
#[derive(Debug, Deserialize)]
pub struct CreateNamespaceRequest {
    pub namespace: String,
    #[serde(default)]
    pub properties: Option<HashMap<String, String>>,
}
