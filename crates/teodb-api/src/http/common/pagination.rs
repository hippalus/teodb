//! Shared pagination types used across domain handlers.

use serde::Deserialize;

/// Pagination query parameters shared across list endpoints.
#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    pub offset: Option<usize>,
    pub limit: Option<usize>,
}
