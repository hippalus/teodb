//! Query engine identification and status types.
//!
//! The full `QueryEngine` trait with Arrow-typed streaming lives in
//! `teodb-query::engine`. This module provides only the domain types
//! that cross crate boundaries without Arrow dependencies.

use serde::{Deserialize, Serialize};

use crate::query_id::QueryId;

/// Status of a running or completed query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryStatus {
    Planning,
    Running,
    Completed,
    Cancelled,
    Failed(String),
}

impl QueryStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed(_))
    }
}

/// Lightweight query descriptor for status reporting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryInfo {
    pub query_id: QueryId,
    pub status: QueryStatus,
    pub sql_preview: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_states() {
        assert!(!QueryStatus::Planning.is_terminal());
        assert!(!QueryStatus::Running.is_terminal());
        assert!(QueryStatus::Completed.is_terminal());
        assert!(QueryStatus::Cancelled.is_terminal());
        assert!(QueryStatus::Failed("err".into()).is_terminal());
    }
}
