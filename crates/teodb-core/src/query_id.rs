//! Unique identifier for a distributed query.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Opaque, globally unique identifier for a query across all cluster nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueryId(Uuid);

impl QueryId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for QueryId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for QueryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "q-{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_id_display() {
        let id = QueryId::new();
        let s = id.to_string();
        assert!(s.starts_with("q-"));
    }

    #[test]
    fn query_id_equality() {
        let a = QueryId::new();
        let b = a;
        assert_eq!(a, b);
        assert_ne!(a, QueryId::new());
    }

    #[test]
    fn query_id_serde_roundtrip() {
        let id = QueryId::new();
        let json = serde_json::to_string(&id).unwrap();
        let id2: QueryId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, id2);
    }
}
