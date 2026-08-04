use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Stable identifier for a column or partition field across the lifetime
/// of a table. Iceberg uses these to support schema evolution; TeoDB uses
/// them for internal correctness everywhere. Column names are display-only.
pub type FieldId = i32;

/// Stable id of a table across renames. Set on creation, never reissued.
pub type TableUuid = Uuid;

/// Monotonic ingest generation. See §12 in the plan.
pub type Generation = u64;

/// Iceberg snapshot id. `i64` to match the Iceberg specification.
pub type SnapshotId = i64;

/// Iceberg sequence number. `i64` to match the Iceberg specification.
pub type SequenceNumber = i64;

/// Fully-qualified table identifier within a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableIdent {
    pub namespace: String,
    pub name: String,
}

impl TableIdent {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    pub fn fully_qualified(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }
}

impl std::fmt::Display for TableIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_ident_fully_qualified() {
        let t = TableIdent::new("analytics", "events");
        assert_eq!(t.fully_qualified(), "analytics.events");
        assert_eq!(t.to_string(), "analytics.events");
    }

    #[test]
    fn table_ident_equality() {
        let a = TableIdent::new("ns", "tbl");
        let b = TableIdent::new("ns", "tbl");
        let c = TableIdent::new("ns", "other");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn table_ident_hash() {
        use std::collections::HashSet;
        let mut s = HashSet::new();
        s.insert(TableIdent::new("ns", "tbl"));
        assert!(s.contains(&TableIdent::new("ns", "tbl")));
    }

    #[test]
    fn table_ident_serde_roundtrip() {
        let t = TableIdent::new("db", "orders");
        let json = serde_json::to_string(&t).unwrap();
        let t2: TableIdent = serde_json::from_str(&json).unwrap();
        assert_eq!(t, t2);
    }
}
