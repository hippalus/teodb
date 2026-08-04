//! DDL result types returned to callers.

/// A row of key-value pairs returned by DDL/metadata operations.
pub type DdlRow = std::collections::HashMap<String, serde_json::Value>;

/// Result of a DDL operation or metadata query (SHOW, DESCRIBE).
#[derive(Debug)]
pub struct DdlResult {
    /// Human-readable status message.
    pub status: String,
    /// Optional result rows (for SHOW/DESCRIBE statements).
    pub rows: Vec<DdlRow>,
    /// True when the statement actually mutated catalog state. False for
    /// `IF [NOT] EXISTS` no-ops and metadata queries — callers use this to
    /// skip side effects like buffer eviction (an `IF NOT EXISTS` no-op must
    /// not discard the existing table's unflushed rows).
    pub changed: bool,
}

impl DdlResult {
    /// A statement that mutated catalog state (table/schema created or dropped).
    pub fn changed(status: impl Into<String>) -> Self {
        Self {
            status: status.into(),
            rows: Vec::new(),
            changed: true,
        }
    }

    /// A statement that completed without mutating catalog state
    /// (`IF [NOT] EXISTS` no-op).
    pub fn unchanged(status: impl Into<String>) -> Self {
        Self {
            status: status.into(),
            rows: Vec::new(),
            changed: false,
        }
    }

    pub fn with_rows(status: impl Into<String>, rows: Vec<DdlRow>) -> Self {
        Self {
            status: status.into(),
            rows,
            changed: false,
        }
    }
}
