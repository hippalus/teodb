//! Low-cardinality catalog protocol observations.

use std::time::Duration;

/// Outcome labels for Iceberg append publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogCommitOutcome {
    Committed,
    Conflict,
    StateUnknown,
    StaleWriterEpoch,
    WriterRegistryFull,
    TableIncarnationMismatch,
    MetadataCorruption,
    ProtocolError,
    RetryableError,
    FatalError,
}

impl CatalogCommitOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::Conflict => "conflict",
            Self::StateUnknown => "state_unknown",
            Self::StaleWriterEpoch => "stale_writer_epoch",
            Self::WriterRegistryFull => "writer_registry_full",
            Self::TableIncarnationMismatch => "table_incarnation_mismatch",
            Self::MetadataCorruption => "metadata_corruption",
            Self::ProtocolError => "protocol_error",
            Self::RetryableError => "retryable_error",
            Self::FatalError => "fatal_error",
        }
    }
}

/// Outcome labels for exact append status checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogStatusCheckOutcome {
    Committed,
    NotCommitted,
    Unknown,
    Error,
}

impl CatalogStatusCheckOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::NotCommitted => "not_committed",
            Self::Unknown => "unknown",
            Self::Error => "error",
        }
    }
}

/// Observer boundary used by the server metrics layer without coupling the
/// catalog crate to Prometheus.
pub trait CatalogObserver: Send + Sync + 'static {
    fn on_append_commit(&self, outcome: CatalogCommitOutcome, duration: Duration);
    fn on_append_rebase(&self, rebases: u32);
    fn on_status_check(&self, outcome: CatalogStatusCheckOutcome, duration: Duration);
    fn on_writer_checkpoint_parse_failure(&self);
    fn on_writer_checkpoint_count(&self, count: usize);
}
