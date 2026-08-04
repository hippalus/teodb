//! Transport-independent row ingestion service.
//!
//! Handlers stay thin: they extract input, authorize, call a service, and map
//! the typed result onto a transport response. The orchestration — schema
//! inference, idempotency, WAL durability, table creation, DDL routing — lives
//! on dedicated components and returns plain `Result<_, TeoDBError>`, so it is
//! testable and reusable without an HTTP request in hand.
//!
//! Two cohesive components, each owning `Arc` handles to its collaborators and
//! constructed once at startup (stored on `AppState`):
//!
//! - [`IngestService`] — the row write path ([`IngestService::ingest_rows`],
//!   with schema-on-write auto-create).

use std::sync::Arc;

use teodb_core::traits::catalog::Catalog;

use crate::buffer::BufferRegistry;
use crate::idempotency::IdempotencyIndex;

pub mod ingest;

pub use ingest::IngestOutcome;

/// The row-ingestion write path shared by the REST and Flight handlers.
///
/// Holds `Arc` handles to its collaborators and is constructed once at startup
/// (stored on `AppState`).
#[derive(Clone)]
pub struct IngestService {
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) buffers: Arc<BufferRegistry>,
    pub(crate) wal: Arc<teodb_storage::wal::WalManager>,
    pub(crate) idempotency: Arc<IdempotencyIndex>,
    pub(crate) default_warehouse_uri: Arc<str>,
}

impl IngestService {
    /// Build the ingest service from its collaborators.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        buffers: Arc<BufferRegistry>,
        wal: Arc<teodb_storage::wal::WalManager>,
        idempotency: Arc<IdempotencyIndex>,
        default_warehouse_uri: Arc<str>,
    ) -> Self {
        Self {
            catalog,
            buffers,
            wal,
            idempotency,
            default_warehouse_uri,
        }
    }
}
