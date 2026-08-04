use std::sync::Arc;

use teodb_core::traits::catalog::Catalog;
use teodb_core::traits::storage::StorageFactory;
use teodb_ingest::buffer::BufferRegistry;
use teodb_ingest::idempotency::IdempotencyIndex;

mod query;
pub(crate) mod table;

pub use query::SqlRouting;
pub use table::{data_type_from_keyword, partition_field_specs};

#[derive(Clone)]
pub struct DdlService {
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) storage_factory: Arc<dyn StorageFactory>,
    pub(crate) buffers: Arc<BufferRegistry>,
    pub(crate) wal: Arc<teodb_storage::wal::WalManager>,
    pub(crate) idempotency: Arc<IdempotencyIndex>,
    pub(crate) default_warehouse_uri: Arc<str>,
}

impl DdlService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog: Arc<dyn Catalog>,
        storage_factory: Arc<dyn StorageFactory>,
        buffers: Arc<BufferRegistry>,
        wal: Arc<teodb_storage::wal::WalManager>,
        idempotency: Arc<IdempotencyIndex>,
        default_warehouse_uri: Arc<str>,
    ) -> Self {
        Self {
            catalog,
            storage_factory,
            buffers,
            wal,
            idempotency,
            default_warehouse_uri,
        }
    }
}
