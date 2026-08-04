use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use teodb_core::error::TeoDBResult;
use teodb_core::traits::catalog::Catalog;
use teodb_core::traits::storage::StorageFactory;
use teodb_core::write_protocol::{ClusterId, NodeId, ResolvedIdentity, WriterSlot};
use teodb_ingest::buffer::BufferRegistry;
use teodb_ingest::flush::Flusher;
use teodb_ingest::idempotency::IdempotencyIndex;
use teodb_ingest::replay::Replayer;
use teodb_ingest::service::IngestService;
use teodb_storage::wal::{WalConfig, WalIdentityConfig, WalManager};

pub struct WriterRuntime {
    pub wal: Arc<WalManager>,
    pub buffers: Arc<BufferRegistry>,
    pub idempotency: Arc<IdempotencyIndex>,
    pub ingest: IngestService,
    pub flusher: Flusher,
}

pub struct WriterHarness {
    wal_dir: tempfile::TempDir,
    cluster_id: ClusterId,
    node_id: NodeId,
    writer_slot: WriterSlot,
    catalog: Arc<dyn Catalog>,
    storage_factory: Arc<dyn StorageFactory>,
    warehouse: Arc<str>,
    runtime: Option<WriterRuntime>,
    ready: bool,
}

impl WriterHarness {
    pub async fn new(
        catalog: Arc<dyn Catalog>,
        storage_factory: Arc<dyn StorageFactory>,
        warehouse: impl Into<Arc<str>>,
        cluster_id: ClusterId,
        slot: &str,
    ) -> TeoDBResult<Self> {
        let mut harness = Self {
            wal_dir: tempfile::tempdir()
                .map_err(|error| teodb_core::error::TeoDBError::wal_source("create writer WAL directory", error))?,
            cluster_id,
            node_id: NodeId::new(format!("node-{slot}"))?,
            writer_slot: WriterSlot::new(slot)?,
            catalog,
            storage_factory,
            warehouse: warehouse.into(),
            runtime: None,
            ready: false,
        };
        harness.start().await?;
        Ok(harness)
    }

    fn wal_config(&self) -> WalConfig {
        WalConfig {
            root_dir: self.wal_dir.path().to_path_buf(),
            fsync_on_append: true,
            identity: WalIdentityConfig {
                cluster_id: Some(self.cluster_id),
                node_id: Some(self.node_id.clone()),
                writer_slot: Some(self.writer_slot.clone()),
            },
            ..WalConfig::default()
        }
    }

    async fn build_runtime(&self) -> TeoDBResult<WriterRuntime> {
        let wal = Arc::new(WalManager::open(self.wal_config()).await?);
        let buffers = Arc::new(BufferRegistry::new(wal.clone(), 64 * 1024 * 1024, 48 * 1024 * 1024));
        let idempotency = Arc::new(IdempotencyIndex::new(Duration::from_secs(60), 1_000));
        let ingest = IngestService::new(
            self.catalog.clone(),
            buffers.clone(),
            wal.clone(),
            idempotency.clone(),
            self.warehouse.clone(),
        );
        let flusher = Flusher::new(
            buffers.clone(),
            self.catalog.clone(),
            self.storage_factory.clone(),
            wal.clone(),
        );
        Ok(WriterRuntime {
            wal,
            buffers,
            idempotency,
            ingest,
            flusher,
        })
    }

    pub async fn start(&mut self) -> TeoDBResult<()> {
        assert!(self.runtime.is_none(), "writer runtime is already started");
        let runtime = self.build_runtime().await?;
        self.runtime = Some(runtime);
        self.ready = true;
        Ok(())
    }

    pub fn crash(&mut self) {
        self.ready = false;
        self.runtime = None;
    }

    pub async fn restart(&mut self) -> TeoDBResult<()> {
        assert!(self.runtime.is_none(), "crash writer before restart");
        self.ready = false;
        let runtime = self.build_runtime().await?;
        let replayer = Replayer::new(
            runtime.wal.clone(),
            runtime.buffers.clone(),
            self.catalog.clone(),
            runtime.idempotency.clone(),
        );
        replayer.replay_wal(None).await?;
        self.runtime = Some(runtime);
        self.ready = true;
        Ok(())
    }

    pub fn replace_catalog(&mut self, catalog: Arc<dyn Catalog>) {
        assert!(self.runtime.is_none(), "replace catalog only while crashed");
        self.catalog = catalog;
    }

    pub fn runtime(&self) -> &WriterRuntime {
        self.runtime
            .as_ref()
            .expect("writer runtime is not running")
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }

    pub fn identity(&self) -> ResolvedIdentity {
        self.runtime().wal.writer_identity()
    }

    pub fn wal_root(&self) -> &Path {
        self.wal_dir.path()
    }

    pub async fn assert_clean(&self) {
        assert!(
            self.runtime()
                .wal
                .list_prepared()
                .await
                .expect("list prepared sidecars")
                .is_empty(),
            "writer must not retain a prepared sidecar"
        );
    }
}
