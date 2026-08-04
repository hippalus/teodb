//! DataFusion catalog/schema providers backed by the TeoDB catalog trait.

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::Result as DFResult;
use moka::sync::Cache;

use teodb_core::traits::catalog::Catalog;
use teodb_core::traits::storage::StorageFactory;

use super::metadata_cache::{MetadataCache, MetadataMetricsSnapshot};
use super::table_loader::DataFusionTableLoader;

const DEFAULT_SCHEMA_PROVIDER_CACHE_MAX_ENTRIES: u64 = 1_000;

pub struct TeoCatalogProvider {
    catalog: Arc<dyn Catalog>,
    storage_factory: Arc<dyn StorageFactory>,
    metadata_ttl: Duration,
    schemas: Cache<String, Arc<dyn SchemaProvider>>,
}

impl Debug for TeoCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TeoCatalogProvider")
            .field("schemas_cached", &self.schemas.entry_count())
            .finish()
    }
}

impl TeoCatalogProvider {
    pub fn new(catalog: Arc<dyn Catalog>, storage_factory: Arc<dyn StorageFactory>) -> Self {
        Self {
            catalog,
            storage_factory,
            metadata_ttl: Duration::ZERO,
            schemas: Cache::builder()
                .max_capacity(DEFAULT_SCHEMA_PROVIDER_CACHE_MAX_ENTRIES)
                .build(),
        }
    }

    pub fn with_metadata_ttl(mut self, ttl: Duration) -> Self {
        self.metadata_ttl = ttl;
        self
    }

    #[cfg(test)]
    pub(super) fn with_schema_cache_capacity(mut self, capacity: u64) -> Self {
        self.schemas = Cache::builder().max_capacity(capacity).build();
        self
    }

    #[cfg(test)]
    pub(super) fn schema_cache_len(&self) -> u64 {
        self.schemas.entry_count()
    }

    #[cfg(test)]
    pub(super) fn run_schema_cache_pending(&self) {
        self.schemas.run_pending_tasks();
    }

    #[cfg(test)]
    pub(super) fn invalidate_schema(&self, name: &str) {
        self.schemas.invalidate(name);
        self.schemas.run_pending_tasks();
    }
}

impl CatalogProvider for TeoCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.schemas
            .iter()
            .map(|(name, _)| name.as_ref().clone())
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(self.schemas.get_with(name.to_owned(), || {
            Arc::new(TeoSchemaProvider::new(
                name.to_owned(),
                self.catalog.clone(),
                self.storage_factory.clone(),
                self.metadata_ttl,
            )) as Arc<dyn SchemaProvider>
        }))
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        let previous = self.schemas.get(name);
        self.schemas.insert(name.to_owned(), schema);
        Ok(previous)
    }
}

pub struct TeoSchemaProvider {
    loader: DataFusionTableLoader,
    cache: MetadataCache,
}

impl Debug for TeoSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TeoSchemaProvider")
            .field("namespace", &self.loader.namespace())
            .finish()
    }
}

impl TeoSchemaProvider {
    pub fn new(
        namespace: String,
        catalog: Arc<dyn Catalog>,
        storage_factory: Arc<dyn StorageFactory>,
        metadata_ttl: Duration,
    ) -> Self {
        Self {
            loader: DataFusionTableLoader::new(namespace, catalog, storage_factory),
            cache: MetadataCache::new(metadata_ttl),
        }
    }

    pub fn metadata_metrics(&self) -> MetadataMetricsSnapshot {
        self.cache.metrics_snapshot()
    }
}

#[async_trait]
impl SchemaProvider for TeoSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.cache.table_names_snapshot(&self.loader)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Ok(self
            .cache
            .table(name, &self.loader)
            .await?
            .map(|provider| provider as Arc<dyn TableProvider>))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.cache
            .table_names_snapshot(&self.loader)
            .iter()
            .any(|table| table == name)
    }
}
