use std::sync::Arc;

use datafusion::common::{DataFusionError, Result as DFResult};
use tracing::debug;

use teodb_core::ident::TableIdent;
use teodb_core::traits::catalog::Catalog;
use teodb_core::traits::storage::StorageFactory;

use super::TeoTableProvider;

#[derive(Clone)]
pub(super) struct DataFusionTableLoader {
    namespace: String,
    catalog: Arc<dyn Catalog>,
    storage_factory: Arc<dyn StorageFactory>,
}

impl DataFusionTableLoader {
    pub(super) fn new(namespace: String, catalog: Arc<dyn Catalog>, storage_factory: Arc<dyn StorageFactory>) -> Self {
        Self {
            namespace,
            catalog,
            storage_factory,
        }
    }

    pub(super) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(super) fn blocking_load_table_names(&self) -> Vec<String> {
        let loader = self.clone();
        let result =
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(loader.load_table_names()));
        match result {
            Ok(names) => names,
            Err(error) => {
                tracing::warn!(namespace = %self.namespace, %error, "table_names: catalog lookup failed");
                Vec::new()
            }
        }
    }

    pub(super) async fn load_table_names(&self) -> teodb_core::error::TeoDBResult<Vec<String>> {
        let tables = self.catalog.list_tables(&self.namespace).await?;
        Ok(tables
            .into_iter()
            .map(|table| table.name.to_string())
            .collect())
    }

    pub(super) async fn load_provider(&self, name: &str) -> DFResult<Option<Arc<TeoTableProvider>>> {
        let ident = TableIdent::new(&self.namespace, name);
        debug!(table = %ident, "DataFusion resolving table from catalog");

        let table_metadata = match self.catalog.load_table(&ident).await {
            Ok(m) => m,
            Err(teodb_core::error::TeoDBError::NotFound { .. }) => return Ok(None),
            Err(e) => {
                return Err(DataFusionError::External(Box::new(std::io::Error::other(format!(
                    "catalog error loading {ident}: {e}"
                )))));
            }
        };

        let live_files = self
            .catalog
            .load_live_files(&ident)
            .await
            .map_err(|e| {
                DataFusionError::External(Box::new(std::io::Error::other(format!(
                    "failed to load data files for {ident}: {e}"
                ))))
            })?;

        let metadata = Arc::new(
            (*table_metadata)
                .clone()
                .with_live_files(live_files)
                .map_err(|e| {
                    DataFusionError::External(Box::new(std::io::Error::other(format!(
                        "invalid catalog metadata for {ident}: {e}"
                    ))))
                })?,
        );

        let (data_files, delete_files) = metadata
            .current_snapshot
            .as_ref()
            .map(|snapshot| (snapshot.data_files.len(), snapshot.delete_files.len()))
            .unwrap_or((0, 0));
        debug!(
            table = %ident,
            data_files,
            delete_files,
            "table metadata loaded"
        );

        let provider = TeoTableProvider::try_new(ident, metadata, self.catalog.clone(), self.storage_factory.clone())
            .map_err(|e| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "table provider creation failed: {e}"
            ))))
        })?;

        Ok(Some(Arc::new(provider)))
    }
}
