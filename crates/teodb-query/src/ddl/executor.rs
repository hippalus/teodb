//! DDL plan executor.
//!
//! Takes a parsed `DdlPlan` and runs it against the TeoDB catalog.

use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

use futures::TryStreamExt;
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::TableIdent;
use teodb_core::location::{ObjectLocation, ObjectPath};
use teodb_core::schema::SchemaDefinition;
use teodb_core::table::{CreateTableRequestBuilder, PartitionFieldSpec, PartitionSpecBuilder, PartitionTransformSpec};
use teodb_core::traits::catalog::Catalog;
use teodb_core::traits::storage::StorageFactory;

use super::plan::*;
use super::types::{DdlResult, DdlRow};

/// Executes DDL plans against the TeoDB catalog.
pub struct DdlExecutor {
    catalog: Arc<dyn Catalog>,
    default_warehouse_uri: String,
    storage_factory: Option<Arc<dyn StorageFactory>>,
}

impl DdlExecutor {
    pub fn new(catalog: Arc<dyn Catalog>, default_warehouse_uri: String) -> Self {
        Self {
            catalog,
            default_warehouse_uri,
            storage_factory: None,
        }
    }

    /// Attach storage access for destructive DDL options such as
    /// `DROP TABLE ... PURGE`.
    pub fn with_storage_factory(mut self, storage_factory: Arc<dyn StorageFactory>) -> Self {
        self.storage_factory = Some(storage_factory);
        self
    }

    /// Execute a DDL plan and return a result.
    #[tracing::instrument(
        name = "ddl.execute",
        skip_all,
        fields(operation = ddl_operation(&plan))
    )]
    pub async fn execute(&self, plan: DdlPlan) -> TeoDBResult<DdlResult> {
        debug!("executing DDL plan");
        match plan {
            DdlPlan::CreateTable(p) => self.create_table(p).await,
            DdlPlan::CreateSchema(p) => self.create_schema(p).await,
            DdlPlan::DropTable(p) => self.drop_table(p).await,
            DdlPlan::DropSchema(p) => self.drop_schema(p).await,
            DdlPlan::ShowTables(p) => self.show_tables(p).await,
            DdlPlan::ShowColumns(p) | DdlPlan::DescribeTable(p) => self.describe_table(p).await,
        }
    }

    async fn create_table(&self, plan: CreateTablePlan) -> TeoDBResult<DdlResult> {
        let ns = &plan.namespace;
        let tbl = &plan.table_name;

        // Auto-create namespace if it doesn't exist.
        self.ensure_namespace(ns).await?;

        let schema_def = SchemaDefinition {
            schema_id: 0,
            columns: plan.columns,
            identifier_field_ids: vec![],
        };

        let partition_spec = PartitionSpecBuilder::for_schema(&schema_def)
            .fields(partition_field_specs(&plan.partition_by))
            .build()?;

        let location =
            ObjectLocation::parse(&format!("{}/{ns}/{tbl}", self.default_warehouse_uri)).unwrap_or_else(|_| {
                ObjectLocation {
                    scheme: teodb_core::location::StorageScheme::S3,
                    bucket: Some("warehouse".into()),
                    key: format!("{ns}/{tbl}"),
                }
            });

        let req = CreateTableRequestBuilder::new(TableIdent::new(ns, tbl), schema_def, location)
            .partition_spec(partition_spec)
            .build()?;

        if plan.if_not_exists {
            match self.catalog.create_table(req).await {
                Ok(_) => {
                    info!(namespace = %ns, table = %tbl, "table created via DDL");
                    Ok(DdlResult::changed(format!("Table {ns}.{tbl} created")))
                }
                Err(TeoDBError::AlreadyExists { .. }) => {
                    Ok(DdlResult::unchanged(format!("Table {ns}.{tbl} already exists")))
                }
                Err(e) => Err(e),
            }
        } else {
            self.catalog.create_table(req).await?;
            info!(namespace = %ns, table = %tbl, "table created via DDL");
            Ok(DdlResult::changed(format!("Table {ns}.{tbl} created")))
        }
    }

    async fn create_schema(&self, plan: CreateSchemaPlan) -> TeoDBResult<DdlResult> {
        let ns = &plan.namespace;
        if plan.if_not_exists {
            match self
                .catalog
                .create_namespace(ns, HashMap::new())
                .await
            {
                Ok(()) => {
                    info!(namespace = %ns, "namespace created via DDL");
                    Ok(DdlResult::changed(format!("Schema {ns} created")))
                }
                Err(TeoDBError::AlreadyExists { .. }) => {
                    Ok(DdlResult::unchanged(format!("Schema {ns} already exists")))
                }
                Err(e) => Err(e),
            }
        } else {
            self.catalog
                .create_namespace(ns, HashMap::new())
                .await?;
            info!(namespace = %ns, "namespace created via DDL");
            Ok(DdlResult::changed(format!("Schema {ns} created")))
        }
    }

    async fn drop_table(&self, plan: DropTablePlan) -> TeoDBResult<DdlResult> {
        let table_location = if plan.options.purge {
            match self.catalog.load_table(&plan.ident).await {
                Ok(metadata) => Some(metadata.table_location.to_uri()),
                Err(TeoDBError::NotFound { .. }) if plan.if_exists => {
                    return Ok(DdlResult::unchanged(format!(
                        "Table {}.{} does not exist (IF EXISTS)",
                        plan.ident.namespace, plan.ident.name
                    )));
                }
                Err(error) => return Err(error),
            }
        } else {
            None
        };

        match self.catalog.drop_table(&plan.ident).await {
            Ok(()) => {
                if let Some(location) = table_location {
                    self.purge_table_storage(&location).await?;
                }
                info!(
                    namespace = %plan.ident.namespace,
                    table = %plan.ident.name,
                    "table dropped via DDL"
                );
                Ok(DdlResult::changed(format!(
                    "Table {}.{} dropped",
                    plan.ident.namespace, plan.ident.name
                )))
            }
            Err(TeoDBError::NotFound { .. }) if plan.if_exists => Ok(DdlResult::unchanged(format!(
                "Table {}.{} does not exist (IF EXISTS)",
                plan.ident.namespace, plan.ident.name
            ))),
            Err(e) => Err(e),
        }
    }

    async fn purge_table_storage(&self, table_location: &str) -> TeoDBResult<()> {
        let storage_factory = self
            .storage_factory
            .as_ref()
            .ok_or_else(|| TeoDBError::InvalidArgument {
                field: "sql".into(),
                message: "DROP TABLE PURGE requires storage access; use a DDL service wired with StorageFactory".into(),
            })?;
        let table_location =
            ObjectLocation::parse(table_location).map_err(|error| TeoDBError::Catalog(error.to_string()))?;
        let (storage, root_path) = storage_factory.resolve(&table_location).await?;
        let prefix = table_prefix_path(&root_path)?;
        let objects = storage
            .list(&prefix)
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        for object in &objects {
            storage.delete(&object.path).await?;
        }

        Ok(())
    }

    async fn drop_schema(&self, plan: DropSchemaPlan) -> TeoDBResult<DdlResult> {
        let ns = &plan.namespace;
        match self.catalog.drop_namespace(ns).await {
            Ok(()) => {
                info!(namespace = %ns, "namespace dropped via DDL");
                Ok(DdlResult::changed(format!("Schema {ns} dropped")))
            }
            Err(TeoDBError::NotFound { .. }) if plan.if_exists => {
                Ok(DdlResult::unchanged(format!("Schema {ns} does not exist (IF EXISTS)")))
            }
            Err(e) => Err(e),
        }
    }

    async fn show_tables(&self, plan: ShowTablesPlan) -> TeoDBResult<DdlResult> {
        let namespaces = if let Some(ns) = &plan.namespace {
            vec![ns.clone()]
        } else {
            self.catalog.list_namespaces().await?
        };

        let mut rows = Vec::new();
        for ns in &namespaces {
            // Surface catalog errors instead of silently returning an empty list,
            // which would make a catalog outage look like "no tables".
            let tables = self.catalog.list_tables(ns).await?;
            for t in tables {
                let mut row = DdlRow::new();
                row.insert("namespace".into(), serde_json::json!(t.namespace));
                row.insert("table_name".into(), serde_json::json!(t.name));
                rows.push(row);
            }
        }
        Ok(DdlResult::with_rows("OK", rows))
    }

    async fn describe_table(&self, plan: DescribeTablePlan) -> TeoDBResult<DdlResult> {
        let ident = TableIdent::new(&plan.namespace, &plan.table_name);
        let meta = self.catalog.load_table(&ident).await?;

        let schema = meta.current_schema()?;
        let mut rows = Vec::new();
        for field in &schema.columns {
            let mut row = DdlRow::new();
            row.insert("field_id".into(), serde_json::json!(field.id));
            row.insert("name".into(), serde_json::json!(field.name));
            row.insert("type".into(), serde_json::json!(field.data_type.to_string()));
            row.insert("nullable".into(), serde_json::json!(field.nullable));
            rows.push(row);
        }
        Ok(DdlResult::with_rows("OK", rows))
    }

    /// Ensure a namespace exists, creating it if needed.
    async fn ensure_namespace(&self, ns: &str) -> TeoDBResult<()> {
        match self
            .catalog
            .create_namespace(ns, HashMap::new())
            .await
        {
            Ok(()) | Err(TeoDBError::AlreadyExists { .. }) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

fn ddl_operation(plan: &DdlPlan) -> &'static str {
    match plan {
        DdlPlan::CreateTable(_) => "create_table",
        DdlPlan::CreateSchema(_) => "create_schema",
        DdlPlan::DropTable(_) => "drop_table",
        DdlPlan::DropSchema(_) => "drop_schema",
        DdlPlan::ShowTables(_) => "show_tables",
        DdlPlan::ShowColumns(_) => "show_columns",
        DdlPlan::DescribeTable(_) => "describe_table",
    }
}

fn table_prefix_path(root_path: &ObjectPath) -> TeoDBResult<ObjectPath> {
    let key = root_path.as_str().trim_matches('/');
    if key.is_empty() {
        return Err(TeoDBError::Config(
            "table location must include a non-empty object prefix before purge".into(),
        ));
    }
    Ok(ObjectPath::new(format!("{key}/")))
}

pub(crate) fn partition_field_specs(fields: &[PartitionFieldDef]) -> impl Iterator<Item = PartitionFieldSpec> + '_ {
    fields.iter().map(|field| {
        let transform = match &field.transform {
            PartitionTransformDef::Identity => PartitionTransformSpec::Identity,
            PartitionTransformDef::Year => PartitionTransformSpec::Year,
            PartitionTransformDef::Month => PartitionTransformSpec::Month,
            PartitionTransformDef::Day => PartitionTransformSpec::Day,
            PartitionTransformDef::Hour => PartitionTransformSpec::Hour,
            PartitionTransformDef::Bucket(n) => PartitionTransformSpec::Bucket(*n),
            PartitionTransformDef::Truncate(w) => PartitionTransformSpec::Truncate(*w),
        };
        PartitionFieldSpec::new(field.column_name.clone(), transform)
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use teodb_core::schema::{ColumnMeta, TeoDataType};
    use teodb_core::traits::catalog::DropTableOptions;
    use teodb_core::traits::storage::Storage;
    use teodb_test_support::{MockCatalog, in_memory_backend, single_backend_factory, table_metadata};

    use super::*;

    #[tokio::test]
    async fn sql_create_table_uses_current_warehouse_location_policy() {
        let catalog = Arc::new(
            MockCatalog::builder()
                .commit_result(table_metadata("s3://unused/result"))
                .build(),
        );
        let executor = DdlExecutor::new(catalog.clone(), "s3://ddl-warehouse".into());

        executor
            .execute(DdlPlan::CreateTable(CreateTablePlan {
                namespace: "analytics".into(),
                table_name: "events".into(),
                columns: vec![ColumnMeta {
                    id: 1,
                    name: "id".into(),
                    data_type: TeoDataType::Int64,
                    nullable: false,
                    doc: None,
                }],
                partition_by: vec![],
                if_not_exists: false,
            }))
            .await
            .unwrap();

        let created = catalog.created_tables();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].location.to_uri(), "s3://ddl-warehouse/analytics/events");
    }

    #[tokio::test]
    async fn sql_drop_table_purge_deletes_only_table_prefix() {
        let backend = in_memory_backend();
        for (path, body) in [
            ("analytics/events/data/file.parquet", b"table data".as_slice()),
            ("analytics/events/metadata/v1.metadata.json", b"metadata".as_slice()),
            ("analytics/events2/data/file.parquet", b"sibling".as_slice()),
        ] {
            backend
                .put(&ObjectPath::new(path), Bytes::copy_from_slice(body))
                .await
                .unwrap();
        }

        let catalog = Arc::new(
            MockCatalog::builder()
                .serves("events", table_metadata("s3://ddl-warehouse/analytics/events"))
                .build(),
        );
        let executor = DdlExecutor::new(catalog.clone(), "s3://ddl-warehouse".into())
            .with_storage_factory(single_backend_factory(backend.clone()));

        executor
            .execute(DdlPlan::DropTable(DropTablePlan {
                ident: TableIdent::new("analytics", "events"),
                if_exists: false,
                options: DropTableOptions { purge: true },
            }))
            .await
            .unwrap();

        assert_eq!(catalog.load_table_calls(), 1);
        assert_eq!(catalog.drop_table_calls(), 1);
        assert!(
            backend
                .head(&ObjectPath::new("analytics/events/data/file.parquet"))
                .await
                .is_err()
        );
        assert!(
            backend
                .head(&ObjectPath::new("analytics/events/metadata/v1.metadata.json"))
                .await
                .is_err()
        );
        assert!(
            backend
                .head(&ObjectPath::new("analytics/events2/data/file.parquet"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn sql_drop_table_purge_requires_storage_factory() {
        let catalog = Arc::new(
            MockCatalog::builder()
                .serves("events", table_metadata("s3://ddl-warehouse/analytics/events"))
                .build(),
        );
        let executor = DdlExecutor::new(catalog, "s3://ddl-warehouse".into());

        let error = executor
            .execute(DdlPlan::DropTable(DropTablePlan {
                ident: TableIdent::new("analytics", "events"),
                if_exists: false,
                options: DropTableOptions { purge: true },
            }))
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TeoDBError::InvalidArgument { ref field, .. } if field == "sql"
        ));
    }
}
