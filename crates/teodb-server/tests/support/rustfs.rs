use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt;

use teodb_core::ident::TableIdent;
use teodb_core::location::{ObjectLocation, ObjectPath, StorageScheme};
use teodb_core::schema::{ColumnMeta, TeoDataType};
use teodb_core::traits::catalog::{Catalog, DropTableOptions};
use teodb_core::traits::storage::{ObjectMeta, Storage, StorageFactory};
use teodb_query::ddl::{CreateSchemaPlan, CreateTablePlan, DdlExecutor, DdlPlan, DropTablePlan, PartitionFieldDef};
use teodb_storage::{DefaultStorageFactory, ObjectStoreBackend};

use super::managed_stack::ManagedStack;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

#[derive(Debug, Clone)]
pub struct TestEnv {
    pub catalog_uri: String,
    pub warehouse: String,
    pub bucket: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

impl TestEnv {
    /// Use an explicitly configured external stack when
    /// `TEODB_TEST_CATALOG_URI` is set; otherwise start one isolated,
    /// digest-pinned Testcontainers stack for this test process.
    pub async fn resolve() -> Self {
        if std::env::var_os("TEODB_TEST_CATALOG_URI").is_some() {
            return Self::from_env();
        }
        static STACK: tokio::sync::OnceCell<ManagedStack> = tokio::sync::OnceCell::const_new();
        let stack = STACK
            .get_or_init(|| async {
                ManagedStack::start()
                    .await
                    .expect("start Testcontainers RustFS/Iceberg stack")
            })
            .await;
        Self {
            catalog_uri: stack.catalog_uri.clone(),
            warehouse: "s3://teodb".into(),
            bucket: "teodb".into(),
            endpoint: stack.s3_endpoint.clone(),
            access_key: "teodbadmin".into(),
            secret_key: "teodbadmin123".into(),
            region: "us-east-1".into(),
        }
    }

    pub fn from_env() -> Self {
        let warehouse = env_or("TEODB_TEST_WAREHOUSE", "s3://teodb");
        let bucket = warehouse
            .strip_prefix("s3://")
            .and_then(|rest| rest.split('/').next())
            .expect("warehouse must be s3://<bucket>")
            .to_owned();
        Self {
            catalog_uri: env_or("TEODB_TEST_CATALOG_URI", "http://localhost:8181"),
            warehouse,
            bucket,
            endpoint: env_or("AWS_ENDPOINT_URL", "http://localhost:19000"),
            access_key: env_or("AWS_ACCESS_KEY_ID", "teodbadmin"),
            secret_key: env_or("AWS_SECRET_ACCESS_KEY", "teodbadmin123"),
            region: env_or("AWS_REGION", "us-east-1"),
        }
    }

    pub fn unique_namespace(&self, prefix: &str) -> String {
        format!("{prefix}_{}", uuid::Uuid::now_v7().simple())
    }

    pub fn s3_props(&self) -> HashMap<String, String> {
        HashMap::from([
            ("s3.endpoint".into(), self.endpoint.clone()),
            ("s3.access-key-id".into(), self.access_key.clone()),
            ("s3.secret-access-key".into(), self.secret_key.clone()),
            ("s3.region".into(), self.region.clone()),
            ("client.region".into(), self.region.clone()),
            ("s3.path-style-access".into(), "true".into()),
            ("s3.disable-ec2-metadata".into(), "true".into()),
            ("s3.disable-config-load".into(), "true".into()),
        ])
    }

    pub async fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog_at(&self.catalog_uri).await
    }

    pub async fn catalog_at(&self, uri: &str) -> Arc<dyn Catalog> {
        let config = teodb_catalog::IcebergCatalogConfig {
            uri: uri.to_owned(),
            warehouse: self.warehouse.clone(),
            credentials: teodb_catalog::IcebergCredentials::None,
            retry: teodb_catalog::RetryConfig::default(),
            request_timeout: Duration::from_secs(5),
            s3_props: self.s3_props(),
            max_writer_checkpoints_per_table: 32,
        };
        let adapter = teodb_catalog::IcebergCatalogAdapter::open(config)
            .await
            .expect("open Iceberg REST catalog (is docker-compose.rustfs.yaml up?)");
        Arc::new(adapter)
    }

    pub fn backend(&self) -> Arc<ObjectStoreBackend> {
        let store = object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(&self.bucket)
            .with_endpoint(&self.endpoint)
            .with_access_key_id(&self.access_key)
            .with_secret_access_key(&self.secret_key)
            .with_region(&self.region)
            .with_allow_http(true)
            .build()
            .expect("build S3 object store");
        Arc::new(ObjectStoreBackend::new(Arc::new(store)))
    }

    pub fn factory(&self, storage: Arc<dyn Storage>) -> Arc<dyn StorageFactory> {
        Arc::new(DefaultStorageFactory::new((
            StorageScheme::S3,
            self.bucket.clone(),
            storage,
        )))
    }
}

pub fn id_column() -> ColumnMeta {
    ColumnMeta {
        id: 1,
        name: "id".into(),
        data_type: TeoDataType::Int64,
        nullable: false,
        doc: None,
    }
}

pub fn string_column(id: i32, name: &str) -> ColumnMeta {
    ColumnMeta {
        id,
        name: name.into(),
        data_type: TeoDataType::Utf8,
        nullable: false,
        doc: None,
    }
}

pub fn table_plan(
    namespace: &str,
    table: &str,
    columns: Vec<ColumnMeta>,
    partition_by: Vec<PartitionFieldDef>,
) -> CreateTablePlan {
    CreateTablePlan {
        namespace: namespace.to_owned(),
        table_name: table.to_owned(),
        columns,
        partition_by,
        if_not_exists: false,
    }
}

pub async fn create_table(
    env: &TestEnv,
    catalog: Arc<dyn Catalog>,
    factory: Arc<dyn StorageFactory>,
    plan: CreateTablePlan,
) -> TableIdent {
    let ident = TableIdent::new(&plan.namespace, &plan.table_name);
    let executor = DdlExecutor::new(catalog, env.warehouse.clone()).with_storage_factory(factory);
    executor
        .execute(DdlPlan::CreateSchema(CreateSchemaPlan {
            namespace: plan.namespace.clone(),
            if_not_exists: true,
        }))
        .await
        .expect("create integration namespace");
    executor
        .execute(DdlPlan::CreateTable(plan))
        .await
        .expect("create integration table");
    ident
}

pub async fn purge_table(
    env: &TestEnv,
    catalog: Arc<dyn Catalog>,
    factory: Arc<dyn StorageFactory>,
    ident: TableIdent,
) {
    let namespace = ident.namespace.clone();
    let executor = DdlExecutor::new(catalog.clone(), env.warehouse.clone()).with_storage_factory(factory);
    let _ = executor
        .execute(DdlPlan::DropTable(DropTablePlan {
            ident,
            if_exists: true,
            options: DropTableOptions { purge: true },
        }))
        .await;
    let _ = catalog.drop_namespace(&namespace).await;
}

pub async fn objects_under(storage: &dyn Storage, prefix: &str) -> Vec<ObjectMeta> {
    storage
        .list(&ObjectPath::new(prefix))
        .await
        .expect("list integration prefix")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect integration listing")
}

pub async fn assert_live_parquet(
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    ident: &TableIdent,
    expected_files: usize,
    expected_rows: u64,
) -> Vec<ObjectLocation> {
    let live = catalog
        .load_live_files(ident)
        .await
        .expect("load authoritative live files");
    assert_eq!(live.len(), expected_files, "unexpected authoritative file count");
    assert_eq!(
        live.iter()
            .map(|file| file.record_count)
            .sum::<u64>(),
        expected_rows,
        "unexpected authoritative row count"
    );

    let mut locations = Vec::with_capacity(live.len());
    for file in live {
        let location = file.path;
        let bytes = storage
            .get(&ObjectPath::new(location.key.clone()))
            .await
            .expect("read live Parquet object");
        assert!(
            bytes.starts_with(b"PAR1") && bytes.ends_with(b"PAR1"),
            "live object must be a complete Parquet file: {}",
            location.key
        );
        locations.push(location);
    }
    locations
}
