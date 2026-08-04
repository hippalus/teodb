use std::borrow::Cow;
use std::error::Error;

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::rustfs::RustFS;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, Image, ImageExt};

const RUSTFS_TAG: &str = "latest@sha256:41fe89380f4120a337790c02af192c3fe7bb55c3edc2e6e9357b487b47c6ab21";
const AWS_CLI_TAG: &str = "latest@sha256:96db65a10383b131af10ff22d6b9eb57eccc4eb65d0ef388ff8368505e22ba1c";
const POSTGRES_TAG: &str = "18.4-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15";
const ICEBERG_REST_TAG: &str = "latest@sha256:3b7d31bdfec626b68e97531c9778a1b9119659e456fe28545a49f6aa6a9ce472";

const ACCESS_KEY: &str = "teodbadmin";
const SECRET_KEY: &str = "teodbadmin123";
const REGION: &str = "us-east-1";

/// Keep the upstream RustFS module's image configuration while replacing its
/// log-based readiness probe. The pinned RustFS image no longer emits the
/// module's `RustFS Http API:` marker; the bucket bootstrap below performs the
/// authoritative S3 readiness poll.
#[derive(Debug, Clone, Default)]
struct RustFsForTests(RustFS);

impl Image for RustFsForTests {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn tag(&self) -> &str {
        self.0.tag()
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::seconds(1)]
    }

    fn env_vars(&self) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        self.0.env_vars()
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        self.0.cmd()
    }
}

pub(super) struct ManagedStack {
    _postgres: ContainerAsync<Postgres>,
    _rustfs: ContainerAsync<RustFsForTests>,
    _bucket_init: ContainerAsync<GenericImage>,
    _iceberg_rest: ContainerAsync<GenericImage>,
    pub catalog_uri: String,
    pub s3_endpoint: String,
}

impl ManagedStack {
    pub async fn start() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let suffix = uuid::Uuid::now_v7().simple().to_string();
        let network = format!("teodb-test-{suffix}");
        let postgres_name = format!("teodb-pg-{suffix}");
        let rustfs_name = format!("teodb-rustfs-{suffix}");
        let iceberg_name = format!("teodb-iceberg-{suffix}");

        let postgres = Postgres::default()
            .with_db_name("iceberg_catalog")
            .with_user("iceberg")
            .with_password("iceberg")
            .with_tag(POSTGRES_TAG)
            .with_network(network.clone())
            .with_container_name(postgres_name.clone())
            .start()
            .await?;

        let rustfs = RustFsForTests::default()
            .with_tag(RUSTFS_TAG)
            .with_network(network.clone())
            .with_container_name(rustfs_name.clone())
            .with_env_var("RUSTFS_VOLUMES", "/data/rustfs0")
            .with_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY)
            .with_env_var("RUSTFS_SECRET_KEY", SECRET_KEY)
            .with_env_var("RUSTFS_CONSOLE_ENABLE", "false")
            .start()
            .await?;

        let bucket_command = format!(
            "for i in $(seq 1 60); do \
             aws --endpoint-url http://{rustfs_name}:9000 s3 ls >/dev/null 2>&1 && break; \
             sleep 1; \
             done; \
             aws --endpoint-url http://{rustfs_name}:9000 s3 mb s3://teodb 2>/dev/null || true; \
             echo 'Bucket teodb ready'; \
             sleep infinity"
        );
        let bucket_init = GenericImage::new("amazon/aws-cli", AWS_CLI_TAG)
            .with_entrypoint("/bin/sh")
            .with_wait_for(WaitFor::message_on_stdout("Bucket teodb ready"))
            .with_network(network.clone())
            .with_container_name(format!("teodb-bucket-{suffix}"))
            .with_env_var("AWS_ACCESS_KEY_ID", ACCESS_KEY)
            .with_env_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
            .with_env_var("AWS_REGION", REGION)
            .with_cmd(["-c", bucket_command.as_str()])
            .start()
            .await?;

        let iceberg_rest = GenericImage::new("tabulario/iceberg-rest", ICEBERG_REST_TAG)
            .with_exposed_port(8181.tcp())
            .with_wait_for(WaitFor::seconds(2))
            .with_network(network)
            .with_container_name(iceberg_name)
            .with_env_var("CATALOG_WAREHOUSE", "s3://teodb/")
            .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
            .with_env_var("CATALOG_S3_ENDPOINT", format!("http://{rustfs_name}:9000"))
            .with_env_var("CATALOG_S3_PATH__STYLE__ACCESS", "true")
            .with_env_var(
                "CATALOG_URI",
                format!("jdbc:postgresql://{postgres_name}:5432/iceberg_catalog"),
            )
            .with_env_var("CATALOG_JDBC_USER", "iceberg")
            .with_env_var("CATALOG_JDBC_PASSWORD", "iceberg")
            .with_env_var("AWS_ACCESS_KEY_ID", ACCESS_KEY)
            .with_env_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
            .with_env_var("AWS_REGION", REGION)
            .start()
            .await?;

        let catalog_host = iceberg_rest.get_host().await?;
        let catalog_port = iceberg_rest.get_host_port_ipv4(8181).await?;
        let rustfs_host = rustfs.get_host().await?;
        let rustfs_port = rustfs.get_host_port_ipv4(9000).await?;

        Ok(Self {
            _postgres: postgres,
            _rustfs: rustfs,
            _bucket_init: bucket_init,
            _iceberg_rest: iceberg_rest,
            catalog_uri: format!("http://{catalog_host}:{catalog_port}"),
            s3_endpoint: format!("http://{rustfs_host}:{rustfs_port}"),
        })
    }
}
