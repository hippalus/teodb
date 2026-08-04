//! Object-store lifecycle integration test.
//!
//! The real multi-writer/restart/fault matrix lives in
//! `multi_writer_rest.rs`; this binary retains the distinct `DROP TABLE PURGE`
//! invariant. It requires `deploy/docker/docker-compose.rustfs.yaml`.

use bytes::Bytes;

use teodb_core::ident::TableIdent;
use teodb_core::location::ObjectPath;
use teodb_core::traits::catalog::DropTableOptions;
use teodb_core::traits::storage::Storage;
use teodb_query::ddl::{CreateSchemaPlan, CreateTablePlan, DdlExecutor, DdlPlan, DropTablePlan};

mod support;

use support::rustfs::{TestEnv, id_column, objects_under};

/// Create a table (writes metadata to S3), drop it with PURGE, and assert the
/// table's object-store prefix is reclaimed while a sibling table is untouched.
#[tokio::test]
#[ignore = "requires Docker for RustFS + Iceberg REST"]
async fn drop_purge_reclaims_only_the_dropped_table_prefix() {
    let env = TestEnv::resolve().await;
    let catalog = env.catalog().await;
    let backend = env.backend();
    let factory = env.factory(backend.clone());
    let namespace = env.unique_namespace("teodb_it");
    let table = "events";
    let sibling = "events_keep";
    let executor = DdlExecutor::new(catalog.clone(), env.warehouse.clone()).with_storage_factory(factory);

    executor
        .execute(DdlPlan::CreateSchema(CreateSchemaPlan {
            namespace: namespace.clone(),
            if_not_exists: true,
        }))
        .await
        .expect("create namespace");

    for name in [table, sibling] {
        executor
            .execute(DdlPlan::CreateTable(CreateTablePlan {
                namespace: namespace.clone(),
                table_name: name.into(),
                columns: vec![id_column()],
                partition_by: vec![],
                if_not_exists: true,
            }))
            .await
            .unwrap_or_else(|error| panic!("create table {name}: {error}"));
    }

    let data_key = format!("{namespace}/{table}/data/manual-0.parquet");
    let sibling_key = format!("{namespace}/{sibling}/data/keep-0.parquet");
    backend
        .put(&ObjectPath::new(&data_key), Bytes::from_static(b"rows"))
        .await
        .expect("put table data object");
    backend
        .put(&ObjectPath::new(&sibling_key), Bytes::from_static(b"rows"))
        .await
        .expect("put sibling data object");

    let table_prefix = format!("{namespace}/{table}/");
    assert!(
        !objects_under(backend.as_ref(), &table_prefix)
            .await
            .is_empty(),
        "table prefix should hold objects before purge"
    );

    executor
        .execute(DdlPlan::DropTable(DropTablePlan {
            ident: TableIdent::new(&namespace, table),
            if_exists: false,
            options: DropTableOptions { purge: true },
        }))
        .await
        .expect("drop table purge");

    assert_eq!(
        objects_under(backend.as_ref(), &table_prefix)
            .await
            .len(),
        0,
        "purge must delete every object under the dropped table's prefix"
    );
    assert!(
        backend
            .head(&ObjectPath::new(&sibling_key))
            .await
            .is_ok(),
        "purge must not touch a sibling table's objects"
    );

    let _ = executor
        .execute(DdlPlan::DropTable(DropTablePlan {
            ident: TableIdent::new(&namespace, sibling),
            if_exists: true,
            options: DropTableOptions { purge: true },
        }))
        .await;
    let _ = catalog.drop_namespace(&namespace).await;
}
