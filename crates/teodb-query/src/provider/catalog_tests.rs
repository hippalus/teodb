use super::*;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};

use teodb_test_support::{MockCatalog, stub_storage_factory, table_metadata};

fn make_provider() -> TeoCatalogProvider {
    let catalog = MockCatalog::builder()
        .namespaces(["default"])
        .build();
    TeoCatalogProvider::new(Arc::new(catalog), stub_storage_factory())
}

#[test]
fn schema_always_returns_some() {
    let provider = make_provider();
    assert!(provider.schema("any_namespace").is_some());
}

#[test]
fn schema_cache_preserves_identity_and_is_bounded() {
    let provider = make_provider().with_schema_cache_capacity(8);
    let first = provider.schema("same").unwrap();
    let second = provider.schema("same").unwrap();
    assert!(Arc::ptr_eq(&first, &second));

    for index in 0..64 {
        provider.schema(&format!("namespace-{index}"));
    }
    provider.run_schema_cache_pending();
    assert!(provider.schema_cache_len() <= 8);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_schema_survives_eviction_and_recreation_is_safe() {
    let provider = make_provider().with_schema_cache_capacity(2);
    let held = provider.schema("held").unwrap();
    provider.invalidate_schema("held");
    let recreated = provider.schema("held").unwrap();

    assert!(!Arc::ptr_eq(&held, &recreated));
    assert_eq!(held.table_names(), recreated.table_names());
}

#[test]
fn register_schema_returns_previous_provider() {
    let provider = make_provider();
    let first = provider.schema("registered").unwrap();
    let replacement = provider.schema("replacement").unwrap();

    let previous = provider
        .register_schema("registered", replacement.clone())
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&previous, &first));
    assert!(Arc::ptr_eq(&provider.schema("registered").unwrap(), &replacement));
}

#[tokio::test]
async fn table_not_found_returns_none() {
    let provider = make_provider();
    let schema = provider.schema("test").unwrap();
    let table = schema.table("nonexistent").await.unwrap();
    assert!(table.is_none());
}

/// A catalog serving one table; its `load_table` counter exercises the
/// metadata TTL cache.
fn counting_catalog() -> MockCatalog {
    MockCatalog::builder()
        .namespaces(["default"])
        .serves_any(table_metadata("file:///data/events"))
        .build()
}

#[tokio::test]
async fn ttl_cache_serves_repeat_lookups_without_reloading() {
    let catalog = Arc::new(counting_catalog());
    let schema = TeoSchemaProvider::new(
        "default".into(),
        catalog.clone(),
        stub_storage_factory(),
        std::time::Duration::from_secs(60),
    );

    assert!(schema.table("events").await.unwrap().is_some());
    assert!(schema.table("events").await.unwrap().is_some());
    assert!(schema.table("events").await.unwrap().is_some());

    assert_eq!(
        catalog.load_table_calls(),
        1,
        "fresh cache entry must be served without catalog round-trips"
    );
}

#[tokio::test]
async fn zero_ttl_reloads_on_every_lookup() {
    let catalog = Arc::new(counting_catalog());
    let schema = TeoSchemaProvider::new(
        "default".into(),
        catalog.clone(),
        stub_storage_factory(),
        std::time::Duration::ZERO,
    );

    assert!(schema.table("events").await.unwrap().is_some());
    assert!(schema.table("events").await.unwrap().is_some());

    assert_eq!(
        catalog.load_table_calls(),
        2,
        "TTL 0 preserves the reload-per-query behavior"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_names_serves_cache_and_refreshes_off_worker() {
    use teodb_core::ident::TableIdent;
    let catalog = Arc::new(
        MockCatalog::builder()
            .namespaces(["default"])
            .tables([TableIdent::new("default", "events")])
            .serves_any(table_metadata("file:///data/events"))
            .build(),
    );
    let schema = TeoSchemaProvider::new(
        "default".into(),
        catalog.clone(),
        stub_storage_factory(),
        std::time::Duration::from_millis(1),
    );

    // Cold cache: one blocking load returns the listing.
    assert_eq!(schema.table_names(), vec!["events".to_string()]);

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    // Stale: returns the cached value immediately and schedules a background
    // refresh (no blocking).
    assert_eq!(schema.table_names(), vec!["events".to_string()]);
    // Give the spawned refresh time to complete; listing stays correct.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(schema.table_names(), vec!["events".to_string()]);

    // The stale serve and the background refresh are both observable (P2-5).
    let metrics = schema.metadata_metrics();
    assert!(metrics.stale_serves >= 1, "serving a stale listing must be counted");
    assert!(metrics.refresh_success >= 1, "the background refresh must be counted");
}

#[tokio::test]
async fn expired_entry_is_refreshed() {
    let catalog = Arc::new(counting_catalog());
    let schema = TeoSchemaProvider::new(
        "default".into(),
        catalog.clone(),
        stub_storage_factory(),
        std::time::Duration::from_millis(1),
    );

    assert!(schema.table("events").await.unwrap().is_some());
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(schema.table("events").await.unwrap().is_some());

    assert_eq!(
        catalog.load_table_calls(),
        2,
        "stale entry triggers a single-flight refresh"
    );

    // The successful refresh is recorded for observability (P2-5).
    let metrics = schema.metadata_metrics();
    assert!(
        metrics.refresh_success >= 1,
        "a successful metadata refresh must be counted"
    );
}
