use std::sync::Arc;

use teodb_core::traits::authz::Principal;
use teodb_test_support::{MockCatalog, stub_storage_factory};

use super::*;

#[test]
fn runtime_creates_spill_directory_and_accepts_object_store_registration() {
    let temp = tempfile::tempdir().unwrap();
    let spill_dir = temp.path().join("query-spill");
    let runtime = DataFusionRuntime::try_new(&DataFusionRuntimeConfig {
        memory_pool_bytes: 64 * 1024 * 1024,
        spill_dir: spill_dir.clone(),
    })
    .unwrap();

    assert!(spill_dir.is_dir());
    runtime
        .register_object_store("s3://warehouse", Arc::new(object_store::memory::InMemory::new()))
        .unwrap();
}

#[test]
fn creates_session_state_with_teodb_bindings() {
    let temp = tempfile::tempdir().unwrap();
    let runtime_config = DataFusionRuntimeConfig {
        memory_pool_bytes: 64 * 1024 * 1024,
        spill_dir: temp.path().to_path_buf(),
    };
    let config = DataFusionSessionConfig {
        batch_size: 4096,
        target_partitions: 2,
        ..Default::default()
    };

    let factory = DataFusionSessionFactory::new(
        Arc::new(MockCatalog::empty()),
        stub_storage_factory(),
        DataFusionRuntime::try_new(&runtime_config).unwrap(),
        config,
    )
    .unwrap();

    let state = factory
        .session_state_for_principal(&Principal {
            subject: "test".into(),
            roles: vec![],
            claims: Default::default(),
        })
        .unwrap();
    assert!(
        state
            .scalar_functions()
            .contains_key("URLPathHash")
    );
}
