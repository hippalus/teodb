use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::config::{ProcessRole, TeoDBConfig};

pub(in crate::server) fn build_query_engine(
    cfg: &TeoDBConfig,
    catalog: &Arc<dyn teodb_core::traits::catalog::Catalog>,
    storage: &crate::builder::StorageComponents,
    snapshot_registry: Arc<dyn teodb_core::snapshot_pin::ActiveSnapshotRegistry>,
    event_observer: Option<Arc<dyn teodb_distributed::EngineEventObserver>>,
) -> eyre::Result<Arc<dyn teodb_query::QueryEngine>> {
    let target_partitions = target_partitions(cfg);
    let runtime = build_query_runtime(cfg, storage)?;
    let session_factory = build_session_factory(cfg, catalog, storage, runtime, target_partitions)?;
    build_engine(
        cfg,
        session_factory,
        target_partitions,
        snapshot_registry,
        event_observer,
    )
}

fn target_partitions(cfg: &TeoDBConfig) -> usize {
    if cfg.query.target_partitions > 0 {
        cfg.query.target_partitions
    } else {
        std::thread::available_parallelism().map_or(4, |count| count.get())
    }
}

fn build_query_runtime(
    cfg: &TeoDBConfig,
    storage: &crate::builder::StorageComponents,
) -> eyre::Result<teodb_query::DataFusionRuntime> {
    let runtime = teodb_query::DataFusionRuntime::try_new(&teodb_query::DataFusionRuntimeConfig {
        memory_pool_bytes: cfg.query.memory_pool_bytes,
        spill_dir: cfg.storage.spill_dir.clone(),
    })
    .map_err(|error| eyre::eyre!("failed to build query runtime: {error}"))?;
    runtime
        .register_object_store_registration(storage.object_store_registration())
        .map_err(|error| eyre::eyre!("failed to register query object store: {error}"))?;
    Ok(runtime)
}

fn build_session_factory(
    cfg: &TeoDBConfig,
    catalog: &Arc<dyn teodb_core::traits::catalog::Catalog>,
    storage: &crate::builder::StorageComponents,
    runtime: teodb_query::DataFusionRuntime,
    target_partitions: usize,
) -> eyre::Result<Arc<teodb_query::DataFusionSessionFactory>> {
    let config = teodb_query::DataFusionSessionConfig {
        batch_size: cfg.query.batch_size,
        target_partitions,
        metadata_refresh: Duration::from_secs(cfg.query.metadata_refresh_secs),
    };
    teodb_query::DataFusionSessionFactory::new(catalog.clone(), storage.factory.clone(), runtime, config)
        .map(Arc::new)
        .map_err(|error| eyre::eyre!("failed to build query session factory: {error}"))
}

fn build_engine(
    cfg: &TeoDBConfig,
    session_factory: Arc<teodb_query::DataFusionSessionFactory>,
    target_partitions: usize,
    snapshot_registry: Arc<dyn teodb_core::snapshot_pin::ActiveSnapshotRegistry>,
    event_observer: Option<Arc<dyn teodb_distributed::EngineEventObserver>>,
) -> eyre::Result<Arc<dyn teodb_query::QueryEngine>> {
    match cfg.role {
        ProcessRole::Standalone => {
            info!(
                parallelism = target_partitions,
                "building standalone Ballista query engine"
            );
            let engine = teodb_distributed::BallistaQueryEngineBuilder::standalone(session_factory, target_partitions)
                .snapshot_registry(snapshot_registry)
                .status_retention(
                    cfg.query.query_status_max_entries,
                    Duration::from_secs(cfg.query.query_status_ttl_secs),
                )
                .build();
            Ok(Arc::new(engine))
        }
        ProcessRole::DataNode => {
            info!(
                scheduler = %cfg.cluster.scheduler_addr,
                local_fallback = cfg.cluster.local_query_fallback,
                "building remote Ballista query engine"
            );
            let mut builder =
                teodb_distributed::BallistaQueryEngineBuilder::remote(session_factory, &cfg.cluster.scheduler_addr)
                    .map_err(|error| eyre::eyre!("failed to configure remote Ballista scheduler: {error}"))?
                    .snapshot_registry(snapshot_registry)
                    .local_fallback(cfg.cluster.local_query_fallback)
                    .status_retention(
                        cfg.query.query_status_max_entries,
                        Duration::from_secs(cfg.query.query_status_ttl_secs),
                    );
            if let Some(observer) = event_observer {
                builder = builder.event_observer(observer);
            }
            Ok(Arc::new(builder.build()))
        }
        ProcessRole::ControlPlane => Err(eyre::eyre!(
            "control-plane role does not configure a public query engine"
        )),
    }
}
