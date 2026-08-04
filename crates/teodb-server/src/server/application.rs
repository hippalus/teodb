//! Server application orchestration.

use std::sync::Arc;
use std::time::Duration;

use tracing::{error, info, warn};

use super::shutdown::ShutdownCoordinator;
use super::startup_error::{StartupError, StartupResult, StartupStage};
use super::{bootstrap, cluster, collectors, flight, http, tls, transport, validate};
use crate::builder::{S3Settings, StorageComponents, build_catalog};
use crate::config::{ProcessRole, TeoDBConfig};
use crate::metrics::Metrics;

/// Main server entry point — called from the runtime built in main().
pub(crate) async fn run(cfg: TeoDBConfig) -> StartupResult<()> {
    info!(
        role = %cfg.role,
        rest_bind = %cfg.server.rest_bind,
        flight_bind = %cfg.server.flight_bind,
        "TeoDB starting"
    );

    validate::validate_production_mode(&cfg)
        .map_err(|error| StartupError::at(StartupStage::SecurityValidation, error))?;

    let metrics = Arc::new(Metrics::new());
    let shutdown = Arc::new(ShutdownCoordinator::new(Duration::from_secs(
        cfg.shutdown.drain_timeout_secs,
    )));

    let s3_settings = S3Settings::from(&cfg.storage);
    let catalog_observer: Arc<dyn teodb_catalog::CatalogObserver> = Arc::new(collectors::MetricsCatalogObserver {
        metrics: metrics.clone(),
    });
    let catalog = build_catalog(
        &cfg.catalog,
        &s3_settings,
        cfg.cluster.max_writer_checkpoints_per_table,
        Some(catalog_observer),
    )
    .await
    .map_err(|error| StartupError::at(StartupStage::Catalog, error))?;

    match cfg.role {
        ProcessRole::Standalone | ProcessRole::DataNode => {
            let wal = bootstrap::open_wal(&cfg)
                .await
                .map_err(|error| StartupError::at(StartupStage::Wal, error))?;
            let storage = StorageComponents::build(&cfg, &s3_settings)
                .map_err(|error| StartupError::at(StartupStage::Storage, error))?;
            run_data_node(
                &cfg,
                DataNodeServices {
                    catalog,
                    storage,
                    wal,
                    metrics,
                    shutdown,
                },
            )
            .await?;
        }
        ProcessRole::ControlPlane => {
            run_control_plane(&cfg, catalog, shutdown).await?;
        }
    }

    info!("TeoDB stopped");
    Ok(())
}

/// Data node: public REST/Flight, WAL-backed ingest, query entry point,
/// flush, and maintenance.
///
/// In the `data-node` role the process additionally runs a Ballista executor and,
/// when `cluster.scheduler_enabled` is true, hosts the cluster's active
/// scheduler. In the `standalone` role the query engine embeds Ballista
/// in-process and no cluster services are spawned.
struct DataNodeServices {
    catalog: Arc<dyn teodb_core::traits::catalog::Catalog>,
    storage: StorageComponents,
    wal: Arc<teodb_storage::wal::WalManager>,
    metrics: Arc<Metrics>,
    shutdown: Arc<ShutdownCoordinator>,
}

async fn run_data_node(cfg: &TeoDBConfig, dependencies: DataNodeServices) -> StartupResult<()> {
    let DataNodeServices {
        catalog,
        storage,
        wal,
        metrics,
        shutdown,
    } = dependencies;
    let distributed = cfg.role == ProcessRole::DataNode;
    let resolved_identity = wal.writer_identity();
    let node_id = resolved_identity.node_id.to_string();

    let ingest = bootstrap::build_ingest_components(cfg, wal.clone());
    let ingest_config = ingest.config;
    let buffers = ingest.buffers;
    let idempotency = ingest.idempotency;

    // Recovery must be able to commit a replayed prefix before bounded buffer
    // admission can make progress on the next durable record.
    let flush_observer: Arc<dyn teodb_ingest::flush::FlushObserver> = Arc::new(collectors::MetricsFlushObserver {
        metrics: metrics.clone(),
    });
    let flusher =
        teodb_ingest::flush::Flusher::new(buffers.clone(), catalog.clone(), storage.factory.clone(), wal.clone())
            .with_status_check_config(ingest_config.commit_status_check.clone())
            .with_observer(flush_observer);
    let replayer =
        teodb_ingest::replay::Replayer::new(wal.clone(), buffers.clone(), catalog.clone(), idempotency.clone())
            .with_recovery_flusher(flusher.clone());
    replay_wal(&replayer, &metrics).await?;

    // Keep a reference to WAL for shutdown.
    let wal_for_shutdown = wal.clone();

    // One pin registry shared by the query engine (pins on prepare) and the
    // maintenance loop (respects pins during snapshot expiration). In the
    // data-node role pins must be visible across processes, so they are stored
    // in the shared catalog; standalone keeps an in-process registry.
    let snapshot_registry: Arc<dyn teodb_core::snapshot_pin::ActiveSnapshotRegistry> = if distributed {
        Arc::new(teodb_distributed::snapshot_registry::CatalogSnapshotRegistry::new(
            catalog.clone(),
            node_id.clone(),
            teodb_distributed::snapshot_registry::SnapshotRegistryConfig {
                lease_ttl: Duration::from_secs(cfg.maintenance.lock_ttl_secs),
                ..Default::default()
            },
        ))
    } else {
        Arc::new(teodb_core::snapshot_pin::InMemorySnapshotRegistry::new())
    };

    let engine_observer: Arc<dyn teodb_distributed::EngineEventObserver> =
        Arc::new(collectors::MetricsEngineEventObserver {
            metrics: metrics.clone(),
        });
    let query_engine = bootstrap::build_query_engine(
        cfg,
        &catalog,
        &storage,
        snapshot_registry.clone(),
        Some(engine_observer),
    )
    .map_err(|error| StartupError::at(StartupStage::Query, error))?;

    let cluster_tasks = cluster::start_data_node_tasks(
        cfg,
        catalog.clone(),
        storage.object_store_registration().clone(),
        &shutdown,
    )
    .map_err(|error| StartupError::at(StartupStage::Cluster, error))?;

    let lifecycle = teodb_core::lifecycle::RoleLifecycle::new();
    let api_observer: Arc<dyn teodb_api::ApiObserver> = Arc::new(collectors::MetricsApiObserver {
        metrics: metrics.clone(),
    });

    let app_state = bootstrap::build_app_state(
        cfg,
        bootstrap::AppStateDependencies {
            catalog: catalog.clone(),
            buffers: buffers.clone(),
            wal,
            idempotency,
            query_engine,
            ingest_config: ingest_config.clone(),
            storage_factory: storage.factory.clone(),
            flusher: flusher.clone(),
            lifecycle: lifecycle.clone(),
            draining: shutdown.drain_flag(),
            api_observer,
        },
    )
    .map_err(|error| StartupError::at(StartupStage::AppState, error))?;

    let rest_router = http::build_http_router(&app_state, &metrics, storage.cache_index.clone(), cfg);
    let flight_server = flight::build_flight_server(&app_state, cfg);

    let flush_handle = spawn_flush_task(
        teodb_ingest::flush::FlushLoopConfig {
            flusher,
            interval: Duration::from_secs(cfg.ingest.flush_interval_secs),
        },
        &shutdown,
    );

    // Readiness checks: validate dependencies before accepting traffic.
    // Done before spawning maintenance to avoid borrowing catalog after move.
    if let Err(e) = check_readiness(&catalog, cfg).await {
        lifecycle.mark_failed(e.to_string());
        return Err(e);
    }

    let maintenance_handle = spawn_maintenance_task(
        crate::maintenance::MaintenanceContext {
            cfg: cfg.maintenance.clone(),
            node_id,
            catalog,
            storage: storage.factory,
            cache_index: storage.cache_index,
            metrics: metrics.clone(),
            snapshot_registry,
            object_store: storage.object_store_registration.clone(),
            spill_dir: cfg.storage.spill_dir.clone(),
        },
        &shutdown,
    )
    .map_err(|error| StartupError::at(StartupStage::Maintenance, error))?;

    let tls_bundle = tls::load_tls_bundle(&cfg.security).map_err(|error| StartupError::at(StartupStage::Tls, error))?;

    if tls_bundle.is_some() {
        info!("TLS configured for REST and Flight gRPC");
    } else if cfg.security.mode.requires_tls() {
        warn!("TLS not configured but mode={} requires it", cfg.security.mode);
    }

    if distributed {
        info!(
            rest = %cfg.server.rest_bind,
            flight = %cfg.server.flight_bind,
            scheduler_enabled = cfg.cluster.scheduler_enabled,
            scheduler = %cfg.cluster.scheduler_addr,
            "Data node ready"
        );
    } else {
        info!(rest = %cfg.server.rest_bind, flight = %cfg.server.flight_bind, "Standalone ready");
    }
    lifecycle.mark_ready();

    let tls = tls_bundle.map(std::sync::Arc::new);

    let rest_handle = transport::spawn_rest_server(
        transport::RestTransportConfig {
            addr: cfg.server.rest_bind.clone(),
            tls_bundle: tls.clone(),
            max_connections: cfg.server.max_http_connections,
            idle_timeout: Duration::from_secs(cfg.server.idle_timeout_secs),
        },
        rest_router,
        metrics.clone(),
        shutdown.subscribe(),
    );
    let flight_handle = transport::spawn_flight_server(
        transport::FlightTransportConfig {
            addr: cfg.server.flight_bind.clone(),
            tls_bundle: tls,
            max_connections: cfg.server.max_flight_connections,
            max_in_flight_requests: cfg.server.max_flight_in_flight_requests,
            max_streams_per_connection: cfg.server.max_flight_streams_per_connection,
            idle_timeout: Duration::from_secs(cfg.server.idle_timeout_secs),
        },
        flight_server,
        app_state.security.authorization.clone(),
        metrics,
        shutdown.subscribe(),
    );

    shutdown.wait_for_signal().await;
    lifecycle.mark_draining();
    info!(is_draining = shutdown.is_draining(), "shutdown signal received");

    let tasks = DataNodeTasks {
        rest: rest_handle,
        flight: flight_handle,
        flush: flush_handle,
        maintenance: maintenance_handle,
        cluster: cluster_tasks,
    };
    let drain_ok = drain_data_node(&shutdown, tasks).await;

    if !drain_ok {
        lifecycle.mark_failed("drain timeout exceeded".into());
        error!("drain timeout exceeded, forcing exit");
        std::process::exit(137);
    }

    lifecycle.mark_stopped();

    // Release the WAL lease so the next instance can start cleanly.
    wal_for_shutdown.release_lease().await;

    crate::startup::shutdown_tracing();

    Ok(())
}

struct DataNodeTasks {
    rest: tokio::task::JoinHandle<()>,
    flight: tokio::task::JoinHandle<()>,
    flush: tokio::task::JoinHandle<()>,
    maintenance: tokio::task::JoinHandle<()>,
    cluster: cluster::ClusterTasks,
}

async fn replay_wal(replayer: &teodb_ingest::replay::Replayer, metrics: &Arc<Metrics>) -> StartupResult<()> {
    let observer = collectors::MetricsReplayObserver {
        metrics: metrics.clone(),
    };
    replayer
        .replay_wal(Some(&observer))
        .await
        .map_err(|error| StartupError::at(StartupStage::WalReplay, error))
}

fn spawn_flush_task(
    config: teodb_ingest::flush::FlushLoopConfig,
    shutdown: &Arc<ShutdownCoordinator>,
) -> tokio::task::JoinHandle<()> {
    let shutdown_rx = shutdown.subscribe();
    tokio::spawn(teodb_ingest::flush::flush_loop(config, shutdown_rx))
}

fn spawn_maintenance_task(
    context: crate::maintenance::MaintenanceContext,
    shutdown: &Arc<ShutdownCoordinator>,
) -> teodb_core::TeoDBResult<tokio::task::JoinHandle<()>> {
    // Validate runtime dependencies (e.g. compactor object stores) before
    // spawning so a misconfigured maintenance loop fails fast at startup.
    let maintenance = crate::maintenance::Maintenance::new(context)?;
    Ok(tokio::spawn(maintenance.run(shutdown.subscribe())))
}

async fn drain_data_node(shutdown: &ShutdownCoordinator, tasks: DataNodeTasks) -> bool {
    shutdown
        .drain_with_timeout(|| async {
            // Shutdown order: stop accepting new requests → flush buffered
            // data → stop maintenance → stop cluster services.
            info!("drain phase 1: stopping REST and Flight listeners");
            await_task("REST server", tasks.rest).await;
            await_task("Flight server", tasks.flight).await;

            info!("drain phase 2: flushing remaining buffers");
            await_task("flush", tasks.flush).await;

            info!("drain phase 3: stopping maintenance");
            await_task("maintenance", tasks.maintenance).await;

            if let Some(executor) = tasks.cluster.executor {
                info!("drain phase 4: stopping Ballista executor");
                await_task("executor", executor).await;
            }

            if let Some(scheduler) = tasks.cluster.scheduler {
                info!("drain phase 5: stopping Ballista scheduler");
                await_task("scheduler", scheduler).await;
            }
        })
        .await
}

async fn await_task(name: &'static str, handle: tokio::task::JoinHandle<()>) {
    if let Err(error) = handle.await {
        error!(%error, task = name, "task panicked during drain");
    }
}

/// Active control-plane service for V1 homogeneous clusters.
async fn run_control_plane(
    cfg: &TeoDBConfig,
    catalog: Arc<dyn teodb_core::traits::catalog::Catalog>,
    shutdown: Arc<ShutdownCoordinator>,
) -> StartupResult<()> {
    let scheduler_handle = cluster::start_scheduler(cfg, catalog, &shutdown)
        .map_err(|error| StartupError::at(StartupStage::Cluster, error))?;

    info!(
        bind = %cfg.cluster.scheduler_bind,
        advertised = %cfg.cluster.scheduler_addr,
        "Control plane ready"
    );

    shutdown.wait_for_signal().await;
    info!(is_draining = shutdown.is_draining(), "shutdown signal received");

    let drain_ok = shutdown
        .drain_with_timeout(|| async {
            info!("drain phase 1: stopping Ballista scheduler");
            if let Err(e) = scheduler_handle.await {
                error!(error = %e, "scheduler task panicked during drain");
            }
        })
        .await;

    if !drain_ok {
        error!("drain timeout exceeded, forcing exit");
        std::process::exit(137);
    }

    crate::startup::shutdown_tracing();
    Ok(())
}

/// Pre-flight readiness checks: validate that critical dependencies are
/// reachable before transitioning to `Ready` state.
async fn check_readiness(
    catalog: &Arc<dyn teodb_core::traits::catalog::Catalog>,
    cfg: &TeoDBConfig,
) -> StartupResult<()> {
    // 1. Catalog connectivity: attempt to list namespaces.
    match catalog.list_namespaces().await {
        Ok(ns) => info!(namespace_count = ns.len(), "readiness: catalog reachable"),
        Err(e) => {
            error!(error = %e, "readiness: catalog unreachable");
            return Err(StartupError::at(StartupStage::CatalogReadiness, e));
        }
    }

    // 2. Spill directory: ensure it exists and is writable.
    let spill_dir = &cfg.storage.spill_dir;
    if let Err(e) = std::fs::create_dir_all(spill_dir) {
        error!(path = %spill_dir.display(), error = %e, "readiness: spill dir not writable");
        return Err(StartupError::at(StartupStage::SpillDirectory, e));
    }

    // 3. Data directory: ensure it exists.
    if let Err(e) = std::fs::create_dir_all(&cfg.data_dir) {
        error!(path = %cfg.data_dir.display(), error = %e, "readiness: data dir not writable");
        return Err(StartupError::at(StartupStage::DataDirectory, e));
    }

    info!("readiness: all checks passed");
    Ok(())
}
