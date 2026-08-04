//! Ballista cluster task construction for data-node and control-plane roles.

use std::sync::Arc;

use tracing::error;

use super::roles;
use super::shutdown::ShutdownCoordinator;
use crate::config::{ProcessRole, TeoDBConfig};

pub(super) struct ClusterTasks {
    pub(super) scheduler: Option<tokio::task::JoinHandle<()>>,
    pub(super) executor: Option<tokio::task::JoinHandle<()>>,
}

pub(super) fn start_data_node_tasks(
    config: &TeoDBConfig,
    catalog: Arc<dyn teodb_core::traits::catalog::Catalog>,
    object_store: teodb_query::ObjectStoreRegistration,
    shutdown: &Arc<ShutdownCoordinator>,
) -> eyre::Result<ClusterTasks> {
    let distributed = config.role == ProcessRole::DataNode;

    let scheduler = if distributed && config.cluster.scheduler_enabled {
        let scheduler_config = roles::scheduler_config_from(config)?;
        let scheduler_shutdown = shutdown.subscribe();
        Some(tokio::spawn(async move {
            if let Err(error) =
                teodb_distributed::ballista::start_scheduler(scheduler_config, catalog, scheduler_shutdown).await
            {
                error!(%error, "Ballista scheduler failed");
            }
        }))
    } else {
        None
    };

    let executor = if distributed {
        let executor_config = roles::executor_config_from(config, object_store)?;
        let executor_shutdown = shutdown.subscribe();
        Some(tokio::spawn(async move {
            if let Err(error) = teodb_distributed::ballista::start_executor(executor_config, executor_shutdown).await {
                error!(%error, "Ballista executor failed");
            }
        }))
    } else {
        None
    };

    Ok(ClusterTasks { scheduler, executor })
}

pub(super) fn start_scheduler(
    config: &TeoDBConfig,
    catalog: Arc<dyn teodb_core::traits::catalog::Catalog>,
    shutdown: &Arc<ShutdownCoordinator>,
) -> eyre::Result<tokio::task::JoinHandle<()>> {
    let scheduler_config = roles::scheduler_config_from(config)?;
    let scheduler_shutdown = shutdown.subscribe();
    Ok(tokio::spawn(async move {
        if let Err(error) =
            teodb_distributed::ballista::start_scheduler(scheduler_config, catalog, scheduler_shutdown).await
        {
            error!(%error, "Ballista scheduler failed");
        }
    }))
}
