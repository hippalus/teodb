//! CLI argument parsing and process role definitions.

use std::path::PathBuf;

use clap::Parser;
use serde::{Deserialize, Serialize};

/// CLI arguments — bootstrap only. Full config lives in the TOML file.
#[derive(Debug, Clone, Parser)]
#[command(name = "teodb", about = "TeoDB — Modern OLAP Database")]
pub struct CliArgs {
    /// Path to TOML configuration file.
    #[arg(short, long, env = "TEODB_CONFIG")]
    pub config: Option<PathBuf>,

    /// Override: process role.
    #[arg(long)]
    pub role: Option<ProcessRole>,

    /// Override: security mode (plaintext, tls, oauth2).
    #[arg(long)]
    pub security_mode: Option<super::sections::SecurityMode>,

    /// Override: REST API bind address.
    #[arg(long)]
    pub rest_bind: Option<String>,

    /// Override: Flight SQL bind address.
    #[arg(long)]
    pub flight_bind: Option<String>,

    /// Override: hostname this data node's Ballista executor advertises to the
    /// cluster (must be routable from the control plane and other data nodes).
    #[arg(long)]
    pub executor_advertise_host: Option<String>,

    /// Override: log level filter.
    #[arg(long)]
    pub log_level: Option<super::sections::LogLevel>,

    /// Override: log format (json | pretty | compact).
    #[arg(long)]
    pub log_format: Option<super::sections::LogFormat>,
}

/// The role this process plays in the cluster.
///
/// - **DataNode**: homogeneous production data-plane process. Serves REST/Flight, accepts
///   ingest/query/DDL, owns WAL/buffers/flush, and runs a Ballista executor.
/// - **ControlPlane**: active cluster control process. It currently hosts the
///   Ballista scheduler and is expected to own cluster coordination state.
/// - **Standalone**: All roles in a single process (dev/test).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum, Serialize, Deserialize)]
pub enum ProcessRole {
    #[serde(rename = "data-node")]
    #[value(name = "data-node")]
    DataNode,
    #[serde(rename = "control-plane")]
    #[value(name = "control-plane")]
    ControlPlane,
    #[serde(rename = "standalone")]
    #[value(name = "standalone")]
    #[default]
    Standalone,
}

impl std::fmt::Display for ProcessRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DataNode => f.write_str("data-node"),
            Self::ControlPlane => f.write_str("control-plane"),
            Self::Standalone => f.write_str("standalone"),
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::ValueEnum;
    use serde::Deserialize;

    use super::ProcessRole;

    #[derive(Deserialize)]
    struct RoleConfig {
        role: ProcessRole,
    }

    #[test]
    fn role_names_match_cli_and_config_surface() {
        assert_eq!(
            ProcessRole::from_str("data-node", false).unwrap(),
            ProcessRole::DataNode
        );
        assert_eq!(
            ProcessRole::from_str("control-plane", false).unwrap(),
            ProcessRole::ControlPlane
        );
        assert_eq!(
            ProcessRole::from_str("standalone", false).unwrap(),
            ProcessRole::Standalone
        );

        assert_eq!(
            toml::from_str::<RoleConfig>("role = \"data-node\"")
                .unwrap()
                .role,
            ProcessRole::DataNode
        );
        assert_eq!(
            toml::from_str::<RoleConfig>("role = \"control-plane\"")
                .unwrap()
                .role,
            ProcessRole::ControlPlane
        );
        assert_eq!(
            toml::from_str::<RoleConfig>("role = \"standalone\"")
                .unwrap()
                .role,
            ProcessRole::Standalone
        );

        assert!(ProcessRole::from_str("node", false).is_err());
        assert!(ProcessRole::from_str("scheduler", false).is_err());
        assert!(toml::from_str::<RoleConfig>("role = \"node\"").is_err());
        assert!(toml::from_str::<RoleConfig>("role = \"scheduler\"").is_err());

        assert_eq!(ProcessRole::DataNode.to_string(), "data-node");
        assert_eq!(ProcessRole::ControlPlane.to_string(), "control-plane");
        assert_eq!(ProcessRole::Standalone.to_string(), "standalone");
    }
}
