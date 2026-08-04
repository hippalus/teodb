//! Layered server configuration.

mod cli;
mod configuration;
mod error;
mod sections;

pub use cli::{CliArgs, ProcessRole};
pub use configuration::TeoDBConfig;
pub use sections::{
    CatalogConfig, LogFormat, MaintenanceConfig, ObservabilityConfig, SecurityConfig, SecurityMode, StorageConfig,
};
