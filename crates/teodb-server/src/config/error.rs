use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum ConfigError {
    #[error("failed to serialize compiled configuration defaults")]
    SerializeDefaults(#[source] toml::ser::Error),

    #[error("config file not found: {}", path.display())]
    FileNotFound { path: PathBuf },

    #[error("failed to build layered configuration")]
    Build(#[source] config::ConfigError),

    #[error("failed to deserialize layered configuration")]
    Deserialize(#[source] config::ConfigError),

    #[error(transparent)]
    Validation(#[from] ConfigValidationError),
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("configuration validation failed:\n  - {}", .issues.join("\n  - "))]
pub(crate) struct ConfigValidationError {
    pub(crate) issues: Vec<String>,
}

impl ConfigValidationError {
    pub(crate) fn new(issues: Vec<String>) -> Self {
        Self { issues }
    }
}
