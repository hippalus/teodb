use std::fmt;

use thiserror::Error;

pub(crate) type StartupResult<T> = Result<T, StartupError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StartupStage {
    SecurityValidation,
    Catalog,
    Wal,
    WalReplay,
    Storage,
    Query,
    Cluster,
    AppState,
    Maintenance,
    CatalogReadiness,
    SpillDirectory,
    DataDirectory,
    Tls,
}

impl fmt::Display for StartupStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::SecurityValidation => "security validation",
            Self::Catalog => "catalog initialization",
            Self::Wal => "WAL initialization",
            Self::WalReplay => "WAL replay",
            Self::Storage => "storage initialization",
            Self::Query => "query initialization",
            Self::Cluster => "cluster initialization",
            Self::AppState => "application state initialization",
            Self::Maintenance => "maintenance initialization",
            Self::CatalogReadiness => "catalog readiness",
            Self::SpillDirectory => "spill directory readiness",
            Self::DataDirectory => "data directory readiness",
            Self::Tls => "TLS initialization",
        };
        formatter.write_str(name)
    }
}

#[derive(Debug, Error)]
#[error("{stage} failed: {message}")]
pub(crate) struct StartupError {
    pub(super) stage: StartupStage,
    message: String,
}

impl StartupError {
    pub(super) fn at(stage: StartupStage, error: impl fmt::Display) -> Self {
        Self {
            stage,
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_error_preserves_matchable_stage() {
        let error = StartupError::at(StartupStage::Catalog, "connection refused");
        assert_eq!(error.stage, StartupStage::Catalog);
        assert!(
            error
                .to_string()
                .contains("catalog initialization")
        );
        assert!(error.to_string().contains("connection refused"));
    }
}
