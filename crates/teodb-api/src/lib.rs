//! TeoDB transport and API application boundary.
//!
//! This crate owns REST, Arrow Flight/FlightSQL, authentication, shared
//! authorization, and API-facing DDL orchestration. `teodb-server` remains the
//! sole composition root and listener owner.

pub mod admission;
pub mod authorization;
pub mod config;
mod ddl_effects;
pub mod flight;
pub mod http;
pub mod observer;
pub mod security;
pub mod service;

pub use authorization::ApiAuthorization;
pub use config::ApiConfig;
pub use http::{AppLifecycle, AppReadiness, AppSecurity, AppServices, AppState, ReadinessProbe};
pub use observer::{ApiObserver, ApiTransport, NoopApiObserver};
pub use service::{DdlService, SqlRouting};
