//! Server assembly and runtime components.

mod application;
mod bootstrap;
mod cluster;
mod collectors;
mod flight;
mod flight_admission;
mod http;
mod incoming;
#[cfg(test)]
mod metrics_e2e;
mod roles;
mod shutdown;
mod startup_error;
mod tls;
mod transport;
mod ui;
mod validate;

pub(crate) use application::run;
