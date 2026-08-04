//! Ballista distributed execution integration.

mod address;
mod config;
mod executor;
mod scheduler;

pub use address::HostPort;
pub use config::{ExecutorConfig, SchedulerConfig};
pub use executor::start_executor;
pub use scheduler::start_scheduler;

#[cfg(test)]
use executor::{build_runtime_producer, parse_object_store_url, wait_then_abort};
#[cfg(test)]
use scheduler::drain_scheduler_jobs;

#[cfg(test)]
mod tests;
