//! Background storage maintenance.

mod compaction;
mod coordinator;
mod sweep;

pub(crate) use coordinator::{Maintenance, MaintenanceContext};
