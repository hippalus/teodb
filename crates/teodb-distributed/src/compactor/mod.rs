//! Compaction planning and execution.

mod execution;
mod runner;

pub use runner::{CompactionOutcome, CompactionPlan, Compactor, CompactorBuilder};

#[cfg(test)]
mod tests;
