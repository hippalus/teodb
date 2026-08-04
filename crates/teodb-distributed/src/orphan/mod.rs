//! Orphan-file retention and cleanup.

mod sweeper;

pub use sweeper::{OrphanSweeper, SweepReport};

#[cfg(test)]
mod tests;
