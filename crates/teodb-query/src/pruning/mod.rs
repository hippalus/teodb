//! Partition pruning — drops files whose typed partition values are
//! incompatible with query filter predicates.
//!
//! This implements the conservative pruning logic described in §11.4 of the
//! plan. A file is kept whenever its partition values cannot be proved
//! incompatible with the predicate (Invariant I4).

pub mod partition;
pub mod statistics;

pub use partition::partition_prune;
pub use statistics::statistics_prune;
