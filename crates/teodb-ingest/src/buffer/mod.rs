//! In-memory write buffering and table-buffer registry.

mod registry;
mod table;

pub use registry::{BufferRegistry, EvictionStats};
pub use table::{BlockedFlush, BufferEntry, BufferSnapshot, BufferStats, InsertOk, TableBuffer};

#[cfg(test)]
mod tests;
