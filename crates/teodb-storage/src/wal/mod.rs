//! Durable write-ahead logging.

mod gc;
mod identity;
mod manager;
mod prepared;
mod replay;
mod segment;
mod state;
mod writer;

pub use identity::WalIdentityConfig;
pub use manager::{WalConfig, WalManager};
pub use prepared::PreparedFlush;
pub use replay::{ReplayPlan, WalRecoveryMode};
pub use segment::{FrameDecode, WalHeader, WalOp, WalRecord, decode_frame, encode_frame};

#[cfg(test)]
mod tests;
