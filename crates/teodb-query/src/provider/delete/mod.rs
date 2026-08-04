//! Position-delete loading and execution.

mod execution;
mod reader;

pub use execution::{DeletePositions, PositionDeleteFilterExec};
pub(super) use reader::PositionDeleteSet;
