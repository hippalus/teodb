//! Role lifecycle state machine.
//!
//! Every TeoDB node tracks a [`RoleState`] that transitions through:
//!
//! ```text
//! Starting → Ready → Draining → Stopped
//!     ↓         ↓
//!   Failed    Failed
//! ```
//!
//! Health and readiness probes expose this state. The transition to `Ready`
//! requires all role-specific prerequisites (catalog reachable, scheduler
//! registered, object-store valid, etc.) to pass.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use serde::Serialize;

/// Lifecycle state of a node role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum RoleState {
    /// Node is initializing; not yet ready to serve traffic.
    Starting = 0,
    /// All prerequisites satisfied; serving traffic.
    Ready = 1,
    /// Graceful shutdown in progress; rejecting new work, draining existing.
    Draining = 2,
    /// Clean shutdown complete.
    Stopped = 3,
    /// Unrecoverable error; node should be replaced.
    Failed = 4,
}

impl RoleState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Starting,
            1 => Self::Ready,
            2 => Self::Draining,
            3 => Self::Stopped,
            4 => Self::Failed,
            _ => Self::Failed,
        }
    }

    /// Whether the node is accepting new work.
    pub fn is_serving(&self) -> bool {
        matches!(self, Self::Ready)
    }

    /// Whether the node is in a terminal state (stopped or failed).
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }
}

impl fmt::Display for RoleState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Starting => f.write_str("starting"),
            Self::Ready => f.write_str("ready"),
            Self::Draining => f.write_str("draining"),
            Self::Stopped => f.write_str("stopped"),
            Self::Failed => f.write_str("failed"),
        }
    }
}

/// Thread-safe lifecycle state tracker.
///
/// Shared across subsystems so readiness probes, shutdown hooks, and
/// health endpoints all see the same state.
#[derive(Clone)]
pub struct RoleLifecycle {
    state: Arc<AtomicU8>,
    /// Optional reason when in `Failed` state.
    failure_reason: Arc<parking_lot::RwLock<Option<String>>>,
}

impl RoleLifecycle {
    /// Create a new lifecycle tracker in `Starting` state.
    pub fn new() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(RoleState::Starting as u8)),
            failure_reason: Arc::new(parking_lot::RwLock::new(None)),
        }
    }

    /// Current state.
    pub fn state(&self) -> RoleState {
        RoleState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Transition to `Ready`. Only valid from `Starting`.
    pub fn mark_ready(&self) -> bool {
        self.state
            .compare_exchange(
                RoleState::Starting as u8,
                RoleState::Ready as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Transition to `Draining`. Valid from `Starting` or `Ready`.
    pub fn mark_draining(&self) -> bool {
        let current = self.state.load(Ordering::Acquire);
        if current == RoleState::Starting as u8 || current == RoleState::Ready as u8 {
            self.state
                .compare_exchange(current, RoleState::Draining as u8, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        } else {
            false
        }
    }

    /// Transition to `Stopped`. Only valid from `Draining`.
    pub fn mark_stopped(&self) -> bool {
        self.state
            .compare_exchange(
                RoleState::Draining as u8,
                RoleState::Stopped as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Transition to `Failed` from any non-terminal state.
    pub fn mark_failed(&self, reason: String) -> bool {
        let current = self.state.load(Ordering::Acquire);
        if RoleState::from_u8(current).is_terminal() {
            return false;
        }
        let ok = self
            .state
            .compare_exchange(current, RoleState::Failed as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if ok {
            *self.failure_reason.write() = Some(reason);
        }
        ok
    }

    /// Get the failure reason, if any.
    pub fn failure_reason(&self) -> Option<String> {
        self.failure_reason.read().clone()
    }
}

impl Default for RoleLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_happy_path() {
        let lc = RoleLifecycle::new();
        assert_eq!(lc.state(), RoleState::Starting);
        assert!(!lc.state().is_serving());

        assert!(lc.mark_ready());
        assert_eq!(lc.state(), RoleState::Ready);
        assert!(lc.state().is_serving());

        assert!(lc.mark_draining());
        assert_eq!(lc.state(), RoleState::Draining);
        assert!(!lc.state().is_serving());

        assert!(lc.mark_stopped());
        assert_eq!(lc.state(), RoleState::Stopped);
        assert!(lc.state().is_terminal());
    }

    #[test]
    fn lifecycle_fail_from_starting() {
        let lc = RoleLifecycle::new();
        assert!(lc.mark_failed("bad config".into()));
        assert_eq!(lc.state(), RoleState::Failed);
        assert!(lc.state().is_terminal());
        assert_eq!(lc.failure_reason().unwrap(), "bad config");
    }

    #[test]
    fn lifecycle_fail_from_ready() {
        let lc = RoleLifecycle::new();
        lc.mark_ready();
        assert!(lc.mark_failed("oom".into()));
        assert_eq!(lc.state(), RoleState::Failed);
    }

    #[test]
    fn cannot_transition_from_terminal() {
        let lc = RoleLifecycle::new();
        lc.mark_ready();
        lc.mark_draining();
        lc.mark_stopped();
        assert!(!lc.mark_ready());
        assert!(!lc.mark_failed("nope".into()));
        assert_eq!(lc.state(), RoleState::Stopped);
    }

    #[test]
    fn cannot_ready_after_draining() {
        let lc = RoleLifecycle::new();
        lc.mark_ready();
        lc.mark_draining();
        assert!(!lc.mark_ready());
        assert_eq!(lc.state(), RoleState::Draining);
    }

    #[test]
    fn display_formatting() {
        assert_eq!(RoleState::Starting.to_string(), "starting");
        assert_eq!(RoleState::Ready.to_string(), "ready");
        assert_eq!(RoleState::Draining.to_string(), "draining");
        assert_eq!(RoleState::Stopped.to_string(), "stopped");
        assert_eq!(RoleState::Failed.to_string(), "failed");
    }

    #[test]
    fn serde_serialization() {
        let json = serde_json::to_string(&RoleState::Ready).unwrap();
        assert_eq!(json, "\"ready\"");
    }

    #[test]
    fn clone_shares_state() {
        let lc = RoleLifecycle::new();
        let lc2 = lc.clone();
        lc.mark_ready();
        assert_eq!(lc2.state(), RoleState::Ready);
    }
}
