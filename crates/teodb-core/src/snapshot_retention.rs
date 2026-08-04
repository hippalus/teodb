//! Retention policy for snapshot expiration.
//!
//! TeoDB does not yet remove snapshots from Iceberg metadata, so expiration
//! is enforced at the file-protection level: the orphan sweeper protects only
//! files referenced by *retained* snapshots, letting files exclusive to
//! expired snapshots be reclaimed. This restores the space reclamation for
//! compacted-away files that full-history protection deliberately gave up.

use std::collections::HashSet;
use std::time::Duration;

use crate::ident::SnapshotId;

/// Policy deciding which snapshots in a table's history are retained.
///
/// A snapshot is retained when **any** of the following holds:
/// - it is the table's current snapshot,
/// - it is younger than `max_age`,
/// - it is among the `keep_last` most recent snapshots,
/// - it is explicitly protected (e.g. pinned by a running query).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotRetention {
    /// Snapshots older than this are eligible for expiration.
    pub max_age: Duration,
    /// Always retain at least this many of the most recent snapshots,
    /// regardless of age. Clamped to a minimum of 1.
    pub keep_last: usize,
}

/// Select the snapshots that are **expired** under `retention`.
///
/// `snapshots` are `(snapshot_id, timestamp_ms)` pairs in any order;
/// `protected` are snapshot ids that must never expire (query pins).
pub fn select_expired_snapshots(
    snapshots: &[(SnapshotId, i64)],
    current_snapshot_id: Option<SnapshotId>,
    retention: &SnapshotRetention,
    protected: &HashSet<SnapshotId>,
    now_ms: i64,
) -> HashSet<SnapshotId> {
    let keep_last = retention.keep_last.max(1);
    let cutoff_ms = now_ms.saturating_sub(
        retention
            .max_age
            .as_millis()
            .min(i64::MAX as u128) as i64,
    );

    let mut by_recency: Vec<(SnapshotId, i64)> = snapshots.to_vec();
    by_recency.sort_by_key(|&(_, ts)| std::cmp::Reverse(ts));

    by_recency
        .iter()
        .skip(keep_last)
        .filter(|(id, ts)| *ts < cutoff_ms && Some(*id) != current_snapshot_id && !protected.contains(id))
        .map(|(id, _)| *id)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTE_MS: i64 = 60_000;

    fn retention(max_age_mins: u64, keep_last: usize) -> SnapshotRetention {
        SnapshotRetention {
            max_age: Duration::from_secs(max_age_mins * 60),
            keep_last,
        }
    }

    /// Snapshots at t = 0, 10, 20, ... minutes; `now` is the newest + 0.
    fn history(n: usize) -> Vec<(SnapshotId, i64)> {
        (0..n)
            .map(|i| (i as SnapshotId + 1, i as i64 * 10 * MINUTE_MS))
            .collect()
    }

    #[test]
    fn snapshots_younger_than_max_age_are_retained() {
        let snaps = history(4); // ids 1..4 at 0, 10, 20, 30 min
        let now = 30 * MINUTE_MS;
        let expired = select_expired_snapshots(&snaps, Some(4), &retention(15, 1), &HashSet::new(), now);
        // Only ids 1 (age 30m) and 2 (age 20m) exceed max_age 15m.
        assert_eq!(expired, HashSet::from([1, 2]));
    }

    #[test]
    fn keep_last_overrides_age() {
        let snaps = history(4);
        let now = 1_000 * MINUTE_MS; // everything far older than max_age
        let expired = select_expired_snapshots(&snaps, Some(4), &retention(15, 3), &HashSet::new(), now);
        // The 3 most recent (ids 2, 3, 4) survive on keep_last alone.
        assert_eq!(expired, HashSet::from([1]));
    }

    #[test]
    fn current_snapshot_never_expires() {
        let snaps = history(3);
        let now = 1_000 * MINUTE_MS;
        // Current is the *oldest* snapshot (e.g. rollback) — still retained.
        let expired = select_expired_snapshots(&snaps, Some(1), &retention(15, 1), &HashSet::new(), now);
        assert_eq!(expired, HashSet::from([2]));
    }

    #[test]
    fn protected_snapshots_never_expire() {
        let snaps = history(4);
        let now = 1_000 * MINUTE_MS;
        let pinned = HashSet::from([1, 2]);
        let expired = select_expired_snapshots(&snaps, Some(4), &retention(15, 1), &pinned, now);
        assert_eq!(expired, HashSet::from([3]));
    }

    #[test]
    fn keep_last_zero_is_clamped_to_one() {
        let snaps = history(2);
        let now = 1_000 * MINUTE_MS;
        let expired = select_expired_snapshots(&snaps, None, &retention(15, 0), &HashSet::new(), now);
        // Even with keep_last = 0 the newest snapshot survives.
        assert_eq!(expired, HashSet::from([1]));
    }

    #[test]
    fn empty_history_expires_nothing() {
        let expired = select_expired_snapshots(&[], None, &retention(15, 1), &HashSet::new(), 0);
        assert!(expired.is_empty());
    }
}
