use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use tracing::{info, warn};

use ahash::AHashMap;
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::{Generation, TableIdent};
use teodb_core::write_protocol::WalTableKey;

use super::WalManager;
use super::segment::{self, ScanFrame, SegmentScan};

struct ScannedSegment {
    path: PathBuf,
    scan: SegmentScan,
    voided: Vec<bool>,
}

pub(super) struct WalGc<'a> {
    manager: &'a WalManager,
}

impl<'a> WalGc<'a> {
    pub(super) fn new(manager: &'a WalManager) -> Self {
        Self { manager }
    }

    pub(super) async fn collect_garbage(&self) -> TeoDBResult<u64> {
        let committed = self.manager.committed.lock().await.clone();
        let candidates = self.candidate_segments().await?;
        let mut segments = self.scan_readable_prefix(candidates).await;
        self.mark_voided_frames(&mut segments);
        let deleted = self
            .delete_dead_segments(&segments, &committed)
            .await;

        if deleted > 0 {
            info!(deleted, "WAL GC completed");
        }
        Ok(deleted)
    }

    async fn candidate_segments(&self) -> TeoDBResult<Vec<(u64, PathBuf)>> {
        let current_seq = match self.manager.current_seq.load(Ordering::Acquire) {
            0 => None,
            value => Some(value - 1),
        };
        let mut segments = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.manager.cfg.root_dir)
            .await
            .map_err(|error| TeoDBError::wal(format!("failed to read WAL dir: {error}")))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|error| TeoDBError::wal(format!("readdir: {error}")))?
        {
            let name = entry.file_name();
            if let Some(sequence) = WalManager::parse_seq(&name.to_string_lossy())
                && current_seq != Some(sequence)
            {
                segments.push((sequence, entry.path()));
            }
        }

        segments.sort_by_key(|(sequence, _)| *sequence);
        Ok(segments)
    }

    async fn scan_readable_prefix(&self, candidates: Vec<(u64, PathBuf)>) -> Vec<ScannedSegment> {
        let mut segments = Vec::with_capacity(candidates.len());
        for (_, path) in candidates {
            match segment::scan_segment(&path).await {
                Ok(scan) => {
                    let voided = vec![false; scan.frames.len()];
                    segments.push(ScannedSegment { path, scan, voided });
                }
                Err(error) => {
                    // An unreadable segment hides unknown tables. Decisions about
                    // later tombstones would be guesses, so only the safe prefix
                    // participates in this pass.
                    warn!(path = %path.display(), %error, "failed to scan WAL segment, stopping GC pass");
                    break;
                }
            }
        }
        segments
    }

    fn mark_voided_frames(&self, segments: &mut [ScannedSegment]) {
        let mut tombstoned = HashSet::new();
        for segment in segments.iter_mut().rev() {
            for (index, frame) in segment.scan.frames.iter().enumerate().rev() {
                match frame {
                    ScanFrame::Append { key, .. } => {
                        segment.voided[index] = tombstoned.contains(&key.ident);
                    }
                    ScanFrame::DropTable { table } => {
                        tombstoned.insert(table);
                    }
                }
            }
        }
    }

    async fn delete_dead_segments(
        &self,
        segments: &[ScannedSegment],
        committed: &AHashMap<WalTableKey, Generation>,
    ) -> u64 {
        let mut retained_tables = HashSet::new();
        let mut deleted = 0;

        for segment in segments {
            if segment.scan.corrupt {
                warn!(path = %segment.path.display(), "WAL GC: segment has a corrupt frame, refusing to delete");
            }

            if self.is_deletable(segment, committed, &retained_tables) {
                match tokio::fs::remove_file(&segment.path).await {
                    Ok(()) => {
                        deleted += 1;
                        continue;
                    }
                    Err(error) => {
                        warn!(path = %segment.path.display(), %error, "failed to delete WAL segment");
                    }
                }
            }

            self.retain_segment_tables(segment, &mut retained_tables);
        }

        deleted
    }

    fn is_deletable(
        &self,
        segment: &ScannedSegment,
        committed: &AHashMap<WalTableKey, Generation>,
        retained_tables: &HashSet<TableIdent>,
    ) -> bool {
        !segment.scan.corrupt
            && !segment.scan.frames.is_empty()
            && segment
                .scan
                .frames
                .iter()
                .zip(&segment.voided)
                .all(|(frame, &is_voided)| match frame {
                    ScanFrame::Append { key, generation } => {
                        is_voided
                            || committed
                                .get(key)
                                .is_some_and(|committed_generation| generation <= committed_generation)
                    }
                    ScanFrame::DropTable { table } => !retained_tables.contains(table),
                })
    }

    fn retain_segment_tables(&self, segment: &ScannedSegment, retained_tables: &mut HashSet<TableIdent>) {
        for frame in &segment.scan.frames {
            if let ScanFrame::Append { key, .. } = frame {
                retained_tables.insert(key.ident.clone());
            }
        }
    }
}
