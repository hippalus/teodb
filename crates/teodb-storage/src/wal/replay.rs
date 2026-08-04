//! Bounded, fail-closed WAL replay.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::{Generation, TableIdent};
use teodb_core::write_protocol::WalTableKey;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::{error, warn};

use super::segment::{self, FrameDecode, WalOp, WalRecord};

/// How WAL replay responds to a structurally corrupt segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WalRecoveryMode {
    #[default]
    Fail,
    Salvage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct FramePosition {
    segment: usize,
    offset: u64,
}

/// Compact pass-one metadata for one live append. The table key is interned so
/// replay memory grows with small position/sort records rather than decoded
/// Arrow batches or repeated table-name strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplayEntry {
    position: FramePosition,
    generation: Generation,
    batch_id: uuid::Uuid,
    table_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SegmentIdentity {
    path: PathBuf,
    len: u64,
    modified: Option<SystemTime>,
}

#[derive(Debug, Clone)]
struct SegmentSnapshot {
    identity: SegmentIdentity,
    /// Complete, validated prefix. This excludes a torn tail and the corrupt
    /// suffix of a segment quarantined in salvage mode.
    replay_len: u64,
    live: bool,
}

/// A fully validated WAL snapshot that yields at most one decoded record per
/// call. Preparing the plan performs all frame/header/IPC/tombstone validation;
/// callers may mutate recovery state only after preparation succeeds.
pub struct ReplayPlan {
    root: PathBuf,
    segments: Vec<SegmentSnapshot>,
    entries: Vec<ReplayEntry>,
    tables: Vec<WalTableKey>,
    table_keys: AHashSet<WalTableKey>,
    entry_index: usize,
    file_segment: Option<usize>,
    file: Option<tokio::fs::File>,
    snapshot_checked: bool,
    live_decoded_records: usize,
    peak_live_decoded_records: usize,
}

impl ReplayPlan {
    pub fn table_keys(&self) -> impl Iterator<Item = &WalTableKey> {
        self.table_keys.iter()
    }

    pub fn record_count(&self) -> usize {
        self.entries.len()
    }

    /// Peak records simultaneously owned by the decoder/iterator layer.
    pub fn peak_live_decoded_records(&self) -> usize {
        self.peak_live_decoded_records
    }

    /// Decode the next live append in canonical `(generation, batch_id)`
    /// order. Exact key ties retain physical segment/frame order.
    pub async fn next_record(&mut self) -> TeoDBResult<Option<WalRecord>> {
        if !self.snapshot_checked {
            self.verify_live_snapshot().await?;
            self.snapshot_checked = true;
        }

        let Some(entry) = self.entries.get(self.entry_index).copied() else {
            return Ok(None);
        };
        let snapshot = self
            .segments
            .get(entry.position.segment)
            .ok_or_else(|| snapshot_mismatch(&self.root))?;

        if self.file_segment != Some(entry.position.segment) {
            verify_identity(&snapshot.identity).await?;
            self.file = Some(open_segment(&snapshot.identity.path).await?);
            self.file_segment = Some(entry.position.segment);
        }
        let file = self
            .file
            .as_mut()
            .expect("replay file opened above");
        file.seek(std::io::SeekFrom::Start(entry.position.offset))
            .await
            .map_err(|error| {
                TeoDBError::wal_source(format!("seek WAL segment {}", snapshot.identity.path.display()), error)
            })?;
        let decoded = read_frame(
            file,
            &snapshot.identity.path,
            entry.position.offset,
            snapshot.replay_len,
        )
        .await?;
        let record = match decoded {
            FrameDecode::Complete(record, _) => *record,
            FrameDecode::Incomplete | FrameDecode::Corrupt(_) => {
                return Err(snapshot_mismatch(&snapshot.identity.path));
            }
        };

        self.live_decoded_records += 1;
        self.peak_live_decoded_records = self
            .peak_live_decoded_records
            .max(self.live_decoded_records);

        let expected_table = self
            .tables
            .get(entry.table_index)
            .ok_or_else(|| snapshot_mismatch(&snapshot.identity.path))?;
        let matches_index = record.header.op == WalOp::Append
            && record.header.generation == entry.generation
            && record.header.batch_id == entry.batch_id
            && record.header.table_key()? == *expected_table;
        self.live_decoded_records -= 1;
        if !matches_index {
            return Err(snapshot_mismatch(&snapshot.identity.path));
        }

        self.entry_index += 1;
        Ok(Some(record))
    }

    async fn verify_live_snapshot(&self) -> TeoDBResult<()> {
        let actual_paths = segment_paths(&self.root).await?;
        let expected: Vec<&SegmentIdentity> = self
            .segments
            .iter()
            .filter(|segment| segment.live)
            .map(|segment| &segment.identity)
            .collect();
        if actual_paths.len() != expected.len()
            || actual_paths
                .iter()
                .zip(&expected)
                .any(|(actual, expected)| actual != &expected.path)
        {
            return Err(snapshot_mismatch(&self.root));
        }
        for identity in expected {
            verify_identity(identity).await?;
        }
        Ok(())
    }

    pub async fn collect(mut self) -> TeoDBResult<Vec<WalRecord>> {
        let mut records = Vec::with_capacity(self.record_count());
        while let Some(record) = self.next_record().await? {
            records.push(record);
        }
        Ok(records)
    }
}

/// WAL replay component. Pass one builds a metadata-only validated plan; pass
/// two is driven by `ReplayPlan::next_record`.
pub(super) struct WalReplay<'a> {
    root: &'a Path,
    committed: &'a AHashMap<WalTableKey, Generation>,
    mode: WalRecoveryMode,
}

impl<'a> WalReplay<'a> {
    pub(super) fn new(root: &'a Path, committed: &'a AHashMap<WalTableKey, Generation>, mode: WalRecoveryMode) -> Self {
        Self { root, committed, mode }
    }

    pub(super) async fn prepare(&self) -> TeoDBResult<ReplayPlan> {
        let paths = segment_paths(self.root).await?;
        let mut segments = Vec::with_capacity(paths.len());
        let mut entries = Vec::new();
        let mut tables: Vec<WalTableKey> = Vec::new();
        let mut table_indexes: AHashMap<WalTableKey, usize> = AHashMap::new();
        let mut latest_tombstones: AHashMap<TableIdent, FramePosition> = AHashMap::new();

        for (segment_index, original_path) in paths.into_iter().enumerate() {
            let identity = segment_identity(original_path.clone()).await?;
            let mut file = open_segment(&identity.path).await?;
            let mut offset = 0u64;
            let mut replay_len = identity.len;
            let mut corruption = None;

            while offset < identity.len {
                let position = FramePosition {
                    segment: segment_index,
                    offset,
                };
                match read_frame(&mut file, &identity.path, offset, identity.len).await? {
                    FrameDecode::Complete(record, consumed) => {
                        offset = offset
                            .saturating_add(consumed as u64)
                            .min(identity.len);
                        match record.header.op {
                            WalOp::DropTable => {
                                // The latest logical-name tombstone voids every
                                // earlier UUID incarnation. Filter once after
                                // validation so repeated recreate/drop cycles
                                // remain linear rather than repeatedly scanning.
                                latest_tombstones.insert(record.header.table, position);
                            }
                            WalOp::Append => {
                                let key = record.header.table_key()?;
                                if !self
                                    .committed
                                    .get(&key)
                                    .is_some_and(|&generation| record.header.generation <= generation)
                                {
                                    let table_index = if let Some(index) = table_indexes.get(&key) {
                                        *index
                                    } else {
                                        let index = tables.len();
                                        tables.push(key.clone());
                                        table_indexes.insert(key, index);
                                        index
                                    };
                                    entries.push(ReplayEntry {
                                        position,
                                        generation: record.header.generation,
                                        batch_id: record.header.batch_id,
                                        table_index,
                                    });
                                }
                            }
                        }
                    }
                    FrameDecode::Incomplete => {
                        replay_len = offset;
                        warn!(
                            path = %identity.path.display(),
                            offset,
                            remaining = identity.len - offset,
                            "partial frame at end of segment (torn write)"
                        );
                        break;
                    }
                    FrameDecode::Corrupt(reason) => {
                        replay_len = offset;
                        corruption = Some(reason);
                        break;
                    }
                }
            }
            drop(file);

            let mut snapshot_identity = identity;
            let live = if let Some(reason) = corruption {
                match self.mode {
                    WalRecoveryMode::Fail => {
                        return Err(corrupt_error(&snapshot_identity.path, offset, &reason));
                    }
                    WalRecoveryMode::Salvage => {
                        let quarantine = quarantine_path(&snapshot_identity.path);
                        error!(
                            path = %snapshot_identity.path.display(),
                            quarantine = %quarantine.display(),
                            offset,
                            reason,
                            "corrupt WAL frame — quarantining segment; validated prefix is preserved, suffix is LOST"
                        );
                        tokio::fs::rename(&snapshot_identity.path, &quarantine)
                            .await
                            .map_err(|error| {
                                TeoDBError::wal(format!("failed to quarantine corrupt segment: {error}"))
                            })?;
                        snapshot_identity.path = quarantine;
                        false
                    }
                }
            } else {
                true
            };

            segments.push(SegmentSnapshot {
                identity: snapshot_identity,
                replay_len,
                live,
            });
        }

        entries.retain(|entry: &ReplayEntry| {
            let table = &tables[entry.table_index].ident;
            latest_tombstones
                .get(table)
                .is_none_or(|tombstone| entry.position > *tombstone)
        });

        // The writer allocates a generation before awaiting WAL persistence,
        // so concurrent requests can be physically appended out of generation
        // order. Canonical replay sorts by `(generation, batch_id)`; physical
        // position is the stable tie-breaker and permits an
        // in-place unstable sort without a second O(record-count) allocation.
        entries.sort_unstable_by(|a, b| {
            a.generation
                .cmp(&b.generation)
                .then_with(|| a.batch_id.cmp(&b.batch_id))
                .then_with(|| a.position.cmp(&b.position))
        });
        let table_keys = entries
            .iter()
            .map(|entry| tables[entry.table_index].clone())
            .collect();

        Ok(ReplayPlan {
            root: self.root.to_path_buf(),
            segments,
            entries,
            tables,
            table_keys,
            entry_index: 0,
            file_segment: None,
            file: None,
            snapshot_checked: false,
            live_decoded_records: 0,
            peak_live_decoded_records: 0,
        })
    }
}

async fn segment_paths(root: &Path) -> TeoDBResult<Vec<PathBuf>> {
    let mut paths = Vec::new();
    let mut entries = tokio::fs::read_dir(root)
        .await
        .map_err(|error| TeoDBError::wal(format!("readdir {}: {error}", root.display())))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|error| TeoDBError::wal(format!("readdir: {error}")))?
    {
        if entry
            .file_name()
            .to_string_lossy()
            .ends_with(".wal")
        {
            paths.push(entry.path());
        }
    }
    paths.sort();
    Ok(paths)
}

async fn segment_identity(path: PathBuf) -> TeoDBResult<SegmentIdentity> {
    let metadata = tokio::fs::metadata(&path)
        .await
        .map_err(|error| TeoDBError::wal_source(format!("inspect WAL segment {}", path.display()), error))?;
    Ok(SegmentIdentity {
        path,
        len: metadata.len(),
        modified: metadata.modified().ok(),
    })
}

async fn verify_identity(expected: &SegmentIdentity) -> TeoDBResult<()> {
    let actual = segment_identity(expected.path.clone())
        .await
        .map_err(|_| snapshot_mismatch(&expected.path))?;
    if &actual != expected {
        return Err(snapshot_mismatch(&expected.path));
    }
    Ok(())
}

async fn open_segment(path: &Path) -> TeoDBResult<tokio::fs::File> {
    tokio::fs::File::open(path)
        .await
        .map_err(|error| TeoDBError::wal_source(format!("failed to read WAL segment {}", path.display()), error))
}

async fn read_frame(file: &mut tokio::fs::File, path: &Path, offset: u64, limit: u64) -> TeoDBResult<FrameDecode> {
    let remaining = limit.saturating_sub(offset);
    if remaining < segment::FRAME_HEADER_SIZE as u64 {
        return Ok(FrameDecode::Incomplete);
    }

    let mut header = [0u8; segment::FRAME_HEADER_SIZE];
    file.read_exact(&mut header)
        .await
        .map_err(|error| TeoDBError::wal_source(format!("read WAL frame header in {}", path.display()), error))?;
    let payload_len = u32::from_le_bytes(header[..4].try_into().expect("four-byte length")) as usize;
    if payload_len == 0 {
        let mut all_zero = header.iter().all(|byte| *byte == 0);
        let mut left = remaining - segment::FRAME_HEADER_SIZE as u64;
        let mut chunk = [0u8; 8192];
        while left > 0 {
            let take = left.min(chunk.len() as u64) as usize;
            file.read_exact(&mut chunk[..take])
                .await
                .map_err(|error| TeoDBError::wal_source(format!("read zero WAL tail in {}", path.display()), error))?;
            all_zero &= chunk[..take].iter().all(|byte| *byte == 0);
            left -= take as u64;
        }
        return Ok(if all_zero {
            FrameDecode::Incomplete
        } else {
            FrameDecode::Corrupt("zero payload length with non-zero trailing bytes".into())
        });
    }
    if payload_len > segment::MAX_PAYLOAD_BYTES {
        return Ok(FrameDecode::Corrupt(format!(
            "implausible payload length {payload_len} (corrupt length field)"
        )));
    }

    let total_content = segment::FRAME_HEADER_SIZE + payload_len;
    if remaining < total_content as u64 {
        return Ok(FrameDecode::Incomplete);
    }
    let padding = (8 - (total_content % 8)) % 8;
    let available_padding = padding.min((remaining - total_content as u64) as usize);
    let mut frame = Vec::with_capacity(total_content + available_padding);
    frame.extend_from_slice(&header);
    frame.resize(total_content + available_padding, 0);
    file.read_exact(&mut frame[segment::FRAME_HEADER_SIZE..])
        .await
        .map_err(|error| TeoDBError::wal_source(format!("read WAL frame in {}", path.display()), error))?;
    Ok(segment::decode_frame(&frame))
}

fn corrupt_error(path: &Path, offset: u64, reason: &str) -> TeoDBError {
    TeoDBError::wal(format!(
        "corrupt WAL frame in {} at offset {offset}: {reason}; acknowledged data after this point may be \
         unrecoverable — repair or remove the segment, or set wal.recovery_mode = \"salvage\" to quarantine it and continue",
        path.display()
    ))
}

fn snapshot_mismatch(path: &Path) -> TeoDBError {
    TeoDBError::wal(format!(
        "WAL segment snapshot mismatch between validation and replay: {}",
        path.display()
    ))
}

fn quarantine_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    path.with_file_name(format!("{file_name}.corrupt"))
}
