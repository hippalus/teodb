//! Dedicated WAL writer task.
//!
//! All appends are funneled through one task that owns the open segment
//! file. Appenders send encoded frames over a bounded channel and wait for
//! an ack that is sent only after the frame is written (and fsynced when
//! `fsync_on_append` is set), preserving invariant I1: every acknowledged
//! batch is durable before the client ACK.
//!
//! Owning the segment in a single task removes the global mutex that was
//! previously held across `write_all` + `sync_data`, and is the foundation
//! for group commit (sharing one fsync across queued frames).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use teodb_core::error::{TeoDBError, TeoDBResult};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

/// Bounded queue depth between appenders and the writer task. A full queue
/// makes `append()` wait, which is the natural backpressure signal.
const WRITE_QUEUE_DEPTH: usize = 1024;

/// Maximum frames persisted under a single fsync. Bounds both group latency
/// (the first frame in a group waits for the whole group's write) and the
/// blast radius of a failed group (every waiter in it gets the error).
const MAX_GROUP_FRAMES: usize = 128;

/// One encoded frame waiting to be persisted.
pub(crate) struct WriteRequest {
    pub frame: Vec<u8>,
    pub ack: oneshot::Sender<TeoDBResult<()>>,
}

/// Commands accepted by the writer task.
pub(crate) enum WriterCommand {
    Write(WriteRequest),
    /// Close the current segment so the next write opens a fresh one.
    Rotate(oneshot::Sender<()>),
}

struct OpenSegment {
    path: PathBuf,
    file: tokio::fs::File,
    bytes_written: u64,
}

pub(crate) struct SegmentWriter {
    root_dir: PathBuf,
    max_segment_bytes: u64,
    fsync_on_append: bool,
    current: Option<OpenSegment>,
    next_seq: u64,
    /// Published for `gc()`: the open segment's `seq + 1`, or 0 when none.
    current_seq: Arc<AtomicU64>,
}

impl SegmentWriter {
    /// Spawn the writer task. Returns the command channel; the task exits
    /// when every sender is dropped.
    pub(crate) fn spawn(
        root_dir: PathBuf,
        max_segment_bytes: u64,
        fsync_on_append: bool,
        next_seq: u64,
        current_seq: Arc<AtomicU64>,
    ) -> mpsc::Sender<WriterCommand> {
        let (tx, rx) = mpsc::channel(WRITE_QUEUE_DEPTH);
        let writer = Self {
            root_dir,
            max_segment_bytes,
            fsync_on_append,
            current: None,
            next_seq,
            current_seq,
        };
        tokio::spawn(writer.run(rx));
        tx
    }

    async fn run(mut self, mut rx: mpsc::Receiver<WriterCommand>) {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                WriterCommand::Rotate(done) => {
                    self.close_current();
                    let _ = done.send(());
                }
                WriterCommand::Write(first) => {
                    // Group commit: drain whatever queued up while the
                    // previous group was being written, so one fsync covers
                    // them all. There is deliberately no delay timer —
                    // batching arises naturally under load (requests queue
                    // behind the in-flight fsync), while a lone append keeps
                    // its minimal latency.
                    let mut group = vec![first];
                    let mut rotate_after: Option<oneshot::Sender<()>> = None;
                    while group.len() < MAX_GROUP_FRAMES {
                        match rx.try_recv() {
                            Ok(WriterCommand::Write(req)) => group.push(req),
                            Ok(WriterCommand::Rotate(done)) => {
                                // Honor ordering: rotate after the frames
                                // that were queued before it are persisted.
                                rotate_after = Some(done);
                                break;
                            }
                            Err(_) => break,
                        }
                    }

                    match self.write_group(&group).await {
                        Ok(()) => {
                            for req in group {
                                let _ = req.ack.send(Ok(()));
                            }
                        }
                        Err(e) => {
                            // The segment may be in an undefined state (e.g.
                            // partially written frame) — drop it so the next
                            // group starts a fresh one. None of the group's
                            // frames were acked, so durability semantics
                            // hold; every waiter sees the failure.
                            self.close_current();
                            let msg = e.to_string();
                            for req in group {
                                let _ = req
                                    .ack
                                    .send(Err(TeoDBError::wal(format!("group write failed: {msg}"))));
                            }
                        }
                    }

                    if let Some(done) = rotate_after {
                        self.close_current();
                        let _ = done.send(());
                    }
                }
            }
        }
    }

    /// Write the group's frames to the current segment and fsync once if
    /// configured. Rotates beforehand when the group would overflow the
    /// segment limit.
    async fn write_group(&mut self, group: &[WriteRequest]) -> TeoDBResult<()> {
        let total: u64 = group.iter().map(|r| r.frame.len() as u64).sum();

        if self
            .current
            .as_ref()
            .is_some_and(|s| s.bytes_written + total > self.max_segment_bytes)
        {
            self.close_current();
        }

        if self.current.is_none() {
            self.open_segment().await?;
        }

        let seg = self
            .current
            .as_mut()
            .ok_or_else(|| TeoDBError::wal("segment missing after successful open"))?;

        // Byte length on disk before this group. On any failure we truncate
        // back to it so that NONE of the group's frames survive — an earlier
        // frame in the group may already be fully written and CRC-valid, and
        // without this rollback it would replay despite `append()` returning
        // Err, breaking the "failed append is never replay-visible" contract.
        // The caller drops the segment afterwards, so the stale file cursor is
        // never reused.
        let group_start = seg.bytes_written;

        for req in group {
            if let Err(e) = seg.file.write_all(&req.frame).await {
                let _ = seg.file.set_len(group_start).await;
                return Err(TeoDBError::wal(format!("write failed: {e}")));
            }
        }

        // `tokio::fs::File::write_all` only copies into Tokio's userspace buffer
        // and dispatches the real write to the blocking pool; it can return
        // before the syscall runs. Flush drains that buffer so the bytes reach
        // the OS before we ack — without it a frame may be acked while still
        // pending, and a crash (or a reader right after `drop`) sees nothing.
        // `sync_data` would also do this via `complete_inflight`, but only on
        // the fsync path; flushing unconditionally keeps the non-fsync ack
        // honest (durable against process crash, if not power loss).
        if let Err(e) = seg.file.flush().await {
            let _ = seg.file.set_len(group_start).await;
            return Err(TeoDBError::wal(format!("flush failed: {e}")));
        }

        if self.fsync_on_append
            && let Err(e) = seg.file.sync_data().await
        {
            let _ = seg.file.set_len(group_start).await;
            return Err(TeoDBError::wal(format!("fsync failed: {e}")));
        }

        seg.bytes_written += total;
        Ok(())
    }

    async fn open_segment(&mut self) -> TeoDBResult<()> {
        let seq = self.next_seq;
        self.next_seq += 1;
        let segment_name = format!("{seq:020}.wal");
        let path = self.root_dir.join(&segment_name);
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| TeoDBError::wal(format!("failed to open segment: {e}")))?;
        self.current = Some(OpenSegment {
            path,
            file,
            bytes_written: 0,
        });
        self.current_seq.store(seq + 1, Ordering::Release);
        Ok(())
    }

    fn close_current(&mut self) {
        if let Some(seg) = self.current.take() {
            drop(seg.file);
            info!(path = %seg.path.display(), bytes = seg.bytes_written, "WAL segment rotated");
        }
        self.current_seq.store(0, Ordering::Release);
    }
}
