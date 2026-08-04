use std::sync::Arc;

use tracing::{error, info, warn};

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::file::TableMetadata;
use teodb_core::traits::catalog::{Catalog, CommitAppend};
use teodb_core::write_protocol::{AppendCommitIdentity, WalTableKey};
use teodb_storage::wal::{PreparedFlush, WalManager};

use crate::buffer::TableBuffer;

use super::FlushOutcome;

pub(super) struct FlushCommit<'a> {
    pub(super) catalog: &'a dyn Catalog,
    pub(super) wal: &'a WalManager,
    pub(super) buffer: &'a TableBuffer,
    pub(super) prepared: &'a PreparedFlush,
}

pub(super) async fn commit_flush(args: FlushCommit<'_>) -> TeoDBResult<FlushOutcome> {
    let request = build_commit_request(args.prepared);
    match args.catalog.commit_append(request).await {
        Ok(updated_iceberg) => complete_flush(args.wal, args.buffer, args.prepared, updated_iceberg).await,
        Err(error @ teodb_core::error::TeoDBError::CommitStateUnknown { .. }) => {
            // The prepared state and sidecar remain authoritative. The caller
            // must run exact status resolution; never merge this range back to
            // pending or issue a new commit ID.
            Err(error)
        }
        Err(error @ TeoDBError::ExternalRetryable(_)) => {
            // Publication is known not to have started. Keep the immutable
            // prepared intent and retry it without rewriting files.
            Err(error)
        }
        Err(error @ TeoDBError::StaleWriterEpoch { current_epoch, .. }) => {
            // A definitive stale-epoch rejection permits retiring this
            // uncommitted intent. Advance the durable local fence first, then
            // return the rows to pending so the next attempt gets the new
            // epoch and a new exact identity.
            args.wal.observe_epoch_and_bump(current_epoch)?;
            args.wal
                .remove_prepared(args.prepared.table_uuid)
                .await?;
            args.buffer.mark_flush_failed(args.prepared)?;
            Err(error)
        }
        Err(
            error @ (TeoDBError::WriterRegistryFull { .. }
            | TeoDBError::MetadataCorruption { .. }
            | TeoDBError::TableIncarnationMismatch { .. }
            | TeoDBError::WriteProtocol { .. }),
        ) => {
            args.buffer
                .mark_flush_blocked_with_class(args.prepared, error.to_string(), error.code(), 1)?;
            Err(error)
        }
        Err(error) => {
            if let Err(cleanup_error) = args
                .wal
                .remove_prepared(args.prepared.table_uuid)
                .await
            {
                // Preserve the exact prepared/in-flight owner. Clearing it
                // while a durable sidecar remains would let a later flush
                // create a different commit ID for the same rows.
                warn!(
                    table = %args.buffer.ident(),
                    error = %cleanup_error,
                    "failed to remove prepared sidecar after rejected commit; preserving intent"
                );
                return Err(cleanup_error);
            }
            args.buffer.mark_flush_failed(args.prepared)?;
            if matches!(&error, teodb_core::error::TeoDBError::Conflict { .. }) {
                refresh_after_conflict(args.catalog, args.buffer).await;
            } else {
                error!(table = %args.buffer.ident(), error = %error, "flush commit failed");
            }
            Err(error)
        }
    }
}

pub(super) fn build_commit_request(prepared: &PreparedFlush) -> CommitAppend {
    CommitAppend {
        table: prepared.table.clone(),
        table_uuid: prepared.table_uuid,
        identity: AppendCommitIdentity {
            writer_id: prepared.writer_id,
            writer_epoch: prepared.writer_epoch,
            commit_id: prepared.commit_id,
            generations: prepared.generations,
        },
        base_snapshot_id: prepared.base_snapshot_id,
        added_data_files: prepared.data_files.clone(),
        properties: Default::default(),
    }
}

pub(super) async fn complete_flush(
    wal: &WalManager,
    buffer: &TableBuffer,
    prepared: &PreparedFlush,
    updated: Arc<TableMetadata>,
) -> TeoDBResult<FlushOutcome> {
    buffer.mark_committed(prepared.generations.hi, updated)?;

    wal.mark_committed(
        WalTableKey::new(prepared.table_uuid, buffer.ident().clone()),
        prepared.generations.hi,
    )
    .await;
    if let Err(error) = wal.remove_prepared(prepared.table_uuid).await {
        warn!(
            table = %buffer.ident(),
            commit_id = %prepared.commit_id,
            error = %error,
            "prepared sidecar cleanup failed after exact catalog success"
        );
    }
    if let Err(error) = wal.gc().await {
        warn!(table = %buffer.ident(), error = %error, "WAL GC failed (non-fatal)");
    }

    info!(
        table = %buffer.ident(),
        table_uuid = %prepared.table_uuid,
        writer_id = %prepared.writer_id,
        writer_epoch = %prepared.writer_epoch,
        commit_id = %prepared.commit_id,
        gen_lo = prepared.generations.lo,
        gen_hi = prepared.generations.hi,
        record_count = prepared.record_count,
        "flush committed"
    );
    Ok(FlushOutcome::Committed {
        gen_lo: prepared.generations.lo,
        gen_hi: prepared.generations.hi,
        record_count: prepared.record_count,
    })
}

async fn refresh_after_conflict(catalog: &dyn Catalog, buffer: &TableBuffer) {
    match catalog.load_table(buffer.ident()).await {
        Ok(fresh) => {
            buffer.refresh_metadata(fresh);
            warn!(
                table = %buffer.ident(),
                "flush conflict: refreshed metadata, will retry next tick"
            );
        }
        Err(load_err) => {
            error!(table = %buffer.ident(), error = %load_err, "failed to reload metadata after conflict");
        }
    }
}
