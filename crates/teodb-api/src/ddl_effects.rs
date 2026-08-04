//! Buffer and WAL side effects of executed DDL statements.
//!
//! Shared by every DDL entry point (REST SQL, Flight SQL, REST table CRUD)
//! so the durability rules live in one place:
//! - a buffer is evicted only when the statement actually mutated catalog
//!   state (`DdlResult::changed`) — an `IF NOT EXISTS` no-op must not
//!   discard the existing table's unflushed rows;
//! - every drop writes a WAL tombstone so replay neither fails on the
//!   missing table nor leaks the old incarnation's rows into a recreated one.

use teodb_core::ident::TableIdent;
use teodb_query::ddl::{DdlPlan, DdlResult};
use teodb_storage::wal::WalManager;
use tracing::warn;

use teodb_ingest::buffer::BufferRegistry;
use teodb_ingest::idempotency::IdempotencyIndex;

/// Apply buffer eviction, idempotency-key eviction, and WAL tombstones after
/// a DDL statement executed successfully. No-op when the statement didn't
/// mutate catalog state.
pub async fn apply_post_ddl(
    buffers: &BufferRegistry,
    wal: &WalManager,
    idempotency: &IdempotencyIndex,
    plan: &DdlPlan,
    result: &DdlResult,
) {
    if !result.changed {
        return;
    }

    match plan {
        DdlPlan::DropTable(p) => {
            buffers.remove(&p.ident);
            idempotency.evict_table(&p.ident);
            append_tombstone(wal, &p.ident).await;
        }
        DdlPlan::CreateTable(p) => {
            // Discard any stale buffer (and receipts) from a previous
            // incarnation of this table — the buffer carries the old UUID
            // and snapshot lineage.
            let ident = TableIdent::new(&p.namespace, &p.table_name);
            buffers.remove(&ident);
            idempotency.evict_table(&ident);
        }
        DdlPlan::DropSchema(p) => {
            for ident in buffers.tables() {
                if ident.namespace == p.namespace {
                    buffers.remove(&ident);
                    idempotency.evict_table(&ident);
                    append_tombstone(wal, &ident).await;
                }
            }
        }
        DdlPlan::CreateSchema(_) | DdlPlan::ShowTables(_) | DdlPlan::ShowColumns(_) | DdlPlan::DescribeTable(_) => {}
    }
}

/// Write a drop tombstone, downgrading failure to a loud warning: the drop
/// already succeeded in the catalog, and startup replay self-heals missing
/// tombstones (it skips tables the catalog no longer knows and writes the
/// tombstone then).
pub async fn append_tombstone(wal: &WalManager, ident: &TableIdent) {
    if let Err(e) = wal.append_drop_tombstone(ident).await {
        warn!(
            table = %ident,
            error = %e,
            "failed to append WAL drop tombstone — replay will skip and re-tombstone this table at next startup"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use teodb_query::ddl::{CreateTablePlan, DropTablePlan};
    use teodb_storage::wal::{WalConfig, WalManager};
    use teodb_test_support::{MockCatalog, table_metadata};

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
    }

    async fn registry_with_buffered_table(ident: &TableIdent, wal: Arc<WalManager>) -> BufferRegistry {
        let registry = BufferRegistry::new(wal, 1024 * 1024, 512 * 1024);
        let catalog = MockCatalog::builder()
            .serves(
                &ident.name,
                table_metadata(&format!("file:///{}/{}", ident.namespace, ident.name)),
            )
            .build();
        let buffer = registry
            .get_or_load(ident, &catalog)
            .await
            .expect("load test buffer");
        buffer
            .insert(uuid::Uuid::now_v7(), test_batch())
            .unwrap();
        registry
    }

    fn test_index() -> IdempotencyIndex {
        IdempotencyIndex::new(std::time::Duration::from_secs(60), 100)
    }

    async fn test_wal(dir: &std::path::Path) -> WalManager {
        WalManager::open(WalConfig {
            root_dir: dir.to_path_buf(),
            fsync_on_append: false,
            ..Default::default()
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn drop_table_evicts_and_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(test_wal(dir.path()).await);
        let ident = TableIdent::new("ns", "t");
        let registry = registry_with_buffered_table(&ident, wal.clone()).await;

        let plan = DdlPlan::DropTable(DropTablePlan {
            ident: ident.clone(),
            if_exists: false,
            options: Default::default(),
        });
        apply_post_ddl(
            &registry,
            wal.as_ref(),
            &test_index(),
            &plan,
            &DdlResult::changed("dropped"),
        )
        .await;

        assert!(registry.get(&ident).is_none(), "buffer evicted");
        assert_eq!(registry.evicted_rows_total(), 3, "discarded rows counted");
        // The tombstone voids the (hypothetical) earlier records on replay.
        assert!(
            wal.prepare_replay()
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn if_not_exists_noop_keeps_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(test_wal(dir.path()).await);
        let ident = TableIdent::new("ns", "t");
        let registry = registry_with_buffered_table(&ident, wal.clone()).await;

        let plan = DdlPlan::CreateTable(CreateTablePlan {
            namespace: "ns".into(),
            table_name: "t".into(),
            columns: vec![],
            partition_by: vec![],
            if_not_exists: true,
        });
        apply_post_ddl(
            &registry,
            wal.as_ref(),
            &test_index(),
            &plan,
            &DdlResult::unchanged("already exists"),
        )
        .await;

        assert!(registry.get(&ident).is_some(), "no-op DDL must not evict");
        assert_eq!(registry.evicted_rows_total(), 0);
    }

    #[tokio::test]
    async fn actual_create_evicts_stale_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(test_wal(dir.path()).await);
        let ident = TableIdent::new("ns", "t");
        let registry = registry_with_buffered_table(&ident, wal.clone()).await;

        let plan = DdlPlan::CreateTable(CreateTablePlan {
            namespace: "ns".into(),
            table_name: "t".into(),
            columns: vec![],
            partition_by: vec![],
            if_not_exists: false,
        });
        apply_post_ddl(
            &registry,
            wal.as_ref(),
            &test_index(),
            &plan,
            &DdlResult::changed("created"),
        )
        .await;

        assert!(registry.get(&ident).is_none(), "stale buffer evicted on real create");
        assert_eq!(registry.evicted_rows_total(), 3);
    }
}
