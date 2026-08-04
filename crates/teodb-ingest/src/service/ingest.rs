//! Row-ingestion service: the JSON write path behind `POST .../ingest`,
//! independent of HTTP.
//!
//! Flattens nested JSON, infers/uses the table schema (auto-creating the table
//! on first write — schema-on-write), enforces per-writer idempotency with a
//! claim-before-WAL handshake, and lands rows in the WAL *before* the buffer so
//! every ACKed batch is durable (invariant I1).

use std::collections::HashMap;
use std::sync::Arc;

use tracing::{debug, info};

use teodb_core::error::TeoDBError;
use teodb_core::ident::TableIdent;
use teodb_core::location::{ObjectLocation, StorageScheme};
use teodb_core::table::CreateTableRequestBuilder;

use crate::idempotency::{Claim, IngestReceipt};
use crate::json;
use crate::service::IngestService;

/// What an ingest attempt produced.
pub enum IngestOutcome {
    /// Rows were durably appended; carries the fresh receipt.
    Accepted(IngestReceipt),
    /// A prior request with the same idempotency key already landed these rows;
    /// carries the original receipt, replayed without re-ingesting.
    Deduplicated(IngestReceipt),
}

impl IngestService {
    /// Flatten, validate, and durably ingest `rows` into `ident`'s hot buffer.
    ///
    /// `idempotency_key`, when present, is claimed before the WAL append so exactly
    /// one concurrent request per key reaches durability; a completed key replays
    /// its original receipt.
    #[tracing::instrument(
        name = "ingest.rows",
        skip_all,
        fields(table = %ident, row_count = rows.len())
    )]
    pub async fn ingest_rows(
        &self,
        ident: &TableIdent,
        rows: &[serde_json::Value],
        idempotency_key: Option<&str>,
    ) -> Result<IngestOutcome, TeoDBError> {
        if rows.is_empty() {
            return Err(TeoDBError::InvalidArgument {
                field: "data".into(),
                message: "request must contain at least one row".into(),
            });
        }

        // Flatten nested JSON.
        let flat_rows = json::flatten_rows(rows);
        let row_count = flat_rows.len() as u64;

        // Infer the Arrow schema from the JSON values.
        let inferred_schema = arrow::json::reader::infer_json_schema_from_iterator(
            flat_rows
                .iter()
                .map(Ok::<_, arrow::error::ArrowError>),
        )
        .map(Arc::new)
        .map_err(|e| TeoDBError::InvalidArgument {
            field: "data".into(),
            message: format!("cannot infer schema from JSON: {e}"),
        })?;

        // Ensure the table exists using schema-on-write.
        let buffer = match self
            .buffers
            .get_or_load(ident, self.catalog.as_ref())
            .await
        {
            Ok(b) => b,
            Err(TeoDBError::NotFound { .. }) => {
                debug!(table = %ident, "table not found, auto-creating from inferred schema");
                self.auto_create_table(ident, &inferred_schema)
                    .await?;
                self.buffers
                    .get_or_load(ident, self.catalog.as_ref())
                    .await?
            }
            Err(e) => return Err(e),
        };

        // ── Use table schema for parsing (existing table may differ from inferred) ──
        let schema = buffer.metadata().current_schema()?.clone();
        let arrow_schema = Arc::new(teodb_storage::schema_to_arrow(&schema));

        // Convert flattened JSON into an Arrow RecordBatch.
        let batch = {
            let decoder = arrow::json::ReaderBuilder::new(arrow_schema)
                .with_batch_size(row_count as usize)
                .build_decoder();
            let result = decoder
                .and_then(|mut d| d.serialize(&flat_rows).map(|()| d))
                .and_then(|mut d| d.flush());
            match result {
                Ok(Some(batch)) => batch,
                Ok(None) => {
                    return Err(TeoDBError::InvalidArgument {
                        field: "data".into(),
                        message: "rows produced no Arrow data".into(),
                    });
                }
                Err(e) => {
                    return Err(TeoDBError::InvalidArgument {
                        field: "data".into(),
                        message: format!("JSON→Arrow conversion failed: {e}"),
                    });
                }
            }
        };

        // Claim idempotency before WAL append within the node scope.
        if let Some(key) = idempotency_key {
            match self.idempotency.claim(ident, key) {
                Claim::Acquired => {}
                Claim::Duplicate(receipt) => {
                    info!(table = %ident, idempotency_key = %key, "duplicate ingest deduplicated");
                    return Ok(IngestOutcome::Deduplicated(receipt));
                }
                Claim::InProgress => {
                    return Err(TeoDBError::Conflict {
                        resource: format!("idempotency key '{key}' on table {ident}"),
                        expected: "key completed or unused".into(),
                        actual: "another request with this key is in flight".into(),
                    });
                }
            }
        }

        // Reserve buffer capacity before WAL append so once a record is durable, its
        // post-WAL buffer admission cannot fail.
        let batch_id = uuid::Uuid::now_v7();
        let created_at_ms = chrono::Utc::now().timestamp_millis();
        let total_rows = batch.num_rows() as u64;
        let reservation = match buffer.reserve(&batch) {
            Ok(reservation) => reservation,
            Err(e) => {
                if let Some(key) = idempotency_key {
                    self.idempotency.abort(ident, key);
                }
                return Err(e);
            }
        };

        let wal_record = teodb_storage::wal::WalRecord {
            header: teodb_storage::wal::WalHeader {
                protocol_version: teodb_core::write_protocol::WRITE_PROTOCOL_VERSION,
                table_uuid: Some(buffer.metadata().table_uuid),
                batch_id,
                table: ident.clone(),
                schema_id: buffer.metadata().current_schema_id,
                generation: reservation.generation,
                created_at_ms,
                idempotency_key: idempotency_key.map(ToString::to_string),
                row_count: total_rows,
                byte_count: batch.get_array_memory_size() as u64,
                op: teodb_storage::wal::WalOp::Append,
            },
            batch: batch.clone(),
        };
        if let Err(e) = self.wal.append(&wal_record).await {
            buffer.release_reservation(reservation);
            // Release the key so a client retry can win it again.
            if let Some(key) = idempotency_key {
                self.idempotency.abort(ident, key);
            }
            return Err(TeoDBError::wal(format!("WAL append failed: {e}")));
        }

        let last_gen = buffer
            .insert_reserved_at(batch_id, reservation, created_at_ms, batch)
            .generation;

        let receipt = IngestReceipt {
            batch_id,
            writer_id: self.wal.writer_identity().writer_id,
            generation: last_gen,
            accepted_rows: total_rows,
        };
        if let Some(key) = idempotency_key {
            self.idempotency
                .complete(ident, key, receipt.clone());
        }

        Ok(IngestOutcome::Accepted(receipt))
    }

    /// Auto-create a table from an inferred Arrow schema (schema-on-write).
    async fn auto_create_table(
        &self,
        ident: &TableIdent,
        arrow_schema: &arrow::datatypes::Schema,
    ) -> Result<(), TeoDBError> {
        // Build TeoDB column metadata from inferred Arrow types.
        let mut columns = Vec::with_capacity(arrow_schema.fields().len());
        for (idx, field) in arrow_schema.fields().iter().enumerate() {
            let teo_type = teodb_storage::arrow_to_teo_data_type(field.data_type())?;
            columns.push(teodb_core::schema::ColumnMeta {
                id: (idx + 1) as teodb_core::ident::FieldId,
                name: field.name().clone(),
                data_type: teo_type,
                nullable: field.is_nullable(),
                doc: None,
            });
        }

        let schema_def = teodb_core::schema::SchemaDefinition {
            schema_id: 0,
            columns,
            identifier_field_ids: vec![],
        };

        let warehouse = &self.default_warehouse_uri;
        let location = ObjectLocation::parse(&format!("{warehouse}/{}/{}", ident.namespace, ident.name))
            .unwrap_or_else(|_| ObjectLocation {
                scheme: StorageScheme::S3,
                bucket: Some("teodb".into()),
                key: format!("{}/{}", ident.namespace, ident.name),
            });

        // Auto-create namespace (ignore AlreadyExists).
        match self
            .catalog
            .create_namespace(&ident.namespace, HashMap::new())
            .await
        {
            Ok(()) => info!(namespace = %ident.namespace, "namespace auto-created for schema-on-write"),
            Err(TeoDBError::AlreadyExists { .. }) => {}
            Err(e) => return Err(e),
        }

        let req = CreateTableRequestBuilder::new(ident.clone(), schema_def, location).build()?;

        match self.catalog.create_table(req).await {
            Ok(_) => {
                info!(namespace = %ident.namespace, table = %ident.name, "table auto-created via schema-on-write");
                Ok(())
            }
            Err(TeoDBError::AlreadyExists { .. }) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{DataType, Field, Schema};
    use teodb_test_support::{MockCatalog, table_metadata};

    use crate::buffer::BufferRegistry;
    use crate::idempotency::IdempotencyIndex;

    #[tokio::test]
    async fn auto_create_uses_current_warehouse_location_policy() {
        let catalog = Arc::new(
            MockCatalog::builder()
                .commit_result(table_metadata("s3://unused/result"))
                .build(),
        );
        let wal_dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(
            teodb_storage::wal::WalManager::open(teodb_storage::wal::WalConfig {
                root_dir: wal_dir.path().to_path_buf(),
                fsync_on_append: false,
                ..Default::default()
            })
            .await
            .unwrap(),
        );
        let service = IngestService::new(
            catalog.clone(),
            Arc::new(BufferRegistry::new(wal.clone(), 1024 * 1024, 512 * 1024)),
            wal,
            Arc::new(IdempotencyIndex::new(std::time::Duration::from_secs(60), 1000)),
            Arc::from("s3://ingest-warehouse"),
        );
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        service
            .auto_create_table(&TableIdent::new("default", "events"), &schema)
            .await
            .unwrap();

        let created = catalog.created_tables();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].location.to_uri(), "s3://ingest-warehouse/default/events");
    }
}
