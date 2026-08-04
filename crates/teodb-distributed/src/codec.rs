//! `TeoLogicalExtensionCodec` — Serializes TeoDB table providers for Ballista.
//!
//! When Ballista distributes a query plan, it serializes logical plan nodes
//! (including table scans) via protobuf. Custom `TableProvider` types like
//! `TeoTableProvider` require a codec that knows how to encode/decode them.
//!
//! **Encoding:** Downcasts to `TeoTableProvider`, builds a frozen
//! `SnapshotScanDescriptor` capturing the exact snapshot, schema, and file
//! list, then serializes it as JSON bytes.
//!
//! **Decoding:** Deserializes the `SnapshotScanDescriptor` and creates a
//! `PinnedScanTableProvider` that scans from the frozen descriptor without
//! touching the catalog — guaranteeing snapshot isolation.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use ballista_core::serde::BallistaLogicalExtensionCodec;
use datafusion::catalog::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::Extension;
use datafusion::sql::TableReference;
use datafusion_proto::logical_plan::LogicalExtensionCodec;

use serde::{Deserialize, Serialize};
use teodb_core::SnapshotScanDescriptor;
use teodb_query::{PinnedScanTableProvider, TeoTableProvider};

/// Wire-format version for the serialized `SnapshotScanDescriptor`. Bump when
/// the descriptor's shape changes incompatibly; the decoder rejects mismatches
/// so a coordinator/executor version skew fails loudly instead of silently
/// misinterpreting bytes.
const DESCRIPTOR_CODEC_VERSION: u32 = 1;

#[derive(Serialize)]
struct VersionedDescriptorRef<'a> {
    version: u32,
    descriptor: &'a SnapshotScanDescriptor,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionedDescriptor {
    // Version is validated via `VersionPeek` before this full decode.
    #[allow(dead_code)]
    version: u32,
    descriptor: SnapshotScanDescriptor,
}

/// Encode a descriptor with its codec version envelope.
fn encode_descriptor(descriptor: &SnapshotScanDescriptor) -> Result<Vec<u8>> {
    serde_json::to_vec(&VersionedDescriptorRef {
        version: DESCRIPTOR_CODEC_VERSION,
        descriptor,
    })
    .map_err(|e| DataFusionError::Internal(format!("failed to serialize SnapshotScanDescriptor: {e}")))
}

/// Decode a descriptor, rejecting an unsupported codec version with context.
fn decode_descriptor(buf: &[u8]) -> Result<SnapshotScanDescriptor> {
    // Peek the version first so a skew yields a clear error, not a parse failure
    // against the current shape.
    #[derive(Deserialize)]
    struct VersionPeek {
        version: u32,
    }
    let peek: VersionPeek = serde_json::from_slice(buf)
        .map_err(|e| DataFusionError::Internal(format!("failed to decode SnapshotScanDescriptor envelope: {e}")))?;
    if peek.version != DESCRIPTOR_CODEC_VERSION {
        return Err(DataFusionError::Internal(format!(
            "unsupported SnapshotScanDescriptor codec version {} (this node supports {DESCRIPTOR_CODEC_VERSION}); \
             ensure coordinator and executors run the same TeoDB version",
            peek.version
        )));
    }
    let envelope: VersionedDescriptor = serde_json::from_slice(buf).map_err(|e| {
        DataFusionError::Internal(format!(
            "failed to decode SnapshotScanDescriptor (version {}): {e}",
            peek.version
        ))
    })?;
    Ok(envelope.descriptor)
}

/// Extension codec that wraps Ballista's default codec and adds support
/// for `TeoTableProvider` ↔ `PinnedScanTableProvider` serialization.
pub struct TeoLogicalExtensionCodec {
    inner: BallistaLogicalExtensionCodec,
}

impl TeoLogicalExtensionCodec {
    pub fn new() -> Self {
        Self {
            inner: BallistaLogicalExtensionCodec::default(),
        }
    }
}

impl Default for TeoLogicalExtensionCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for TeoLogicalExtensionCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TeoLogicalExtensionCodec")
            .finish()
    }
}

impl LogicalExtensionCodec for TeoLogicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[datafusion::logical_expr::LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _table_ref: &TableReference,
        _schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let descriptor = decode_descriptor(buf)?;

        let provider = PinnedScanTableProvider::try_new(descriptor)
            .map_err(|e| DataFusionError::Internal(format!("failed to create PinnedScanTableProvider: {e}")))?;

        Ok(Arc::new(provider))
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        // Handle TeoTableProvider — serialize its snapshot as a frozen descriptor.
        if let Some(teo_provider) = node.downcast_ref::<TeoTableProvider>() {
            let descriptor = teo_provider
                .snapshot_scan_descriptor()
                .map_err(|e| DataFusionError::Internal(format!("failed to build scan descriptor: {e}")))?
                .ok_or_else(|| DataFusionError::Plan("cannot serialize table with no current snapshot".into()))?;

            buf.extend_from_slice(&encode_descriptor(&descriptor)?);
            return Ok(());
        }

        // Handle PinnedScanTableProvider — already has a descriptor, re-serialize it.
        if let Some(pinned) = node.downcast_ref::<PinnedScanTableProvider>() {
            buf.extend_from_slice(&encode_descriptor(pinned.descriptor())?);
            return Ok(());
        }

        // Fall through to the inner codec for non-Teo table providers.
        self.inner
            .try_encode_table_provider(_table_ref, node, buf)
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn datafusion::datasource::file_format::FileFormatFactory>> {
        self.inner.try_decode_file_format(buf, ctx)
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn datafusion::datasource::file_format::FileFormatFactory>,
    ) -> Result<()> {
        self.inner.try_encode_file_format(buf, node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_roundtrip_snapshot_descriptor() {
        use std::collections::HashMap;
        use teodb_core::file::*;
        use teodb_core::location::{ObjectLocation, StorageScheme};
        use teodb_core::schema::*;

        let snapshot = Snapshot {
            snapshot_id: 42,
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: 1000,
            operation: SnapshotOperation::Append,
            data_files: vec![DataFile {
                content: DataContent::Data,
                path: ObjectLocation {
                    scheme: StorageScheme::S3,
                    bucket: Some("teodb".into()),
                    key: "tpch/region/data/part-0.parquet".into(),
                },
                format: FileFormat::Parquet,
                partition_spec_id: 0,
                sort_order_id: Some(0),
                schema_id: 0,
                partition_values: HashMap::new(),
                record_count: 5,
                file_size_bytes: 1024,
                column_sizes: HashMap::new(),
                value_counts: HashMap::new(),
                null_value_counts: HashMap::new(),
                nan_value_counts: HashMap::new(),
                lower_bounds: HashMap::new(),
                upper_bounds: HashMap::new(),
                split_offsets: vec![],
                equality_ids: vec![],
                key_metadata: None,
            }],
            delete_files: vec![],
            summary: HashMap::new(),
        };

        let metadata = TableMetadata {
            table_uuid: uuid::Uuid::nil(),
            namespace: "tpch".into(),
            table_name: "region".into(),
            table_location: ObjectLocation {
                scheme: StorageScheme::S3,
                bucket: Some("teodb".into()),
                key: "tpch/region".into(),
            },
            current_snapshot_id: Some(42),
            current_schema_id: 0,
            current_partition_spec_id: 0,
            current_sort_order_id: 0,
            schemas: vec![SchemaDefinition {
                schema_id: 0,
                columns: vec![ColumnMeta {
                    id: 1,
                    name: "r_regionkey".into(),
                    data_type: TeoDataType::Int32,
                    nullable: false,
                    doc: None,
                }],
                identifier_field_ids: vec![1],
            }],
            partition_specs: vec![PartitionSpec {
                spec_id: 0,
                fields: vec![],
            }],
            sort_orders: vec![SortOrder {
                order_id: 0,
                fields: vec![],
            }],
            snapshots: vec![snapshot.clone()],
            current_snapshot: Some(snapshot.clone()),
            properties: HashMap::new(),
        };

        let descriptor = SnapshotScanDescriptor::from_metadata(&metadata, &snapshot).unwrap();

        // Encode + decode through the versioned envelope.
        let buf = encode_descriptor(&descriptor).unwrap();
        let decoded = decode_descriptor(&buf).unwrap();
        assert_eq!(decoded.snapshot_id, 42);
        assert_eq!(decoded.namespace, "tpch");
        assert_eq!(decoded.table_name, "region");
        assert_eq!(decoded.data_files.len(), 1);
        assert_eq!(DESCRIPTOR_CODEC_VERSION, 1);

        // Verify PinnedScanTableProvider can be created
        let provider = PinnedScanTableProvider::try_new(decoded).unwrap();
        assert_eq!(provider.descriptor().snapshot_id, 42);

        let mut unsupported: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        unsupported["descriptor"]
            .as_object_mut()
            .unwrap()
            .insert("max_committed_generation".into(), 99.into());
        let error = decode_descriptor(&serde_json::to_vec(&unsupported).unwrap()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unknown field `max_committed_generation`"),
            "removed descriptor fields must fail closed: {error}"
        );
    }

    #[test]
    fn decode_rejects_unknown_codec_version() {
        // A descriptor encoded by a hypothetical future node (version 999).
        let buf = br#"{"version":999,"descriptor":{}}"#;
        let err = decode_descriptor(buf).unwrap_err();
        assert!(
            err.to_string()
                .contains("unsupported SnapshotScanDescriptor codec version 999"),
            "got: {err}"
        );
    }

    #[test]
    fn decode_rejects_envelope_without_version() {
        // Pre-versioning raw descriptor bytes must not be silently accepted.
        let buf = br#"{"snapshot_id":42}"#;
        assert!(decode_descriptor(buf).is_err());
    }
}
