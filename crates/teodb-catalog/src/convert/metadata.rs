//! Iceberg → TeoDB snapshot and table metadata conversions.

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::file::{DataFile, Snapshot, SnapshotOperation, TableMetadata};
use teodb_core::ident::TableIdent;
use teodb_core::schema::{PartitionSpec, SchemaDefinition, SortOrder};

use super::LoadedIcebergDataFile;

use super::TypeLookup;
use super::partition::iceberg_partition_spec_to_teodb;
use super::schema::{iceberg_primitive_to_teodb, iceberg_schema_to_teodb};
use super::sort::iceberg_sort_order_to_teodb;

/// Convert an iceberg `Snapshot` to a TeoDB `Snapshot` using
/// pre-loaded data files. Pass empty slices if data files are not
/// available (e.g. for metadata-only operations).
pub fn iceberg_snapshot_to_teodb(
    snapshot: &iceberg::spec::Snapshot,
    data_files: &[DataFile],
    delete_files: &[DataFile],
) -> Snapshot {
    let summary = snapshot.summary();
    let operation = match summary.operation {
        iceberg::spec::Operation::Append => SnapshotOperation::Append,
        iceberg::spec::Operation::Overwrite => SnapshotOperation::Overwrite,
        iceberg::spec::Operation::Replace => SnapshotOperation::Replace,
        iceberg::spec::Operation::Delete => SnapshotOperation::Delete,
    };

    Snapshot {
        snapshot_id: snapshot.snapshot_id(),
        parent_snapshot_id: snapshot.parent_snapshot_id(),
        sequence_number: snapshot.sequence_number(),
        timestamp_ms: snapshot.timestamp_ms(),
        operation,
        data_files: data_files.to_vec(),
        delete_files: delete_files.to_vec(),
        summary: summary.additional_properties.clone(),
    }
}

/// Convert an `iceberg::spec::TableMetadata` to the internal
/// `teodb_core::file::TableMetadata`.
///
/// `data_files` and `delete_files` are pre-loaded from manifests.
/// Pass empty slices if they are not available.
pub fn iceberg_to_teo_metadata(
    ident: &TableIdent,
    meta: &iceberg::spec::TableMetadata,
    data_files: &[DataFile],
    delete_files: &[DataFile],
) -> TeoDBResult<TableMetadata> {
    let schemas: Vec<SchemaDefinition> = meta
        .schemas_iter()
        .map(|s| iceberg_schema_to_teodb(s))
        .collect::<TeoDBResult<Vec<_>>>()?;

    let partition_specs: Vec<PartitionSpec> = meta
        .partition_specs_iter()
        .map(|s| iceberg_partition_spec_to_teodb(s))
        .collect::<TeoDBResult<Vec<_>>>()?;

    let sort_orders: Vec<SortOrder> = meta
        .sort_orders_iter()
        .map(|s| iceberg_sort_order_to_teodb(s))
        .collect::<TeoDBResult<Vec<_>>>()?;

    let location = super::iceberg_location_to_teodb(meta.location())?;

    let snapshots = meta
        .snapshots()
        .map(|snapshot| iceberg_snapshot_to_teodb(snapshot, &[], &[]))
        .collect();
    let current_snapshot = meta
        .current_snapshot()
        .map(|snap| iceberg_snapshot_to_teodb(snap, data_files, delete_files));

    Ok(TableMetadata {
        table_uuid: meta.uuid(),
        namespace: ident.namespace.clone(),
        table_name: ident.name.clone(),
        table_location: location,
        current_snapshot_id: meta.current_snapshot_id(),
        current_schema_id: meta.current_schema_id(),
        current_partition_spec_id: meta.default_partition_spec_id(),
        current_sort_order_id: meta
            .default_sort_order_id()
            .try_into()
            .map_err(|_| TeoDBError::Catalog("Iceberg sort order id exceeds i32".into()))?,
        schemas,
        partition_specs,
        sort_orders,
        snapshots,
        current_snapshot,
        properties: meta.properties().clone(),
    })
}

pub fn iceberg_data_files_to_teodb(files: &[LoadedIcebergDataFile]) -> TeoDBResult<Vec<DataFile>> {
    files
        .iter()
        .map(|loaded| {
            let type_lookup = build_type_lookup_from_iceberg(loaded.schema.as_ref())?;
            super::data_file::iceberg_data_file_to_teodb(
                &loaded.file,
                loaded.schema_id,
                &type_lookup,
                loaded.partition_spec_id,
                &loaded.partition_spec,
                loaded.schema.as_ref(),
            )
        })
        .collect()
}

/// Build a `TypeLookup` from an iceberg `Schema`.
pub fn build_type_lookup_from_iceberg(schema: &iceberg::spec::Schema) -> TeoDBResult<TypeLookup> {
    use iceberg::spec::Type;
    schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| {
            let dt = match &*f.field_type {
                Type::Primitive(p) => iceberg_primitive_to_teodb(p)?,
                _ => {
                    return Err(TeoDBError::Catalog(format!(
                        "unsupported type for field '{}': {:?}",
                        f.name, f.field_type
                    )));
                }
            };
            Ok((f.id, dt))
        })
        .collect()
}
