use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use teodb_core::error::TeoDBResult;
use teodb_core::file::{DataFile, TableMetadata};
use teodb_core::location::ObjectLocation;
use teodb_core::schema::SortOrder;
use teodb_core::traits::storage::StorageFactory;
use teodb_core::write_protocol::{CommitId, GenerationRange, WriterId};

use super::partitioning::partition_batches;

#[derive(Debug, Clone, Copy)]
pub(super) struct FlushWriteContext {
    pub(super) writer_id: WriterId,
    pub(super) commit_id: CommitId,
    pub(super) generations: GenerationRange,
}

#[tracing::instrument(
    name = "ingest.flush_write",
    skip_all,
    fields(commit_id = %context.commit_id, batch_count = batches.len())
)]
pub(super) async fn write_flush_data_files(
    storage_factory: &dyn StorageFactory,
    metadata: &TableMetadata,
    batches: Vec<RecordBatch>,
    context: FlushWriteContext,
) -> TeoDBResult<Vec<DataFile>> {
    let schema_def = metadata.current_schema()?;
    let partition_spec = metadata.current_partition_spec()?;
    let arrow_schema = Arc::new(teodb_storage::schema_to_arrow(schema_def));
    let sort_order = current_sort_order(metadata);
    let (storage, _path) = storage_factory
        .resolve(&metadata.table_location)
        .await?;

    if partition_spec.fields.is_empty() {
        let target = flush_file_location(metadata, context.writer_id, context.commit_id, None);
        let spec = teodb_storage::parquet::WriteSpec::builder(arrow_schema)
            .schema_id(metadata.current_schema_id)
            .partition_spec_id(partition_spec.spec_id)
            .sort_order(sort_order)
            .generation_range(context.generations.lo, context.generations.hi)
            .build()?;
        return teodb_storage::parquet::write_sorted_rolled(&*storage, &target, batches, &spec).await;
    }

    let partitioned = partition_batches(&batches, schema_def, partition_spec)?;
    let mut data_files = Vec::with_capacity(partitioned.len());
    for (idx, group) in partitioned.into_iter().enumerate() {
        let partition_path =
            teodb_catalog::iceberg_partition_path(schema_def, partition_spec, &group.partition_values)?;
        let target = flush_file_location(
            metadata,
            context.writer_id,
            context.commit_id,
            Some((&partition_path, idx)),
        );
        let spec = teodb_storage::parquet::WriteSpec::builder(arrow_schema.clone())
            .schema_id(metadata.current_schema_id)
            .partition_spec_id(partition_spec.spec_id)
            .partition_values(group.partition_values)
            .sort_order(sort_order.clone())
            .generation_range(context.generations.lo, context.generations.hi)
            .build()?;
        let files = teodb_storage::parquet::write_sorted_rolled(&*storage, &target, group.batches, &spec).await?;
        data_files.extend(files);
    }

    Ok(data_files)
}

fn current_sort_order(metadata: &TableMetadata) -> SortOrder {
    metadata
        .sort_orders
        .iter()
        .find(|s| s.order_id == metadata.current_sort_order_id)
        .cloned()
        .unwrap_or(SortOrder {
            order_id: 0,
            fields: vec![],
        })
}

fn flush_file_location(
    metadata: &TableMetadata,
    writer_id: WriterId,
    commit_id: CommitId,
    partition: Option<(&str, usize)>,
) -> ObjectLocation {
    let file_name = match partition {
        Some((_, idx)) => format!("{commit_id}-p{idx:04}-f0000.parquet"),
        None => format!("{commit_id}-f0000.parquet"),
    };
    let data_dir = match partition {
        Some((path, _)) => format!("data/{path}/{writer_id}"),
        None => format!("data/{writer_id}"),
    };
    ObjectLocation {
        scheme: metadata.table_location.scheme,
        bucket: metadata.table_location.bucket.clone(),
        key: format!(
            "{}/{data_dir}/{file_name}",
            metadata.table_location.key.trim_end_matches('/'),
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use teodb_core::location::StorageScheme;
    use teodb_core::scalar::TeoScalar;
    use teodb_core::schema::{
        ColumnMeta, PartitionField, PartitionSpec, PartitionTransform, SchemaDefinition, TeoDataType,
    };
    use teodb_core::write_protocol::{ClusterId, WriterSlot};

    use super::*;

    #[test]
    fn partitioned_flush_target_nests_encoded_partition_before_writer() {
        let schema = SchemaDefinition {
            schema_id: 0,
            columns: vec![ColumnMeta {
                id: 1,
                name: "region".into(),
                data_type: TeoDataType::Utf8,
                nullable: false,
                doc: None,
            }],
            identifier_field_ids: vec![],
        };
        let partition_spec = PartitionSpec {
            spec_id: 0,
            fields: vec![PartitionField {
                source_id: 1,
                field_id: 1000,
                name: "region".into(),
                transform: PartitionTransform::Identity,
            }],
        };
        let metadata = TableMetadata {
            table_uuid: uuid::Uuid::now_v7(),
            namespace: "test".into(),
            table_name: "events".into(),
            table_location: ObjectLocation {
                scheme: StorageScheme::S3,
                bucket: Some("warehouse".into()),
                key: "test/events".into(),
            },
            current_snapshot_id: None,
            current_schema_id: 0,
            current_partition_spec_id: 0,
            current_sort_order_id: 0,
            schemas: vec![schema.clone()],
            partition_specs: vec![partition_spec.clone()],
            sort_orders: vec![],
            snapshots: vec![],
            current_snapshot: None,
            properties: HashMap::new(),
        };
        let partition_path = teodb_catalog::iceberg_partition_path(
            &schema,
            &partition_spec,
            &HashMap::from([(1000, TeoScalar::Utf8("eu/west".into()))]),
        )
        .unwrap();
        let writer_id = WriterId::derive(
            ClusterId::from_uuid(uuid::Uuid::from_u128(1)),
            &WriterSlot::new("writer-a").unwrap(),
        );
        let commit_id = CommitId::from_uuid(uuid::Uuid::from_u128(2));

        let target = flush_file_location(&metadata, writer_id, commit_id, Some((&partition_path, 0)));

        assert_eq!(
            target.key,
            format!("test/events/data/region=eu%2Fwest/{writer_id}/{commit_id}-p0000-f0000.parquet")
        );
    }
}
