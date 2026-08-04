use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_execution::object_store::ObjectStoreUrl;

use teodb_core::file::DataFile;
use teodb_core::location::ObjectLocation;

pub(super) struct ParquetScanBuilder {
    table_location: ObjectLocation,
    arrow_schema: SchemaRef,
    files: Vec<DataFile>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
}

impl ParquetScanBuilder {
    pub(super) fn new(table_location: ObjectLocation, arrow_schema: SchemaRef) -> Self {
        Self {
            table_location,
            arrow_schema,
            files: Vec::new(),
            projection: None,
            limit: None,
        }
    }

    pub(super) fn files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.files.extend(files);
        self
    }

    pub(super) fn projection(mut self, projection: Option<&Vec<usize>>) -> Self {
        self.projection = projection.cloned();
        self
    }

    pub(super) fn limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub(super) fn build(self) -> DFResult<Arc<dyn ExecutionPlan>> {
        let store_url = object_store_url_from_location(&self.table_location)?;
        let partitioned_files: Vec<PartitionedFile> = self
            .files
            .iter()
            .map(|file| PartitionedFile::new(file.path.key.clone(), file.file_size_bytes))
            .collect();

        let source = Arc::new(ParquetSource::new(self.arrow_schema));
        let mut builder = FileScanConfigBuilder::new(store_url, source)
            .with_file_groups(vec![partitioned_files.into()])
            .with_limit(self.limit);

        if let Some(projection) = self.projection {
            builder = builder
                .with_projection_indices(Some(projection))
                .map_err(|error| datafusion::error::DataFusionError::Plan(format!("projection error: {error}")))?;
        }

        Ok(DataSourceExec::from_data_source(builder.build()))
    }
}

pub(super) struct SnapshotScanBuilder<'a> {
    table_location: ObjectLocation,
    arrow_schema: SchemaRef,
    files: Vec<DataFile>,
    delete_set: Option<&'a super::delete::PositionDeleteSet>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
}

impl<'a> SnapshotScanBuilder<'a> {
    pub(super) fn new(table_location: ObjectLocation, arrow_schema: SchemaRef) -> Self {
        Self {
            table_location,
            arrow_schema,
            files: Vec::new(),
            delete_set: None,
            projection: None,
            limit: None,
        }
    }

    pub(super) fn files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.files.extend(files);
        self
    }

    pub(super) fn delete_set(mut self, delete_set: Option<&'a super::delete::PositionDeleteSet>) -> Self {
        self.delete_set = delete_set;
        self
    }

    pub(super) fn projection(mut self, projection: Option<&Vec<usize>>) -> Self {
        self.projection = projection.cloned();
        self
    }

    pub(super) fn limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub(super) fn build(self) -> DFResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = projected_schema(&self.arrow_schema, self.projection.as_ref())?;
        let (clean_files, deleted_files) = split_by_deletes(self.files, self.delete_set);
        let mut branches = Vec::new();

        if !clean_files.is_empty() {
            branches.push(
                ParquetScanBuilder::new(self.table_location.clone(), self.arrow_schema.clone())
                    .files(clean_files)
                    .projection(self.projection.as_ref())
                    .limit(self.limit)
                    .build()?,
            );
        }
        for (file, positions) in deleted_files {
            let scan = ParquetScanBuilder::new(self.table_location.clone(), self.arrow_schema.clone())
                .files(std::iter::once(file))
                .projection(self.projection.as_ref())
                .limit(None)
                .build()?;
            branches.push(Arc::new(super::delete::PositionDeleteFilterExec::new(
                scan,
                super::delete::DeletePositions { positions },
            )) as Arc<dyn ExecutionPlan>);
        }

        match branches.len() {
            0 => Ok(Arc::new(EmptyExec::new(projected_schema))),
            1 => Ok(branches.remove(0)),
            _ => UnionExec::try_new(branches),
        }
    }
}

pub(super) fn split_by_deletes(
    files: Vec<DataFile>,
    delete_set: Option<&super::delete::PositionDeleteSet>,
) -> (Vec<DataFile>, Vec<(DataFile, std::collections::HashSet<i64>)>) {
    let Some(set) = delete_set else {
        return (files, Vec::new());
    };

    let mut clean = Vec::new();
    let mut deleted = Vec::new();
    for file in files {
        let positions = set.positions_for_file(&file.path.key);
        if positions.is_empty() {
            clean.push(file);
        } else {
            deleted.push((file, positions));
        }
    }
    (clean, deleted)
}

fn projected_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> DFResult<SchemaRef> {
    match projection {
        Some(indices) => Ok(Arc::new(schema.project(indices)?)),
        None => Ok(schema.clone()),
    }
}

fn object_store_url_from_location(loc: &ObjectLocation) -> DFResult<ObjectStoreUrl> {
    let url_str = loc.scheme.url_prefix(loc.bucket.as_deref());
    ObjectStoreUrl::parse(&url_str)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use teodb_core::file::{DataContent, FileFormat};
    use teodb_core::location::{ObjectLocation, StorageScheme};

    use super::*;

    fn data_file(path: &str) -> DataFile {
        DataFile {
            content: DataContent::Data,
            path: ObjectLocation {
                scheme: StorageScheme::Local,
                bucket: None,
                key: path.into(),
            },
            format: FileFormat::Parquet,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            schema_id: 0,
            partition_values: HashMap::new(),
            record_count: 10,
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
        }
    }

    #[test]
    fn snapshot_scan_wraps_position_deleted_files() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "event_id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let mut delete_set = super::super::delete::PositionDeleteSet::new();
        delete_set.insert_for_test("data/events/part-0.parquet", 3);

        let plan = SnapshotScanBuilder::new(
            ObjectLocation {
                scheme: StorageScheme::Local,
                bucket: None,
                key: "data/events".into(),
            },
            schema,
        )
        .files(vec![data_file("data/events/part-0.parquet")])
        .delete_set(Some(&delete_set))
        .limit(Some(10))
        .build()
        .unwrap();

        assert_eq!(plan.name(), "PositionDeleteFilterExec");
    }
}
