//! Write specification — everything a Parquet write needs to know about
//! the table: schema, partition/sort context, codec, and sizing targets.

use std::collections::HashMap;

use arrow_schema::SchemaRef;
use parquet::file::properties::WriterProperties;
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::{FieldId, Generation};
use teodb_core::scalar::TeoScalar;
use teodb_core::schema::SortOrder;

use super::compression::CompressionCodec;

/// Specification for a Parquet write operation.
///
/// Fields are crate-private and `WriteSpec` has no public `Default`: outside
/// `teodb-storage`, a spec can only be built through [`WriteSpecBuilder`],
/// which enforces that required write metadata (`schema_id`,
/// `partition_spec_id`) is supplied.
#[derive(Debug, Clone)]
pub struct WriteSpec {
    /// Arrow schema with `PARQUET: field_id` metadata on every field.
    pub(crate) schema: SchemaRef,
    pub(crate) schema_id: i32,
    pub(crate) partition_spec_id: i32,
    pub(crate) partition_values: HashMap<FieldId, TeoScalar>,
    pub(crate) sort_order: SortOrder,
    /// Compression codec for output files (default: ZSTD level 3).
    pub(crate) compression: CompressionCodec,
    pub(crate) row_group_target_bytes: u64,
    pub(crate) row_group_target_rows: u64,
    /// WAL generation range of the input batches.
    pub(crate) generation_lo: Generation,
    pub(crate) generation_hi: Generation,
}

/// Test-only `Default` so in-crate tests can construct specs concisely with
/// struct-update syntax. Production and other crates must use
/// [`WriteSpecBuilder`].
#[cfg(test)]
impl Default for WriteSpec {
    fn default() -> Self {
        Self {
            schema: std::sync::Arc::new(arrow_schema::Schema::empty()),
            schema_id: 0,
            partition_spec_id: 0,
            partition_values: HashMap::new(),
            sort_order: SortOrder {
                order_id: 0,
                fields: vec![],
            },
            compression: CompressionCodec::default(),
            row_group_target_bytes: 128 * 1024 * 1024,
            row_group_target_rows: 8 * 1024 * 1024,
            generation_lo: 0,
            generation_hi: 0,
        }
    }
}

impl WriteSpec {
    pub fn builder(schema: SchemaRef) -> WriteSpecBuilder {
        WriteSpecBuilder::new(schema)
    }

    /// Express this spec as parquet `WriterProperties`, embedding TeoDB
    /// metadata (schema/spec ids, generation range, sort order) in the
    /// file footer.
    pub(super) fn writer_properties(&self) -> TeoDBResult<WriterProperties> {
        let compression = self.compression.to_parquet()?;

        let mut builder = WriterProperties::builder()
            .set_compression(compression)
            .set_dictionary_enabled(true)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
            .set_max_row_group_row_count(Some(self.row_group_target_rows as usize))
            .set_data_page_size_limit(1024 * 1024)
            .set_write_batch_size(8192)
            .set_bloom_filter_enabled(true)
            .set_bloom_filter_fpp(0.01);

        // Embed TeoDB metadata in the file footer.
        let mut kv = vec![
            parquet::file::metadata::KeyValue::new("teodb.schema_id".into(), self.schema_id.to_string()),
            parquet::file::metadata::KeyValue::new(
                "teodb.partition_spec_id".into(),
                self.partition_spec_id.to_string(),
            ),
            parquet::file::metadata::KeyValue::new("teodb.generation_lo".into(), self.generation_lo.to_string()),
            parquet::file::metadata::KeyValue::new("teodb.generation_hi".into(), self.generation_hi.to_string()),
        ];
        if let Ok(sort_json) = serde_json::to_string(&self.sort_order) {
            kv.push(parquet::file::metadata::KeyValue::new(
                "teodb.sort_order".into(),
                sort_json,
            ));
        }

        builder = builder.set_key_value_metadata(Some(kv));
        Ok(builder.build())
    }
}

pub struct WriteSpecBuilder {
    schema: SchemaRef,
    schema_id: Option<i32>,
    partition_spec_id: Option<i32>,
    partition_values: HashMap<FieldId, TeoScalar>,
    sort_order: SortOrder,
    compression: CompressionCodec,
    row_group_target_bytes: u64,
    row_group_target_rows: u64,
    generation_lo: Generation,
    generation_hi: Generation,
}

impl WriteSpecBuilder {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            schema_id: None,
            partition_spec_id: None,
            partition_values: HashMap::new(),
            sort_order: SortOrder {
                order_id: 0,
                fields: vec![],
            },
            compression: CompressionCodec::default(),
            row_group_target_bytes: 128 * 1024 * 1024,
            row_group_target_rows: 8 * 1024 * 1024,
            generation_lo: 0,
            generation_hi: 0,
        }
    }

    pub fn schema_id(mut self, schema_id: i32) -> Self {
        self.schema_id = Some(schema_id);
        self
    }

    pub fn partition_spec_id(mut self, partition_spec_id: i32) -> Self {
        self.partition_spec_id = Some(partition_spec_id);
        self
    }

    pub fn partition_values(mut self, partition_values: HashMap<FieldId, TeoScalar>) -> Self {
        self.partition_values = partition_values;
        self
    }

    pub fn sort_order(mut self, sort_order: SortOrder) -> Self {
        self.sort_order = sort_order;
        self
    }

    pub fn compression(mut self, compression: CompressionCodec) -> Self {
        self.compression = compression;
        self
    }

    pub fn row_group_target_bytes(mut self, row_group_target_bytes: u64) -> Self {
        self.row_group_target_bytes = row_group_target_bytes;
        self
    }

    pub fn row_group_target_rows(mut self, row_group_target_rows: u64) -> Self {
        self.row_group_target_rows = row_group_target_rows;
        self
    }

    pub fn generation_range(mut self, generation_lo: Generation, generation_hi: Generation) -> Self {
        self.generation_lo = generation_lo;
        self.generation_hi = generation_hi;
        self
    }

    pub fn build(self) -> TeoDBResult<WriteSpec> {
        let schema_id = self
            .schema_id
            .ok_or_else(|| TeoDBError::Config("write spec schema_id is required".into()))?;
        let partition_spec_id = self
            .partition_spec_id
            .ok_or_else(|| TeoDBError::Config("write spec partition_spec_id is required".into()))?;

        Ok(WriteSpec {
            schema: self.schema,
            schema_id,
            partition_spec_id,
            partition_values: self.partition_values,
            sort_order: self.sort_order,
            compression: self.compression,
            row_group_target_bytes: self.row_group_target_bytes,
            row_group_target_rows: self.row_group_target_rows,
            generation_lo: self.generation_lo,
            generation_hi: self.generation_hi,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    #[test]
    fn builder_requires_schema_id() {
        let error = WriteSpec::builder(schema())
            .partition_spec_id(0)
            .build()
            .expect_err("missing schema_id must fail");
        assert!(matches!(error, TeoDBError::Config(msg) if msg.contains("schema_id")));
    }

    #[test]
    fn builder_requires_partition_spec_id() {
        let error = WriteSpec::builder(schema())
            .schema_id(0)
            .build()
            .expect_err("missing partition_spec_id must fail");
        assert!(matches!(error, TeoDBError::Config(msg) if msg.contains("partition_spec_id")));
    }

    #[test]
    fn builder_builds_with_required_metadata() {
        let spec = WriteSpec::builder(schema())
            .schema_id(3)
            .partition_spec_id(7)
            .build()
            .expect("valid spec");
        assert_eq!(spec.schema_id, 3);
        assert_eq!(spec.partition_spec_id, 7);
    }
}
