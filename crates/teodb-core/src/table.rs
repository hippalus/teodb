use std::collections::HashMap;

use crate::error::{TeoDBError, TeoDBResult};
use crate::ident::TableIdent;
use crate::location::ObjectLocation;
use crate::schema::{PartitionTransform, SchemaDefinition, SortOrder, UnboundPartitionField, UnboundPartitionSpec};
use crate::traits::catalog::CreateTableRequest;

#[derive(Debug, Clone)]
pub struct TableDefinition {
    pub ident: TableIdent,
    pub schema: SchemaDefinition,
    pub location: ObjectLocation,
    pub partition_spec: UnboundPartitionSpec,
    pub sort_order: SortOrder,
    pub properties: HashMap<String, String>,
}

pub struct CreateTableRequestBuilder {
    ident: TableIdent,
    schema: SchemaDefinition,
    location: ObjectLocation,
    partition_spec: UnboundPartitionSpec,
    sort_order: SortOrder,
    properties: HashMap<String, String>,
}

impl CreateTableRequestBuilder {
    pub fn new(ident: TableIdent, schema: SchemaDefinition, location: ObjectLocation) -> Self {
        Self {
            ident,
            schema,
            location,
            partition_spec: UnboundPartitionSpec::unpartitioned(),
            sort_order: SortOrder {
                order_id: 0,
                fields: vec![],
            },
            properties: HashMap::new(),
        }
    }

    pub fn partition_spec(mut self, partition_spec: UnboundPartitionSpec) -> Self {
        self.partition_spec = partition_spec;
        self
    }

    pub fn sort_order(mut self, sort_order: SortOrder) -> Self {
        self.sort_order = sort_order;
        self
    }

    pub fn properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties = properties;
        self
    }

    pub fn table_definition(self) -> TableDefinition {
        TableDefinition {
            ident: self.ident,
            schema: self.schema,
            location: self.location,
            partition_spec: self.partition_spec,
            sort_order: self.sort_order,
            properties: self.properties,
        }
    }

    pub fn build(self) -> TeoDBResult<CreateTableRequest> {
        self.table_definition().into_create_request()
    }
}

impl TableDefinition {
    pub fn into_create_request(self) -> TeoDBResult<CreateTableRequest> {
        Ok(CreateTableRequest {
            ident: self.ident,
            schema: self.schema,
            partition_spec: self.partition_spec,
            sort_order: self.sort_order,
            location: self.location,
            properties: self.properties,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionTransformSpec {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket(u32),
    Truncate(u32),
}

impl PartitionTransformSpec {
    fn to_domain(&self) -> PartitionTransform {
        match self {
            Self::Identity => PartitionTransform::Identity,
            Self::Year => PartitionTransform::Year,
            Self::Month => PartitionTransform::Month,
            Self::Day => PartitionTransform::Day,
            Self::Hour => PartitionTransform::Hour,
            Self::Bucket(n) => PartitionTransform::Bucket { num_buckets: *n },
            Self::Truncate(w) => PartitionTransform::Truncate { width: *w },
        }
    }

    fn default_field_name(&self, column_name: &str) -> String {
        match self {
            Self::Identity => column_name.to_owned(),
            Self::Year => format!("{column_name}_year"),
            Self::Month => format!("{column_name}_month"),
            Self::Day => format!("{column_name}_day"),
            Self::Hour => format!("{column_name}_hour"),
            Self::Bucket(n) => format!("{column_name}_bucket_{n}"),
            Self::Truncate(w) => format!("{column_name}_trunc_{w}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionFieldSpec {
    pub column_name: String,
    pub field_name: Option<String>,
    pub transform: PartitionTransformSpec,
}

impl PartitionFieldSpec {
    pub fn new(column_name: impl Into<String>, transform: PartitionTransformSpec) -> Self {
        Self {
            column_name: column_name.into(),
            field_name: None,
            transform,
        }
    }

    pub fn field_name(mut self, field_name: impl Into<String>) -> Self {
        self.field_name = Some(field_name.into());
        self
    }
}

pub struct PartitionSpecBuilder<'a> {
    schema: &'a SchemaDefinition,
    fields: Vec<PartitionFieldSpec>,
    spec_id: i32,
}

impl<'a> PartitionSpecBuilder<'a> {
    pub fn for_schema(schema: &'a SchemaDefinition) -> Self {
        Self {
            schema,
            fields: Vec::new(),
            spec_id: 0,
        }
    }

    pub fn spec_id(mut self, spec_id: i32) -> Self {
        self.spec_id = spec_id;
        self
    }

    pub fn field(mut self, field: PartitionFieldSpec) -> Self {
        self.fields.push(field);
        self
    }

    pub fn fields(mut self, fields: impl IntoIterator<Item = PartitionFieldSpec>) -> Self {
        self.fields.extend(fields);
        self
    }

    pub fn build(self) -> TeoDBResult<UnboundPartitionSpec> {
        let fields = self
            .fields
            .into_iter()
            .map(|field| {
                let column = self
                    .schema
                    .by_name(&field.column_name)
                    .ok_or_else(|| TeoDBError::InvalidArgument {
                        field: "partition_by".into(),
                        message: format!("partition column '{}' not found in table schema", field.column_name),
                    })?;
                let field_name = field.field_name.unwrap_or_else(|| {
                    field
                        .transform
                        .default_field_name(&field.column_name)
                });

                Ok(UnboundPartitionField {
                    source_id: column.id,
                    field_id: None,
                    name: field_name,
                    transform: field.transform.to_domain(),
                })
            })
            .collect::<TeoDBResult<Vec<_>>>()?;

        Ok(UnboundPartitionSpec {
            spec_id: Some(self.spec_id),
            fields,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::{ColumnMeta, TeoDataType};

    use super::*;

    fn schema() -> SchemaDefinition {
        SchemaDefinition {
            schema_id: 0,
            columns: vec![ColumnMeta {
                id: 7,
                name: "created_at".into(),
                data_type: TeoDataType::TimestampMicros { tz: Some("UTC".into()) },
                nullable: false,
                doc: None,
            }],
            identifier_field_ids: vec![],
        }
    }

    #[test]
    fn partition_builder_generates_transform_names() {
        let spec = PartitionSpecBuilder::for_schema(&schema())
            .field(PartitionFieldSpec::new("created_at", PartitionTransformSpec::Day))
            .build()
            .unwrap();

        assert_eq!(spec.fields.len(), 1);
    }

    #[test]
    fn create_table_builder_converts_schema() {
        let location = ObjectLocation::parse("s3://warehouse/ns/events").unwrap();
        let request = CreateTableRequestBuilder::new(TableIdent::new("ns", "events"), schema(), location)
            .build()
            .unwrap();

        assert_eq!(request.ident, TableIdent::new("ns", "events"));
        assert_eq!(request.schema.schema_id, 0);
    }
}
