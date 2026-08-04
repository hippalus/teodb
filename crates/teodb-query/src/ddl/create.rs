//! CREATE TABLE / CREATE SCHEMA parsing.

use sqlparser::ast::{ColumnOption, SchemaName, Statement};

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::schema::ColumnMeta;

use super::idents::{object_name_to_string, resolve_table_name};
use super::partition::{parse_hive_partition, parse_partition_by};
use super::plan::{CreateSchemaPlan, CreateTablePlan, DdlPlan};
use super::sql_types::sql_type_to_teo;

/// Parse a `CREATE TABLE` statement into a `DdlPlan`.
pub fn parse_create_table(stmt: &Statement) -> TeoDBResult<DdlPlan> {
    let Statement::CreateTable(ct) = stmt else {
        return Err(TeoDBError::Internal("expected CREATE TABLE statement".into()));
    };

    let (ns, table_name) = resolve_table_name(&ct.name)?;
    let columns = convert_columns(&ct.columns)?;

    // Support both `PARTITION BY (expr)` and Hive-style `PARTITIONED BY (col ...)`.
    let partition_by = if let Some(ref expr) = ct.partition_by {
        parse_partition_by(&Some(expr.clone()))?
    } else {
        parse_hive_partition(&ct.hive_distribution)?
    };

    Ok(DdlPlan::CreateTable(CreateTablePlan {
        namespace: ns,
        table_name,
        columns,
        partition_by,
        if_not_exists: ct.if_not_exists,
    }))
}

/// Parse a `CREATE SCHEMA` or `CREATE DATABASE` statement into a `DdlPlan`.
pub fn parse_create_schema(stmt: &Statement) -> TeoDBResult<DdlPlan> {
    match stmt {
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
            ..
        } => {
            let ns = match schema_name {
                SchemaName::Simple(name) => object_name_to_string(name),
                SchemaName::UnnamedAuthorization(ident) => ident.value.clone(),
                SchemaName::NamedAuthorization(name, _) => object_name_to_string(name),
            };
            Ok(DdlPlan::CreateSchema(CreateSchemaPlan {
                namespace: ns,
                if_not_exists: *if_not_exists,
            }))
        }
        Statement::CreateDatabase {
            db_name, if_not_exists, ..
        } => {
            let ns = object_name_to_string(db_name);
            Ok(DdlPlan::CreateSchema(CreateSchemaPlan {
                namespace: ns,
                if_not_exists: *if_not_exists,
            }))
        }
        _ => Err(TeoDBError::Internal("expected CREATE SCHEMA/DATABASE".into())),
    }
}

/// Convert sqlparser column definitions to TeoDB column metadata.
fn convert_columns(columns: &[sqlparser::ast::ColumnDef]) -> TeoDBResult<Vec<ColumnMeta>> {
    columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let data_type = sql_type_to_teo(&col.data_type)?;
            let nullable = !col
                .options
                .iter()
                .any(|o| matches!(o.option, ColumnOption::NotNull));
            Ok(ColumnMeta {
                id: (i + 1) as i32,
                name: col.name.value.clone(),
                data_type,
                nullable,
                doc: None,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::plan::PartitionTransformDef;

    /// End-to-end test: parse CREATE TABLE SQL into the catalog-neutral schema.
    #[test]
    fn create_table_builds_domain_schema() {
        let sql = "CREATE TABLE IF NOT EXISTS tpch.region (
            r_regionkey INTEGER NOT NULL,
            r_name VARCHAR(25) NOT NULL,
            r_comment VARCHAR(152)
        )";
        let result = super::super::classify::classify_sql(sql).unwrap();
        let plan = match result {
            super::super::classify::SqlStatement::Ddl(super::super::plan::DdlPlan::CreateTable(p)) => p,
            other => panic!("expected CreateTable, got {other:?}"),
        };
        assert_eq!(plan.namespace, "tpch");
        assert_eq!(plan.table_name, "region");
        assert!(plan.if_not_exists);
        assert_eq!(plan.columns.len(), 3);

        let schema_def = teodb_core::schema::SchemaDefinition {
            schema_id: 0,
            columns: plan.columns,
            identifier_field_ids: vec![],
        };
        assert_eq!(schema_def.columns.len(), 3);
        assert_eq!(schema_def.columns[0].name, "r_regionkey");
        assert_eq!(schema_def.columns[0].data_type, teodb_core::schema::TeoDataType::Int32);
        assert!(!schema_def.columns[0].nullable);
        assert!(schema_def.columns[2].nullable);
    }

    #[test]
    fn partitioned_by_hive_syntax() {
        let sql = "CREATE TABLE perf.events (
            event_id VARCHAR NOT NULL,
            region VARCHAR NOT NULL
        ) PARTITIONED BY (region)";
        let result = super::super::classify::classify_sql(sql).unwrap();
        let plan = match result {
            super::super::classify::SqlStatement::Ddl(super::super::plan::DdlPlan::CreateTable(p)) => p,
            other => panic!("expected CreateTable, got {other:?}"),
        };
        // Verify partition fields
        assert_eq!(plan.partition_by.len(), 1);
        assert_eq!(plan.partition_by[0].column_name, "region");
        assert_eq!(plan.partition_by[0].transform, PartitionTransformDef::Identity);
        // Verify region is still in the column list (needed for partition spec resolution)
        assert!(
            plan.columns.iter().any(|c| c.name == "region"),
            "region must be in columns for partition spec resolution, got: {:?}",
            plan.columns
                .iter()
                .map(|c| &c.name)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn partition_by_standard_syntax() {
        let sql = "CREATE TABLE perf.events (
            event_id VARCHAR NOT NULL,
            region VARCHAR NOT NULL
        ) PARTITION BY (region)";
        let result = super::super::classify::classify_sql(sql).unwrap();
        let plan = match result {
            super::super::classify::SqlStatement::Ddl(super::super::plan::DdlPlan::CreateTable(p)) => p,
            other => panic!("expected CreateTable, got {other:?}"),
        };
        assert_eq!(plan.partition_by.len(), 1);
        assert_eq!(plan.partition_by[0].column_name, "region");
        assert_eq!(plan.partition_by[0].transform, PartitionTransformDef::Identity);
    }

    /// End-to-end: PARTITIONED BY → partition spec with resolved field IDs.
    #[test]
    fn partitioned_by_builds_valid_spec() {
        let sql = "CREATE TABLE perf.events (
            event_id VARCHAR NOT NULL,
            region VARCHAR NOT NULL
        ) PARTITIONED BY (region)";
        let result = super::super::classify::classify_sql(sql).unwrap();
        let plan = match result {
            super::super::classify::SqlStatement::Ddl(super::super::plan::DdlPlan::CreateTable(p)) => p,
            other => panic!("expected CreateTable, got {other:?}"),
        };
        let schema_def = teodb_core::schema::SchemaDefinition {
            schema_id: 0,
            columns: plan.columns,
            identifier_field_ids: vec![],
        };
        let spec = teodb_core::table::PartitionSpecBuilder::for_schema(&schema_def)
            .fields(super::super::executor::partition_field_specs(&plan.partition_by))
            .build();
        assert!(spec.is_ok(), "partition spec build failed: {:?}", spec.err());
        let spec = spec.unwrap();
        assert_eq!(spec.fields.len(), 1, "expected 1 partition field");
    }
}
