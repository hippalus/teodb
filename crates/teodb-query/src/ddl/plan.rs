//! Typed DDL plan structures.
//!
//! Each DDL operation is parsed into a concrete plan struct before execution.
//! This separates parsing (SQL → Plan) from execution (Plan → catalog ops).

use teodb_core::ident::TableIdent;
use teodb_core::schema::ColumnMeta;
use teodb_core::traits::catalog::DropTableOptions;

/// A parsed DDL plan ready for execution.
#[derive(Debug, Clone)]
pub enum DdlPlan {
    CreateTable(CreateTablePlan),
    CreateSchema(CreateSchemaPlan),
    DropTable(DropTablePlan),
    DropSchema(DropSchemaPlan),
    ShowTables(ShowTablesPlan),
    ShowColumns(DescribeTablePlan),
    DescribeTable(DescribeTablePlan),
}

/// Partition field definition parsed from SQL or REST API.
#[derive(Debug, Clone)]
pub struct PartitionFieldDef {
    /// Source column name to partition on.
    pub column_name: String,
    /// Iceberg partition transform to apply.
    pub transform: PartitionTransformDef,
}

/// Partition transform parsed from SQL or REST API.
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionTransformDef {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket(u32),
    Truncate(u32),
}

/// Plan for `CREATE TABLE [IF NOT EXISTS] <schema>.<table> (...)`.
#[derive(Debug, Clone)]
pub struct CreateTablePlan {
    pub namespace: String,
    pub table_name: String,
    pub columns: Vec<ColumnMeta>,
    pub partition_by: Vec<PartitionFieldDef>,
    pub if_not_exists: bool,
}

/// Plan for `CREATE SCHEMA|DATABASE [IF NOT EXISTS] <name>`.
#[derive(Debug, Clone)]
pub struct CreateSchemaPlan {
    pub namespace: String,
    pub if_not_exists: bool,
}

/// Plan for `DROP TABLE [IF EXISTS] <schema>.<table>`.
#[derive(Debug, Clone)]
pub struct DropTablePlan {
    pub ident: TableIdent,
    pub if_exists: bool,
    pub options: DropTableOptions,
}

/// Plan for `DROP SCHEMA|DATABASE [IF EXISTS] <name>`.
#[derive(Debug, Clone)]
pub struct DropSchemaPlan {
    pub namespace: String,
    pub if_exists: bool,
}

/// Plan for `SHOW TABLES [IN <namespace>]`.
#[derive(Debug, Clone)]
pub struct ShowTablesPlan {
    pub namespace: Option<String>,
}

/// Plan for `SHOW COLUMNS FROM <table>` or `DESCRIBE <table>`.
#[derive(Debug, Clone)]
pub struct DescribeTablePlan {
    pub namespace: String,
    pub table_name: String,
}
