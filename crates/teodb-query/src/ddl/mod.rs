//! SQL DDL support for TeoDB.
//!
//! Parses SQL statements using `sqlparser`, classifies them as DDL or DQL,
//! and executes DDL operations against the TeoDB catalog. DQL statements
//! are delegated to DataFusion's `SessionContext::sql()`.
//!
//! Structured as:
//! - `classify`  — SQL statement classification (DDL vs DQL)
//! - `plan`      — typed plan structs for each DDL operation
//! - `create`    — CREATE TABLE/SCHEMA parsing
//! - `drop`      — DROP TABLE/SCHEMA parsing
//! - `show`      — SHOW TABLES/COLUMNS, DESCRIBE parsing
//! - `partition` — PARTITION BY transform-expression parsing
//! - `sql_types` — SQL → TeoDB data type mapping
//! - `idents`    — shared identifier/name resolution
//! - `executor`  — plan execution against the catalog
//! - `types`     — result types returned to callers

mod classify;
mod create;
mod drop;
mod executor;
mod idents;
mod partition;
mod plan;
mod show;
mod sql_types;
mod types;

pub use classify::{SqlStatement, classify_sql};
pub use executor::DdlExecutor;
pub use plan::{
    CreateSchemaPlan, CreateTablePlan, DdlPlan, DescribeTablePlan, DropSchemaPlan, DropTablePlan, PartitionFieldDef,
    PartitionTransformDef, ShowTablesPlan,
};
pub use types::{DdlResult, DdlRow};
