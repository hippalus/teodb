//! SQL statement classification using `sqlparser`.
//!
//! Parses SQL text and classifies it as DDL (handled by TeoDB catalog)
//! or DQL/DML (delegated to DataFusion).

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use tracing::debug;

use teodb_core::error::{TeoDBError, TeoDBResult};

use super::plan::DdlPlan;

/// Classified SQL statement.
#[derive(Debug)]
pub enum SqlStatement {
    /// DDL or metadata statement handled by TeoDB directly.
    Ddl(DdlPlan),
    /// DQL/DML statement delegated to DataFusion.
    Query(String),
}

/// Parse and classify a SQL string.
///
/// Returns `Ddl` for CREATE/DROP/ALTER/SHOW/DESCRIBE statements,
/// `Query` for everything else (SELECT, INSERT, EXPLAIN, etc.).
pub fn classify_sql(sql: &str) -> TeoDBResult<SqlStatement> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| TeoDBError::InvalidArgument {
        field: "sql".into(),
        message: format!("SQL parse error: {e}"),
    })?;

    if statements.is_empty() {
        return Err(TeoDBError::InvalidArgument {
            field: "sql".into(),
            message: "empty SQL statement".into(),
        });
    }

    if statements.len() > 1 {
        return Err(TeoDBError::InvalidArgument {
            field: "sql".into(),
            message: "only single statements are supported".into(),
        });
    }

    let stmt = statements
        .into_iter()
        .next()
        .ok_or_else(|| TeoDBError::InvalidArgument {
            field: "sql".into(),
            message: "empty SQL statement".into(),
        })?;

    let result = match &stmt {
        Statement::CreateTable { .. } => super::create::parse_create_table(&stmt).map(SqlStatement::Ddl),
        Statement::CreateSchema { .. } | Statement::CreateDatabase { .. } => {
            super::create::parse_create_schema(&stmt).map(SqlStatement::Ddl)
        }
        Statement::Drop { .. } => super::drop::parse_drop(&stmt).map(SqlStatement::Ddl),
        Statement::ShowTables { .. } => super::show::parse_show_tables(&stmt).map(SqlStatement::Ddl),
        Statement::ShowColumns { .. } => super::show::parse_show_columns(&stmt).map(SqlStatement::Ddl),
        Statement::ExplainTable { .. } => super::show::parse_explain_table(&stmt).map(SqlStatement::Ddl),
        _ => Ok(SqlStatement::Query(sql.to_owned())),
    };

    if let Ok(ref classified) = result {
        match classified {
            SqlStatement::Ddl(plan) => {
                debug!(sql = %sql, plan = ?std::mem::discriminant(plan), "SQL classified as DDL")
            }
            SqlStatement::Query(_) => debug!(sql = %sql, "SQL classified as DQL/DML"),
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_select_as_query() {
        let result = classify_sql("SELECT 1").unwrap();
        assert!(matches!(result, SqlStatement::Query(_)));
    }

    #[test]
    fn classify_create_table_as_ddl() {
        let result = classify_sql("CREATE TABLE tpch.region (r_regionkey INTEGER, r_name VARCHAR)").unwrap();
        assert!(matches!(result, SqlStatement::Ddl(DdlPlan::CreateTable(_))));
    }

    #[test]
    fn classify_create_schema_as_ddl() {
        let result = classify_sql("CREATE SCHEMA tpch").unwrap();
        assert!(matches!(result, SqlStatement::Ddl(DdlPlan::CreateSchema(_))));
    }

    #[test]
    fn classify_drop_table_as_ddl() {
        let result = classify_sql("DROP TABLE tpch.region").unwrap();
        assert!(matches!(result, SqlStatement::Ddl(DdlPlan::DropTable(_))));
    }

    #[test]
    fn classify_drop_schema_as_ddl() {
        let result = classify_sql("DROP SCHEMA tpch").unwrap();
        assert!(matches!(result, SqlStatement::Ddl(DdlPlan::DropSchema(_))));
    }

    #[test]
    fn classify_show_tables_as_ddl() {
        let result = classify_sql("SHOW TABLES").unwrap();
        assert!(matches!(result, SqlStatement::Ddl(DdlPlan::ShowTables(_))));
    }

    #[test]
    fn classify_show_columns_as_ddl() {
        let result = classify_sql("SHOW COLUMNS FROM tpch.region").unwrap();
        assert!(matches!(result, SqlStatement::Ddl(DdlPlan::ShowColumns(_))));
    }

    #[test]
    fn classify_describe_as_ddl() {
        let result = classify_sql("DESCRIBE tpch.region").unwrap();
        assert!(matches!(result, SqlStatement::Ddl(DdlPlan::DescribeTable(_))));
    }

    #[test]
    fn reject_empty_sql() {
        assert!(classify_sql("").is_err());
    }

    #[test]
    fn reject_multiple_statements() {
        assert!(classify_sql("SELECT 1; SELECT 2").is_err());
    }

    #[test]
    fn classify_insert_as_query() {
        let result = classify_sql("INSERT INTO t VALUES (1, 'a')").unwrap();
        assert!(matches!(result, SqlStatement::Query(_)));
    }

    #[test]
    fn create_table_if_not_exists() {
        let result = classify_sql("CREATE TABLE IF NOT EXISTS tpch.region (id INT)").unwrap();
        if let SqlStatement::Ddl(DdlPlan::CreateTable(plan)) = result {
            assert!(plan.if_not_exists);
            assert_eq!(plan.namespace, "tpch");
            assert_eq!(plan.table_name, "region");
        } else {
            panic!("expected CreateTable plan");
        }
    }

    #[test]
    fn create_schema_if_not_exists() {
        let result = classify_sql("CREATE SCHEMA IF NOT EXISTS tpch").unwrap();
        if let SqlStatement::Ddl(DdlPlan::CreateSchema(plan)) = result {
            assert!(plan.if_not_exists);
            assert_eq!(plan.namespace, "tpch");
        } else {
            panic!("expected CreateSchema plan");
        }
    }
}
