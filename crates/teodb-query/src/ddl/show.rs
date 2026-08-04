//! SHOW TABLES / SHOW COLUMNS / DESCRIBE parsing.

use sqlparser::ast::Statement;

use teodb_core::error::{TeoDBError, TeoDBResult};

use super::idents::{object_name_to_string, resolve_table_name};
use super::plan::{DdlPlan, DescribeTablePlan, ShowTablesPlan};

/// Parse a `SHOW TABLES [IN <namespace>]` statement.
pub fn parse_show_tables(stmt: &Statement) -> TeoDBResult<DdlPlan> {
    let Statement::ShowTables { show_options, .. } = stmt else {
        return Err(TeoDBError::Internal("expected SHOW TABLES statement".into()));
    };

    let namespace = show_options
        .show_in
        .as_ref()
        .and_then(|si| si.parent_name.as_ref())
        .map(object_name_to_string);

    Ok(DdlPlan::ShowTables(ShowTablesPlan { namespace }))
}

/// Parse a `SHOW COLUMNS FROM <table>` statement.
pub fn parse_show_columns(stmt: &Statement) -> TeoDBResult<DdlPlan> {
    let Statement::ShowColumns { show_options, .. } = stmt else {
        return Err(TeoDBError::Internal("expected SHOW COLUMNS statement".into()));
    };

    let table_name = show_options
        .show_in
        .as_ref()
        .and_then(|si| si.parent_name.as_ref())
        .ok_or_else(|| TeoDBError::InvalidArgument {
            field: "sql".into(),
            message: "SHOW COLUMNS requires FROM <table>".into(),
        })?;

    let (ns, tbl) = resolve_table_name(table_name)?;
    Ok(DdlPlan::ShowColumns(DescribeTablePlan {
        namespace: ns,
        table_name: tbl,
    }))
}

/// Parse a `DESCRIBE <table>` / `EXPLAIN TABLE <table>` statement.
pub fn parse_explain_table(stmt: &Statement) -> TeoDBResult<DdlPlan> {
    let Statement::ExplainTable { table_name, .. } = stmt else {
        return Err(TeoDBError::Internal("expected EXPLAIN TABLE / DESCRIBE".into()));
    };

    let (ns, tbl) = resolve_table_name(table_name)?;
    Ok(DdlPlan::DescribeTable(DescribeTablePlan {
        namespace: ns,
        table_name: tbl,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse_single(sql: &str) -> Statement {
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        stmts.into_iter().next().unwrap()
    }

    #[test]
    fn show_tables_all() {
        let stmt = parse_single("SHOW TABLES");
        let plan = parse_show_tables(&stmt).unwrap();
        if let DdlPlan::ShowTables(p) = plan {
            assert!(p.namespace.is_none());
        } else {
            panic!("expected ShowTables");
        }
    }

    #[test]
    fn show_columns_from_table() {
        let stmt = parse_single("SHOW COLUMNS FROM tpch.region");
        let plan = parse_show_columns(&stmt).unwrap();
        if let DdlPlan::ShowColumns(p) = plan {
            assert_eq!(p.namespace, "tpch");
            assert_eq!(p.table_name, "region");
        } else {
            panic!("expected ShowColumns");
        }
    }

    #[test]
    fn describe_table() {
        let stmt = parse_single("DESCRIBE tpch.region");
        let plan = parse_explain_table(&stmt).unwrap();
        if let DdlPlan::DescribeTable(p) = plan {
            assert_eq!(p.namespace, "tpch");
            assert_eq!(p.table_name, "region");
        } else {
            panic!("expected DescribeTable");
        }
    }
}
