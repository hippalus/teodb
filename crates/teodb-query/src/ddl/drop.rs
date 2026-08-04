//! DROP TABLE / DROP SCHEMA parsing.

use sqlparser::ast::{ObjectType, Statement};

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::TableIdent;
use teodb_core::traits::catalog::DropTableOptions;

use super::idents::{object_name_to_string, resolve_table_name};
use super::plan::{DdlPlan, DropSchemaPlan, DropTablePlan};

/// Parse a `DROP TABLE|SCHEMA|DATABASE` statement into a `DdlPlan`.
pub fn parse_drop(stmt: &Statement) -> TeoDBResult<DdlPlan> {
    let Statement::Drop {
        object_type,
        names,
        if_exists,
        purge,
        ..
    } = stmt
    else {
        return Err(TeoDBError::Internal("expected DROP statement".into()));
    };

    match object_type {
        ObjectType::Table => {
            if names.len() != 1 {
                return Err(TeoDBError::InvalidArgument {
                    field: "sql".into(),
                    message: "DROP TABLE supports exactly one table per statement".into(),
                });
            }
            let (ns, tbl) = resolve_table_name(&names[0])?;
            Ok(DdlPlan::DropTable(DropTablePlan {
                ident: TableIdent::new(&ns, &tbl),
                if_exists: *if_exists,
                options: DropTableOptions { purge: *purge },
            }))
        }
        ObjectType::Schema | ObjectType::Database => {
            if names.len() != 1 {
                return Err(TeoDBError::InvalidArgument {
                    field: "sql".into(),
                    message: "DROP SCHEMA supports exactly one schema per statement".into(),
                });
            }
            let ns = object_name_to_string(&names[0]);
            Ok(DdlPlan::DropSchema(DropSchemaPlan {
                namespace: ns,
                if_exists: *if_exists,
            }))
        }
        other => Err(TeoDBError::InvalidArgument {
            field: "object_type".into(),
            message: format!("DROP {other:?} is not supported"),
        }),
    }
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
    fn drop_table() {
        let stmt = parse_single("DROP TABLE tpch.region");
        let plan = parse_drop(&stmt).unwrap();
        if let DdlPlan::DropTable(p) = plan {
            assert_eq!(p.ident.namespace, "tpch");
            assert_eq!(p.ident.name, "region");
            assert!(!p.if_exists);
            assert!(!p.options.purge);
        } else {
            panic!("expected DropTable");
        }
    }

    #[test]
    fn drop_table_if_exists() {
        let stmt = parse_single("DROP TABLE IF EXISTS default.orders");
        let plan = parse_drop(&stmt).unwrap();
        if let DdlPlan::DropTable(p) = plan {
            assert!(p.if_exists);
            assert!(!p.options.purge);
        } else {
            panic!("expected DropTable");
        }
    }

    #[test]
    fn drop_table_purge() {
        let stmt = parse_single("DROP TABLE IF EXISTS default.orders PURGE");
        let plan = parse_drop(&stmt).unwrap();
        if let DdlPlan::DropTable(p) = plan {
            assert!(p.if_exists);
            assert!(p.options.purge);
        } else {
            panic!("expected DropTable");
        }
    }

    #[test]
    fn drop_schema() {
        let stmt = parse_single("DROP SCHEMA tpch");
        let plan = parse_drop(&stmt).unwrap();
        if let DdlPlan::DropSchema(p) = plan {
            assert_eq!(p.namespace, "tpch");
            assert!(!p.if_exists);
        } else {
            panic!("expected DropSchema");
        }
    }
}
