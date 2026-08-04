//! Shared SQL identifier resolution for DDL statements.

use teodb_core::error::{TeoDBError, TeoDBResult};

/// Extract a simple string from an `ObjectName` (uses the last part).
pub(super) fn object_name_to_string(name: &sqlparser::ast::ObjectName) -> String {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.clone())
        .unwrap_or_default()
}

/// Resolve a `schema.table` or bare `table` name.
/// Returns `("default", table)` if no schema is specified.
pub(super) fn resolve_table_name(name: &sqlparser::ast::ObjectName) -> TeoDBResult<(String, String)> {
    let idents: Vec<String> = name
        .0
        .iter()
        .filter_map(|part| part.as_ident().map(|id| id.value.clone()))
        .collect();

    match idents.as_slice() {
        [ns, tbl] => Ok((ns.clone(), tbl.clone())),
        [tbl] => Ok(("default".into(), tbl.clone())),
        [_catalog, ns, tbl] => Ok((ns.clone(), tbl.clone())),
        _ => Err(TeoDBError::InvalidArgument {
            field: "table_name".into(),
            message: format!("invalid table name: {name}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_qualified() {
        let name = sqlparser::ast::ObjectName::from(vec![
            sqlparser::ast::Ident::new("tpch"),
            sqlparser::ast::Ident::new("region"),
        ]);
        let (ns, tbl) = resolve_table_name(&name).unwrap();
        assert_eq!(ns, "tpch");
        assert_eq!(tbl, "region");
    }

    #[test]
    fn resolve_bare() {
        let name = sqlparser::ast::ObjectName::from(vec![sqlparser::ast::Ident::new("region")]);
        let (ns, tbl) = resolve_table_name(&name).unwrap();
        assert_eq!(ns, "default");
        assert_eq!(tbl, "region");
    }
}
