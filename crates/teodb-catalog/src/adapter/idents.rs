//! TeoDB ↔ Iceberg identifier conversion.

use teodb_core::ident::TableIdent;

pub(super) fn make_namespace(ns: &str) -> iceberg::NamespaceIdent {
    let parts: Vec<String> = ns.split('.').map(|s| s.to_owned()).collect();
    iceberg::NamespaceIdent::from_strs(parts).unwrap_or_else(|_| iceberg::NamespaceIdent::new(ns.to_owned()))
}

pub(super) fn make_table_ident(ident: &TableIdent) -> iceberg::TableIdent {
    let ns = make_namespace(&ident.namespace);
    iceberg::TableIdent::new(ns, ident.name.clone())
}
