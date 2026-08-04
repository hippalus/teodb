use teodb_core::error::TeoDBError;

/// Map an `iceberg::Error` to `TeoDBError`.
pub fn map_iceberg_error(e: iceberg::Error) -> TeoDBError {
    use iceberg::ErrorKind;
    match e.kind() {
        ErrorKind::DataInvalid => TeoDBError::InvalidArgument {
            field: "iceberg".into(),
            message: e.to_string(),
        },
        ErrorKind::FeatureUnsupported => TeoDBError::Catalog(format!("unsupported: {e}")),

        // Already exists
        ErrorKind::TableAlreadyExists | ErrorKind::NamespaceAlreadyExists => TeoDBError::AlreadyExists {
            resource: e.to_string(),
        },

        // Not found
        ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound => TeoDBError::NotFound {
            resource: e.to_string(),
        },

        // Concurrency conflicts
        ErrorKind::PreconditionFailed | ErrorKind::CatalogCommitConflicts => TeoDBError::Conflict {
            resource: e.to_string(),
            expected: String::new(),
            actual: "<concurrent change>".into(),
        },

        // Catch-all for unexpected and future variants
        // Only match HTTP status codes — never substring-match error class names
        // like "AlreadyExists" which can appear in HTML error pages from the catalog.
        ErrorKind::Unexpected => {
            let msg = e.to_string();
            if msg.contains("404")
                || msg.contains("NoSuchTable")
                || msg.contains("not found")
                || msg.contains("does not exist")
            {
                TeoDBError::NotFound { resource: msg }
            } else if msg.contains("409") || msg.contains("already exists") {
                TeoDBError::AlreadyExists { resource: msg }
            } else if msg.contains("Conflict") || msg.contains("CommitFailed") {
                TeoDBError::Conflict {
                    resource: msg,
                    expected: String::new(),
                    actual: "<concurrent change>".into(),
                }
            } else if msg.contains("401") {
                TeoDBError::Unauthorized
            } else if msg.contains("403") {
                TeoDBError::Forbidden("access denied".into())
            } else {
                TeoDBError::Catalog(msg)
            }
        }
        _ => TeoDBError::Catalog(format!("iceberg error: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::{Error, ErrorKind};

    #[test]
    fn map_data_invalid() {
        let e = Error::new(ErrorKind::DataInvalid, "bad schema");
        let te = map_iceberg_error(e);
        assert!(matches!(te, TeoDBError::InvalidArgument { .. }));
    }

    #[test]
    fn map_feature_unsupported() {
        let e = Error::new(ErrorKind::FeatureUnsupported, "views not supported");
        let te = map_iceberg_error(e);
        assert!(matches!(te, TeoDBError::Catalog(_)));
    }

    #[test]
    fn map_unexpected() {
        let e = Error::new(ErrorKind::Unexpected, "internal error");
        let te = map_iceberg_error(e);
        assert!(matches!(te, TeoDBError::Catalog(_)));
    }

    #[test]
    fn map_table_already_exists() {
        let e = Error::new(ErrorKind::TableAlreadyExists, "table tpch.region exists");
        let te = map_iceberg_error(e);
        assert!(matches!(te, TeoDBError::AlreadyExists { .. }));
    }

    #[test]
    fn map_namespace_already_exists() {
        let e = Error::new(ErrorKind::NamespaceAlreadyExists, "namespace tpch exists");
        let te = map_iceberg_error(e);
        assert!(matches!(te, TeoDBError::AlreadyExists { .. }));
    }

    #[test]
    fn map_table_not_found() {
        let e = Error::new(ErrorKind::TableNotFound, "table tpch.missing not found");
        let te = map_iceberg_error(e);
        assert!(matches!(te, TeoDBError::NotFound { .. }));
    }

    #[test]
    fn map_namespace_not_found() {
        let e = Error::new(ErrorKind::NamespaceNotFound, "namespace missing not found");
        let te = map_iceberg_error(e);
        assert!(matches!(te, TeoDBError::NotFound { .. }));
    }

    #[test]
    fn map_commit_conflict() {
        let e = Error::new(ErrorKind::CatalogCommitConflicts, "commit conflict");
        let te = map_iceberg_error(e);
        assert!(matches!(te, TeoDBError::Conflict { .. }));
    }

    #[test]
    fn map_precondition_failed() {
        let e = Error::new(ErrorKind::PreconditionFailed, "precondition failed");
        let te = map_iceberg_error(e);
        assert!(matches!(te, TeoDBError::Conflict { .. }));
    }
}
