//! Input validation for identifiers and query parameters.
//!
//! All user-supplied names (namespace, table, column) must pass through
//! these validators at the API boundary before being used internally.

use crate::error::{TeoDBError, TeoDBResult};

/// Maximum length for any identifier (namespace, table, column name).
const MAX_IDENTIFIER_LEN: usize = 128;

/// Maximum query result limit to prevent unbounded memory usage.
pub const MAX_QUERY_LIMIT: usize = 100_000;

/// Default page size for list endpoints.
pub const DEFAULT_PAGE_SIZE: usize = 100;

/// Maximum page size for list endpoints.
pub const MAX_PAGE_SIZE: usize = 1_000;

/// Validate an identifier (namespace, table, or column name).
///
/// Rules:
/// - Must be 1–128 characters
/// - Must start with a letter or underscore
/// - May contain only ASCII letters, digits, and underscores
pub fn validate_identifier(kind: &str, value: &str) -> TeoDBResult<()> {
    if value.is_empty() {
        return Err(TeoDBError::InvalidArgument {
            field: kind.into(),
            message: format!("{kind} must not be empty"),
        });
    }

    if value.len() > MAX_IDENTIFIER_LEN {
        return Err(TeoDBError::InvalidArgument {
            field: kind.into(),
            message: format!("{kind} exceeds maximum length of {MAX_IDENTIFIER_LEN} characters"),
        });
    }

    let first = value.as_bytes()[0];
    if !first.is_ascii_alphabetic() && first != b'_' {
        return Err(TeoDBError::InvalidArgument {
            field: kind.into(),
            message: format!(
                "{kind} must start with a letter or underscore, got '{}'",
                value.chars().next().unwrap_or('?')
            ),
        });
    }

    if let Some(bad) = value
        .chars()
        .find(|c| !c.is_ascii_alphanumeric() && *c != '_')
    {
        return Err(TeoDBError::InvalidArgument {
            field: kind.into(),
            message: format!(
                "{kind} contains invalid character '{bad}'; only ASCII letters, digits, and underscores are allowed"
            ),
        });
    }

    Ok(())
}

/// Clamp a user-supplied page size to the allowed range.
pub fn clamp_page_size(requested: Option<usize>) -> usize {
    requested
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE)
}

/// Clamp a user-supplied query limit to the allowed range.
pub fn clamp_query_limit(requested: usize) -> usize {
    requested.clamp(1, MAX_QUERY_LIMIT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_identifiers() {
        assert!(validate_identifier("table", "users").is_ok());
        assert!(validate_identifier("table", "_internal").is_ok());
        assert!(validate_identifier("table", "my_table_2").is_ok());
        assert!(validate_identifier("column", "A").is_ok());
    }

    #[test]
    fn empty_identifier_rejected() {
        assert!(validate_identifier("table", "").is_err());
    }

    #[test]
    fn too_long_identifier_rejected() {
        let long = "a".repeat(129);
        assert!(validate_identifier("table", &long).is_err());
    }

    #[test]
    fn leading_digit_rejected() {
        assert!(validate_identifier("table", "2fast").is_err());
    }

    #[test]
    fn special_chars_rejected() {
        assert!(validate_identifier("table", "my-table").is_err());
        assert!(validate_identifier("table", "my.table").is_err());
        assert!(validate_identifier("table", "my table").is_err());
        assert!(validate_identifier("ns", "ns;drop").is_err());
    }

    #[test]
    fn max_length_accepted() {
        let max = "a".repeat(128);
        assert!(validate_identifier("table", &max).is_ok());
    }

    #[test]
    fn page_size_clamping() {
        assert_eq!(clamp_page_size(None), 100);
        assert_eq!(clamp_page_size(Some(50)), 50);
        assert_eq!(clamp_page_size(Some(5000)), 1000);
        assert_eq!(clamp_page_size(Some(0)), 1);
    }

    #[test]
    fn query_limit_clamping() {
        assert_eq!(clamp_query_limit(500), 500);
        assert_eq!(clamp_query_limit(999_999), 100_000);
        assert_eq!(clamp_query_limit(0), 1);
    }
}
