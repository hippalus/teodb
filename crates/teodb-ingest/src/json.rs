//! JSON utilities for schema-on-write ingestion.
//!
//! Provides:
//! - **Flattening**: Nested JSON objects are recursively flattened into
//!   dot-separated column names (e.g. `{"a":{"b":1}}` → `{"a.b":1}`).
//!   Arrays are kept as-is (stored as JSON strings).
//! - **Schema inference**: Arrow schema is inferred from the JSON values
//!   when no table schema exists yet.

/// Flatten a JSON object recursively.
///
/// `{"user": {"name": "Alice", "age": 30}, "active": true}`
/// becomes:
/// `{"user.name": "Alice", "user.age": 30, "active": true}`
///
/// Non-object arrays are serialized to JSON strings.
pub fn flatten(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut flat = serde_json::Map::new();
            flatten_into(map, &mut flat, "");
            serde_json::Value::Object(flat)
        }
        other => other.clone(),
    }
}

fn flatten_into(
    src: &serde_json::Map<String, serde_json::Value>,
    dst: &mut serde_json::Map<String, serde_json::Value>,
    prefix: &str,
) {
    for (key, value) in src {
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match value {
            serde_json::Value::Object(nested) => {
                flatten_into(nested, dst, &full_key);
            }
            serde_json::Value::Array(_) => {
                // Store arrays as JSON strings — they can be queried with JSON functions.
                let serialized = serde_json::to_string(value).unwrap_or_else(|_| "[]".to_string());
                dst.insert(full_key, serde_json::Value::String(serialized));
            }
            _ => {
                dst.insert(full_key, value.clone());
            }
        }
    }
}

/// Flatten all rows and return as new Vec.
pub fn flatten_rows(rows: &[serde_json::Value]) -> Vec<serde_json::Value> {
    rows.iter().map(flatten).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn flat_passthrough() {
        let input = json!({"name": "Alice", "age": 30});
        assert_eq!(flatten(&input), input);
    }

    #[test]
    fn nested_one_level() {
        let input = json!({"user": {"name": "Alice"}, "active": true});
        let expected = json!({"user.name": "Alice", "active": true});
        assert_eq!(flatten(&input), expected);
    }

    #[test]
    fn nested_two_levels() {
        let input = json!({"a": {"b": {"c": 42}}});
        let expected = json!({"a.b.c": 42});
        assert_eq!(flatten(&input), expected);
    }

    #[test]
    fn array_serialized_as_string() {
        let input = json!({"tags": ["a", "b"]});
        let result = flatten(&input);
        assert_eq!(result["tags"], json!("[\"a\",\"b\"]"));
    }

    #[test]
    fn null_preserved() {
        let input = json!({"x": null});
        assert_eq!(flatten(&input), json!({"x": null}));
    }
}
