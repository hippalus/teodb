//! Request/response DTOs for the ingest domain.

use serde::{Deserialize, Serialize};

/// JSON request body for row ingestion.
///
/// Accepts any of:
///   - `{"rows": [{...}, ...]}` — explicit rows wrapper (with optional `idempotency_key`)
///   - `[{...}, {...}, ...]`    — bare JSON array of row objects
///   - `{...}`                  — single row object
#[derive(Debug)]
pub struct IngestRequest {
    pub rows: Vec<serde_json::Value>,
    pub idempotency_key: Option<String>,
}

impl<'de> Deserialize<'de> for IngestRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::Array(arr) => Ok(IngestRequest {
                rows: arr,
                idempotency_key: None,
            }),
            serde_json::Value::Object(ref map) if map.contains_key("rows") => {
                let rows = map
                    .get("rows")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                let idempotency_key = map
                    .get("idempotency_key")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                Ok(IngestRequest { rows, idempotency_key })
            }
            serde_json::Value::Object(_) => Ok(IngestRequest {
                rows: vec![value],
                idempotency_key: None,
            }),
            _ => Err(serde::de::Error::custom(
                "expected a JSON object, array of objects, or {\"rows\": [...]}",
            )),
        }
    }
}

/// JSON response for successful ingestion.
#[derive(Debug, Serialize)]
pub struct IngestResponse {
    pub accepted_rows: u64,
    pub batch_id: String,
    pub writer_id: String,
    pub generation: u64,
    /// True when this request's `idempotency_key` matched an earlier ingest
    /// on this writer: the fields above are the original receipt and no rows
    /// were ingested again.
    pub deduplicated: bool,
}

/// JSON response for successful flush.
#[derive(Debug, Serialize)]
pub struct FlushResponse {
    pub status: String,
}
