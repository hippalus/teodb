//! `URLPathHash(Utf8) -> Int32` scalar UDF.
//!
//! Hashes the path component of a URL to a 32-bit integer. Used by the
//! ClickBench query suite for cardinality-bucketed aggregations.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::Result as DFResult;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility};

/// Returns a `ScalarUDF` for `URLPathHash`.
pub fn url_path_hash_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(UrlPathHashUdf::new())
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct UrlPathHashUdf {
    signature: Signature,
}

impl UrlPathHashUdf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Exact(vec![DataType::Utf8]), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UrlPathHashUdf {
    fn name(&self) -> &str {
        "URLPathHash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(
                "URLPathHash expects exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            ColumnarValue::Array(arr) => {
                let str_arr = arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| datafusion_common::DataFusionError::Internal("expected Utf8 array".into()))?;

                let hashes: Int32Array = str_arr
                    .iter()
                    .map(|opt| opt.map(hash_url_path))
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(hashes) as ArrayRef))
            }
            ColumnarValue::Scalar(sv) => {
                let hash = match sv {
                    datafusion_common::ScalarValue::Utf8(Some(s)) => Some(hash_url_path(s)),
                    _ => None,
                };
                Ok(ColumnarValue::Scalar(datafusion_common::ScalarValue::Int32(hash)))
            }
        }
    }
}

/// Extract the path component from a URL-like string and hash it.
fn hash_url_path(url: &str) -> i32 {
    // Find the path: skip scheme + authority, take until '?' or '#'.
    let path = extract_path(url);
    // FNV-1a hash truncated to i32.
    fnv1a_32(path.as_bytes()) as i32
}

fn extract_path(url: &str) -> &str {
    // Skip "scheme://" if present.
    let after_scheme = if let Some(pos) = url.find("://") {
        &url[pos + 3..]
    } else {
        url
    };

    // Skip authority (up to first '/').
    let path_start = after_scheme
        .find('/')
        .unwrap_or(after_scheme.len());
    let path = &after_scheme[path_start..];

    // Truncate at '?' or '#'.
    let end = path.find(['?', '#']).unwrap_or(path.len());
    &path[..end]
}

fn fnv1a_32(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for &b in data {
        hash ^= b as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_extraction() {
        assert_eq!(extract_path("https://example.com/path/to/page?q=1"), "/path/to/page");
        assert_eq!(extract_path("https://example.com/"), "/");
        assert_eq!(extract_path("https://example.com"), "");
        assert_eq!(extract_path("/just/a/path"), "/just/a/path");
    }

    #[test]
    fn hash_determinism() {
        let h1 = hash_url_path("https://example.com/foo");
        let h2 = hash_url_path("https://example.com/foo");
        assert_eq!(h1, h2);
    }

    #[test]
    fn different_paths_different_hashes() {
        let h1 = hash_url_path("https://example.com/foo");
        let h2 = hash_url_path("https://example.com/bar");
        assert_ne!(h1, h2);
    }
}
