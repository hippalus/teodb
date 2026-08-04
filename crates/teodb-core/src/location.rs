use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StorageScheme {
    S3,
    Gcs,
    AzureBlob,
    Local,
}

impl StorageScheme {
    /// Returns the URI prefix for this storage scheme (e.g. `s3://bucket/`).
    /// Used internally by `ObjectLocation::to_uri()`.
    pub fn uri_prefix(&self, bucket: Option<&str>) -> String {
        match self {
            Self::S3 => format!("s3://{}/", bucket.unwrap_or("")),
            Self::Gcs => format!("gs://{}/", bucket.unwrap_or("")),
            Self::AzureBlob => format!("abfs://{}/", bucket.unwrap_or("")),
            Self::Local => "file://".to_owned(),
        }
    }

    /// Returns the base URL without trailing path, suitable for DataFusion `ObjectStoreUrl`.
    /// e.g. `s3://bucket` (no trailing slash).
    pub fn url_prefix(&self, bucket: Option<&str>) -> String {
        match self {
            Self::S3 => format!("s3://{}", bucket.unwrap_or("")),
            Self::Gcs => format!("gs://{}", bucket.unwrap_or("")),
            Self::AzureBlob => format!("abfs://{}", bucket.unwrap_or("")),
            Self::Local => "file://localhost".to_owned(),
        }
    }
}

/// A canonical, scheme-aware reference to an object in object storage.
/// This is the *only* shape that crosses crate boundaries when an object
/// must be identified to a catalog or a peer node.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectLocation {
    pub scheme: StorageScheme,
    /// Bucket / container / volume name. `None` only for `Local`.
    pub bucket: Option<String>,
    /// Canonical key within the bucket. Always normalized: no leading slash,
    /// no `..` segments, no double slashes.
    pub key: String,
}

#[derive(Error, Debug)]
pub enum LocationError {
    #[error("malformed URI: {0}")]
    Malformed(String),
    #[error("unsupported scheme: {0}")]
    UnsupportedScheme(String),
    #[error("missing bucket for non-local scheme")]
    MissingBucket,
}

impl ObjectLocation {
    /// Render a fully-qualified URI suitable for catalog metadata.
    pub fn to_uri(&self) -> String {
        let prefix = self.scheme.uri_prefix(self.bucket.as_deref());
        format!("{prefix}{}", self.key)
    }

    /// Parse a URI string into an `ObjectLocation`.
    ///
    /// Normalizes the key: strips leading slash, rejects `..` segments,
    /// collapses double slashes.
    pub fn parse(uri: &str) -> Result<Self, LocationError> {
        let (scheme, rest) = Self::split_scheme(uri)?;

        match scheme {
            StorageScheme::Local => {
                let key = Self::normalize_key(rest)?;
                Ok(Self {
                    scheme,
                    bucket: None,
                    key,
                })
            }
            _ => {
                let (bucket, key_part) = rest
                    .split_once('/')
                    .ok_or_else(|| LocationError::Malformed(format!("no key after bucket in: {uri}")))?;

                if bucket.is_empty() {
                    return Err(LocationError::MissingBucket);
                }
                let key = Self::normalize_key(key_part)?;
                Ok(Self {
                    scheme,
                    bucket: Some(bucket.to_owned()),
                    key,
                })
            }
        }
    }

    fn split_scheme(uri: &str) -> Result<(StorageScheme, &str), LocationError> {
        if let Some(rest) = uri.strip_prefix("s3://") {
            Ok((StorageScheme::S3, rest))
        } else if let Some(rest) = uri.strip_prefix("gs://") {
            Ok((StorageScheme::Gcs, rest))
        } else if let Some(rest) = uri.strip_prefix("abfs://") {
            Ok((StorageScheme::AzureBlob, rest))
        } else if let Some(rest) = uri.strip_prefix("file://") {
            Ok((StorageScheme::Local, rest))
        } else {
            let scheme_end = uri.find("://").unwrap_or(0);
            let scheme = &uri[..scheme_end];
            Err(LocationError::UnsupportedScheme(scheme.to_owned()))
        }
    }

    fn normalize_key(raw: &str) -> Result<String, LocationError> {
        let stripped = raw.strip_prefix('/').unwrap_or(raw);

        // Reject path traversal
        for segment in stripped.split('/') {
            if segment == ".." {
                return Err(LocationError::Malformed("path contains '..' segment".into()));
            }
        }

        // Collapse double slashes and strip trailing slash
        let normalized: String = stripped
            .split('/')
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("/");

        Ok(normalized)
    }
}

/// A store-relative key. Input to the concrete storage implementation's
/// `get`/`put` methods. Must never be a full URI.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectPath(String);

impl ObjectPath {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ObjectPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_s3_uri() {
        let loc = ObjectLocation::parse("s3://my-bucket/data/file.parquet").unwrap();
        assert_eq!(loc.scheme, StorageScheme::S3);
        assert_eq!(loc.bucket.as_deref(), Some("my-bucket"));
        assert_eq!(loc.key, "data/file.parquet");
    }

    #[test]
    fn parse_gs_uri() {
        let loc = ObjectLocation::parse("gs://bucket/path/to/obj").unwrap();
        assert_eq!(loc.scheme, StorageScheme::Gcs);
        assert_eq!(loc.bucket.as_deref(), Some("bucket"));
        assert_eq!(loc.key, "path/to/obj");
    }

    #[test]
    fn parse_azure_uri() {
        let loc = ObjectLocation::parse("abfs://container/dir/file").unwrap();
        assert_eq!(loc.scheme, StorageScheme::AzureBlob);
        assert_eq!(loc.bucket.as_deref(), Some("container"));
        assert_eq!(loc.key, "dir/file");
    }

    #[test]
    fn parse_local_uri() {
        let loc = ObjectLocation::parse("file:///tmp/data/file.parquet").unwrap();
        assert_eq!(loc.scheme, StorageScheme::Local);
        assert_eq!(loc.bucket, None);
        assert_eq!(loc.key, "tmp/data/file.parquet");
    }

    #[test]
    fn parse_normalizes_double_slashes() {
        let loc = ObjectLocation::parse("s3://b/a//b///c").unwrap();
        assert_eq!(loc.key, "a/b/c");
    }

    #[test]
    fn parse_strips_leading_slash_in_key() {
        let loc = ObjectLocation::parse("s3://b//key").unwrap();
        assert_eq!(loc.key, "key");
    }

    #[test]
    fn parse_rejects_dotdot() {
        let err = ObjectLocation::parse("s3://b/a/../secret").unwrap_err();
        assert!(matches!(err, LocationError::Malformed(_)));
    }

    #[test]
    fn parse_rejects_missing_bucket() {
        let err = ObjectLocation::parse("s3:///key").unwrap_err();
        assert!(matches!(err, LocationError::MissingBucket));
    }

    #[test]
    fn parse_rejects_unsupported_scheme() {
        let err = ObjectLocation::parse("ftp://host/path").unwrap_err();
        assert!(matches!(err, LocationError::UnsupportedScheme(_)));
    }

    #[test]
    fn roundtrip_to_uri_parse() {
        let original = ObjectLocation {
            scheme: StorageScheme::S3,
            bucket: Some("warehouse".into()),
            key: "db/tbl/data/0001.parquet".into(),
        };
        let uri = original.to_uri();
        let parsed = ObjectLocation::parse(&uri).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn roundtrip_local() {
        let original = ObjectLocation {
            scheme: StorageScheme::Local,
            bucket: None,
            key: "tmp/test/file.parquet".into(),
        };
        let uri = original.to_uri();
        assert_eq!(uri, "file://tmp/test/file.parquet");
        let parsed = ObjectLocation::parse(&uri).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn object_path_display() {
        let p = ObjectPath::new("data/file.parquet");
        assert_eq!(p.as_str(), "data/file.parquet");
        assert_eq!(p.to_string(), "data/file.parquet");
    }
}
