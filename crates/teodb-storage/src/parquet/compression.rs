//! Parquet compression codec configuration.
//!
//! Parses operator-facing codec strings (`"zstd(3)"`, `"snappy"`, …) into a
//! typed codec and converts it to the parquet crate's `Compression`.

use parquet::basic::Compression;
use teodb_core::error::{TeoDBError, TeoDBResult};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CompressionParseError {
    #[error("invalid {codec} level: '{value}'")]
    InvalidLevel { codec: &'static str, value: String },

    #[error("{codec} level must be {min}-{max}, got {actual}")]
    LevelOutOfRange {
        codec: &'static str,
        min: i32,
        max: i32,
        actual: i32,
    },

    #[error(
        "unknown compression codec: '{0}'. Supported: zstd, zstd(N), snappy, gzip, gzip(N), lz4, lzo, brotli, brotli(N), none"
    )]
    UnknownCodec(String),
}

/// Parquet compression codec with optional codec-specific parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    /// No compression.
    Uncompressed,
    /// Snappy — fast, moderate ratio. Good default for hot-path writes.
    Snappy,
    /// Gzip with compression level (0–9, default 6).
    Gzip(u8),
    /// LZ4 (raw) — very fast, low ratio.
    Lz4Raw,
    /// ZSTD with compression level (1–22, default 3).
    Zstd(i32),
    /// LZO — legacy; fast, moderate ratio.
    Lzo,
    /// Brotli with compression level (0–11, default 4).
    Brotli(u32),
}

impl Default for CompressionCodec {
    fn default() -> Self {
        Self::Zstd(3)
    }
}

impl std::fmt::Display for CompressionCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Uncompressed => write!(f, "none"),
            Self::Snappy => write!(f, "snappy"),
            Self::Gzip(level) => write!(f, "gzip({level})"),
            Self::Lz4Raw => write!(f, "lz4"),
            Self::Zstd(level) => write!(f, "zstd({level})"),
            Self::Lzo => write!(f, "lzo"),
            Self::Brotli(level) => write!(f, "brotli({level})"),
        }
    }
}

impl CompressionCodec {
    /// Parse a codec from a string like "zstd", "zstd(3)", "snappy", "lz4",
    /// "gzip(6)", "brotli(4)", "none", "uncompressed".
    pub fn from_str_config(s: &str) -> Result<Self, CompressionParseError> {
        let s = s.trim().to_lowercase();

        if let Some(inner) = s
            .strip_prefix("zstd(")
            .and_then(|r| r.strip_suffix(')'))
        {
            let level: i32 = inner
                .trim()
                .parse()
                .map_err(|_| CompressionParseError::InvalidLevel {
                    codec: "zstd",
                    value: inner.into(),
                })?;
            if !(1..=22).contains(&level) {
                return Err(CompressionParseError::LevelOutOfRange {
                    codec: "zstd",
                    min: 1,
                    max: 22,
                    actual: level,
                });
            }
            return Ok(Self::Zstd(level));
        }
        if let Some(inner) = s
            .strip_prefix("gzip(")
            .and_then(|r| r.strip_suffix(')'))
        {
            let level: u8 = inner
                .trim()
                .parse()
                .map_err(|_| CompressionParseError::InvalidLevel {
                    codec: "gzip",
                    value: inner.into(),
                })?;
            if level > 9 {
                return Err(CompressionParseError::LevelOutOfRange {
                    codec: "gzip",
                    min: 0,
                    max: 9,
                    actual: i32::from(level),
                });
            }
            return Ok(Self::Gzip(level));
        }
        if let Some(inner) = s
            .strip_prefix("brotli(")
            .and_then(|r| r.strip_suffix(')'))
        {
            let level: u32 = inner
                .trim()
                .parse()
                .map_err(|_| CompressionParseError::InvalidLevel {
                    codec: "brotli",
                    value: inner.into(),
                })?;
            if level > 11 {
                return Err(CompressionParseError::LevelOutOfRange {
                    codec: "brotli",
                    min: 0,
                    max: 11,
                    actual: level as i32,
                });
            }
            return Ok(Self::Brotli(level));
        }

        match s.as_str() {
            "zstd" => Ok(Self::Zstd(3)),
            "snappy" => Ok(Self::Snappy),
            "gzip" => Ok(Self::Gzip(6)),
            "lz4" | "lz4_raw" => Ok(Self::Lz4Raw),
            "lzo" => Ok(Self::Lzo),
            "brotli" => Ok(Self::Brotli(4)),
            "none" | "uncompressed" => Ok(Self::Uncompressed),
            other => Err(CompressionParseError::UnknownCodec(other.into())),
        }
    }

    /// Convert to the parquet crate's `Compression` enum.
    pub(super) fn to_parquet(self) -> TeoDBResult<Compression> {
        use parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};

        match self {
            Self::Uncompressed => Ok(Compression::UNCOMPRESSED),
            Self::Snappy => Ok(Compression::SNAPPY),
            Self::Gzip(level) => {
                let gl = GzipLevel::try_new(level as u32)
                    .map_err(|e| TeoDBError::Parquet(format!("bad gzip level: {e}")))?;
                Ok(Compression::GZIP(gl))
            }
            Self::Lz4Raw => Ok(Compression::LZ4_RAW),
            Self::Zstd(level) => {
                let zl = ZstdLevel::try_new(level).map_err(|e| TeoDBError::Parquet(format!("bad zstd level: {e}")))?;
                Ok(Compression::ZSTD(zl))
            }
            Self::Lzo => Ok(Compression::LZO),
            Self::Brotli(level) => {
                let bl =
                    BrotliLevel::try_new(level).map_err(|e| TeoDBError::Parquet(format!("bad brotli level: {e}")))?;
                Ok(Compression::BROTLI(bl))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_codec_parse() {
        assert_eq!(
            CompressionCodec::from_str_config("zstd").unwrap(),
            CompressionCodec::Zstd(3)
        );
        assert_eq!(
            CompressionCodec::from_str_config("zstd(7)").unwrap(),
            CompressionCodec::Zstd(7)
        );
        assert_eq!(
            CompressionCodec::from_str_config("snappy").unwrap(),
            CompressionCodec::Snappy
        );
        assert_eq!(
            CompressionCodec::from_str_config("lz4").unwrap(),
            CompressionCodec::Lz4Raw
        );
        assert_eq!(
            CompressionCodec::from_str_config("lz4_raw").unwrap(),
            CompressionCodec::Lz4Raw
        );
        assert_eq!(
            CompressionCodec::from_str_config("gzip").unwrap(),
            CompressionCodec::Gzip(6)
        );
        assert_eq!(
            CompressionCodec::from_str_config("gzip(9)").unwrap(),
            CompressionCodec::Gzip(9)
        );
        assert_eq!(
            CompressionCodec::from_str_config("brotli").unwrap(),
            CompressionCodec::Brotli(4)
        );
        assert_eq!(
            CompressionCodec::from_str_config("brotli(11)").unwrap(),
            CompressionCodec::Brotli(11)
        );
        assert_eq!(
            CompressionCodec::from_str_config("none").unwrap(),
            CompressionCodec::Uncompressed
        );
        assert_eq!(
            CompressionCodec::from_str_config("SNAPPY").unwrap(),
            CompressionCodec::Snappy
        );
        assert!(CompressionCodec::from_str_config("zstd(99)").is_err());
        assert!(CompressionCodec::from_str_config("gzip(10)").is_err());
        assert!(CompressionCodec::from_str_config("brotli(12)").is_err());
        assert_eq!(
            CompressionCodec::from_str_config("unknown").unwrap_err(),
            CompressionParseError::UnknownCodec("unknown".into())
        );
    }

    #[test]
    fn compression_codec_display() {
        assert_eq!(CompressionCodec::Zstd(3).to_string(), "zstd(3)");
        assert_eq!(CompressionCodec::Snappy.to_string(), "snappy");
        assert_eq!(CompressionCodec::Gzip(6).to_string(), "gzip(6)");
        assert_eq!(CompressionCodec::Lz4Raw.to_string(), "lz4");
        assert_eq!(CompressionCodec::Brotli(4).to_string(), "brotli(4)");
        assert_eq!(CompressionCodec::Lzo.to_string(), "lzo");
        assert_eq!(CompressionCodec::Uncompressed.to_string(), "none");
    }

    #[test]
    fn compression_codec_to_parquet() {
        // All codecs should produce a valid parquet Compression value.
        let codecs = vec![
            CompressionCodec::Uncompressed,
            CompressionCodec::Snappy,
            CompressionCodec::Gzip(6),
            CompressionCodec::Lz4Raw,
            CompressionCodec::Zstd(3),
            CompressionCodec::Brotli(4),
        ];
        for codec in codecs {
            assert!(codec.to_parquet().is_ok(), "failed for {codec}");
        }
    }
}
