use serde::{Deserialize, Serialize};

use crate::ident::FieldId;

/// TeoDB's own typed scalar enum. Keeps `teodb-core` free of
/// `datafusion-common`. Conversion to/from `datafusion_common::ScalarValue`
/// lives in downstream crates (`teodb-query`, `teodb-storage`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TeoScalar {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Decimal128 { value: i128, precision: u8, scale: i8 },
    Date32(i32),
    TimestampMicros { value: i64, tz: Option<String> },
    Time64Micros(i64),
    Utf8(String),
    Binary(Vec<u8>),
    FixedSizeBinary(Vec<u8>),
}

impl TeoScalar {
    /// Returns `true` if this is the `Null` variant.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

/// Implement `PartialOrd` for `TeoScalar` with type-safe comparisons.
///
/// Cross-type comparisons return `None`. Within the same type,
/// standard numeric / lexicographic ordering applies.
impl PartialOrd for TeoScalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use TeoScalar::*;
        match (self, other) {
            (Null, Null) => Some(std::cmp::Ordering::Equal),
            (Null, _) | (_, Null) => None,

            (Boolean(a), Boolean(b)) => a.partial_cmp(b),
            (Int8(a), Int8(b)) => a.partial_cmp(b),
            (Int16(a), Int16(b)) => a.partial_cmp(b),
            (Int32(a), Int32(b)) => a.partial_cmp(b),
            (Int64(a), Int64(b)) => a.partial_cmp(b),
            (UInt8(a), UInt8(b)) => a.partial_cmp(b),
            (UInt16(a), UInt16(b)) => a.partial_cmp(b),
            (UInt32(a), UInt32(b)) => a.partial_cmp(b),
            (UInt64(a), UInt64(b)) => a.partial_cmp(b),
            (Float32(a), Float32(b)) => a.partial_cmp(b),
            (Float64(a), Float64(b)) => a.partial_cmp(b),
            (
                Decimal128 {
                    value: a,
                    precision: ap,
                    scale: as_,
                },
                Decimal128 {
                    value: b,
                    precision: bp,
                    scale: bs,
                },
            ) => {
                if ap == bp && as_ == bs {
                    a.partial_cmp(b)
                } else {
                    None
                }
            }
            (Date32(a), Date32(b)) => a.partial_cmp(b),
            (TimestampMicros { value: a, tz: atz }, TimestampMicros { value: b, tz: btz }) => {
                if atz == btz {
                    a.partial_cmp(b)
                } else {
                    None
                }
            }
            (Time64Micros(a), Time64Micros(b)) => a.partial_cmp(b),
            (Utf8(a), Utf8(b)) => a.partial_cmp(b),
            (Binary(a), Binary(b)) => a.partial_cmp(b),
            (FixedSizeBinary(a), FixedSizeBinary(b)) => {
                if a.len() == b.len() {
                    a.partial_cmp(b)
                } else {
                    None
                }
            }

            // Cross-type: not comparable
            _ => None,
        }
    }
}

/// Lower/upper bounds keyed by stable field id. Never by column name.
pub type ColumnBounds = std::collections::HashMap<FieldId, TeoScalar>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_is_null() {
        assert!(TeoScalar::Null.is_null());
        assert!(!TeoScalar::Int32(0).is_null());
    }

    #[test]
    fn same_type_ordering() {
        assert!(TeoScalar::Int64(1) < TeoScalar::Int64(2));
        assert!(TeoScalar::Utf8("a".into()) < TeoScalar::Utf8("b".into()));
        assert!(TeoScalar::Float64(1.0) < TeoScalar::Float64(2.0));
    }

    #[test]
    fn cross_type_incomparable() {
        assert_eq!(TeoScalar::Int32(1).partial_cmp(&TeoScalar::Int64(1)), None);
        assert_eq!(TeoScalar::Utf8("1".into()).partial_cmp(&TeoScalar::Int32(1)), None);
    }

    #[test]
    fn null_incomparable_with_values() {
        assert_eq!(TeoScalar::Null.partial_cmp(&TeoScalar::Int32(0)), None);
    }

    #[test]
    fn decimal_same_precision_comparable() {
        let a = TeoScalar::Decimal128 {
            value: 100,
            precision: 10,
            scale: 2,
        };
        let b = TeoScalar::Decimal128 {
            value: 200,
            precision: 10,
            scale: 2,
        };
        assert!(a < b);
    }

    #[test]
    fn decimal_different_scale_incomparable() {
        let a = TeoScalar::Decimal128 {
            value: 100,
            precision: 10,
            scale: 2,
        };
        let b = TeoScalar::Decimal128 {
            value: 100,
            precision: 10,
            scale: 3,
        };
        assert_eq!(a.partial_cmp(&b), None);
    }

    #[test]
    fn serde_roundtrip() {
        let scalars = vec![
            TeoScalar::Null,
            TeoScalar::Boolean(true),
            TeoScalar::Int64(42),
            TeoScalar::Float64(1.23),
            TeoScalar::Utf8("hello".into()),
            TeoScalar::Binary(vec![0xDE, 0xAD]),
            TeoScalar::Decimal128 {
                value: 12345,
                precision: 18,
                scale: 2,
            },
            TeoScalar::TimestampMicros {
                value: 1_000_000,
                tz: Some("UTC".into()),
            },
        ];

        for s in &scalars {
            let json = serde_json::to_string(s).unwrap();
            let s2: TeoScalar = serde_json::from_str(&json).unwrap();
            assert_eq!(s, &s2);
        }
    }
}
