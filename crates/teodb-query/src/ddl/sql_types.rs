//! SQL → TeoDB data type mapping.

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::schema::TeoDataType;

/// Map SQL data types to TeoDB data types.
///
/// Comprehensive mapping inspired by production SQL parsers — covers standard
/// SQL types, vendor aliases, and ClickHouse/DuckDB extensions.
pub(super) fn sql_type_to_teo(dt: &sqlparser::ast::DataType) -> TeoDBResult<TeoDataType> {
    use sqlparser::ast::DataType as S;

    match dt {
        // Boolean
        S::Boolean | S::Bool => Ok(TeoDataType::Boolean),

        // Signed integers
        S::TinyInt(_) => Ok(TeoDataType::Int8),
        S::SmallInt(_) | S::Int2(_) => Ok(TeoDataType::Int16),
        S::Int(_) | S::Integer(_) | S::Int4(_) | S::MediumInt(_) => Ok(TeoDataType::Int32),
        S::BigInt(_) | S::Int8(_) => Ok(TeoDataType::Int64),

        // Unsigned integers
        S::UInt8 | S::UTinyInt | S::TinyIntUnsigned(_) => Ok(TeoDataType::UInt8),
        S::UInt16 | S::USmallInt | S::SmallIntUnsigned(_) | S::Int2Unsigned(_) => Ok(TeoDataType::UInt16),
        S::UInt32 | S::IntUnsigned(_) | S::Int4Unsigned(_) | S::IntegerUnsigned(_) | S::MediumIntUnsigned(_) => {
            Ok(TeoDataType::UInt32)
        }
        S::UInt64 | S::UBigInt | S::BigIntUnsigned(_) | S::Unsigned | S::UnsignedInteger => Ok(TeoDataType::UInt64),

        // ClickHouse-style fixed-width integers
        S::Int16 | S::Int32 | S::Int64 | S::Int128 | S::Int256 => {
            match dt {
                S::Int16 => Ok(TeoDataType::Int16),
                S::Int32 => Ok(TeoDataType::Int32),
                S::Int64 => Ok(TeoDataType::Int64),
                S::Int128 => Ok(TeoDataType::Int64), // best-effort
                S::Int256 => Ok(TeoDataType::Int64), // best-effort
                _ => unreachable!(),
            }
        }
        S::UInt128 | S::UInt256 | S::UHugeInt => Ok(TeoDataType::UInt64), // best-effort
        S::Signed | S::SignedInteger | S::HugeInt => Ok(TeoDataType::Int64),

        // Floating point
        S::Float(_) | S::Real | S::Float4 | S::Float32 | S::RealUnsigned => Ok(TeoDataType::Float32),
        S::Double(_)
        | S::DoublePrecision
        | S::DoublePrecisionUnsigned
        | S::DoubleUnsigned(_)
        | S::Float8
        | S::Float64 => Ok(TeoDataType::Float64),

        // Decimal and numeric
        S::Decimal(info) | S::Numeric(info) | S::Dec(info) | S::DecUnsigned(info) => {
            let (precision, scale) = exact_number_info_to_ps(info);
            Ok(TeoDataType::Decimal128 { precision, scale })
        }

        // Date and time
        S::Date | S::Date32 => Ok(TeoDataType::Date32),
        S::Timestamp(_, _) | S::Datetime(_) | S::Datetime64(_, _) => {
            Ok(TeoDataType::TimestampMicros { tz: Some("UTC".into()) })
        }
        S::Time(_, _) => Ok(TeoDataType::Time64Micros),

        // Strings
        S::Varchar(_)
        | S::CharVarying(_)
        | S::CharacterVarying(_)
        | S::Char(_)
        | S::Character(_)
        | S::Text
        | S::TinyText
        | S::MediumText
        | S::LongText
        | S::String(_)
        | S::FixedString(_)
        | S::Nvarchar(_)
        | S::CharacterLargeObject(_)
        | S::CharLargeObject(_)
        | S::Clob(_) => Ok(TeoDataType::Utf8),

        // Binary
        S::Binary(_)
        | S::Varbinary(_)
        | S::Blob(_)
        | S::Bytea
        | S::Bytes(_)
        | S::TinyBlob
        | S::MediumBlob
        | S::LongBlob => Ok(TeoDataType::Binary),

        other => Err(TeoDBError::InvalidArgument {
            field: "data_type".into(),
            message: format!("unsupported SQL data type: {other}"),
        }),
    }
}

fn exact_number_info_to_ps(info: &sqlparser::ast::ExactNumberInfo) -> (u8, i8) {
    match info {
        sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u8, *s as i8),
        sqlparser::ast::ExactNumberInfo::Precision(p) => (*p as u8, 0),
        sqlparser::ast::ExactNumberInfo::None => (38, 10),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::DataType as S;

    #[test]
    fn integer_types() {
        assert_eq!(sql_type_to_teo(&S::Int(None)).unwrap(), TeoDataType::Int32);
        assert_eq!(sql_type_to_teo(&S::BigInt(None)).unwrap(), TeoDataType::Int64);
        assert_eq!(sql_type_to_teo(&S::SmallInt(None)).unwrap(), TeoDataType::Int16);
        assert_eq!(sql_type_to_teo(&S::TinyInt(None)).unwrap(), TeoDataType::Int8);
    }

    #[test]
    fn unsigned_types() {
        assert_eq!(sql_type_to_teo(&S::UInt8).unwrap(), TeoDataType::UInt8);
        assert_eq!(sql_type_to_teo(&S::UInt16).unwrap(), TeoDataType::UInt16);
        assert_eq!(sql_type_to_teo(&S::UInt32).unwrap(), TeoDataType::UInt32);
        assert_eq!(sql_type_to_teo(&S::UInt64).unwrap(), TeoDataType::UInt64);
    }

    #[test]
    fn varchar_to_utf8() {
        assert_eq!(sql_type_to_teo(&S::Varchar(None)).unwrap(), TeoDataType::Utf8);
        assert_eq!(sql_type_to_teo(&S::Text).unwrap(), TeoDataType::Utf8);
    }

    #[test]
    fn decimal() {
        let dt = S::Decimal(sqlparser::ast::ExactNumberInfo::PrecisionAndScale(18, 2));
        match sql_type_to_teo(&dt).unwrap() {
            TeoDataType::Decimal128 { precision, scale } => {
                assert_eq!(precision, 18);
                assert_eq!(scale, 2);
            }
            other => panic!("expected Decimal128, got {other:?}"),
        }
    }

    #[test]
    fn float_types() {
        assert_eq!(sql_type_to_teo(&S::Real).unwrap(), TeoDataType::Float32);
        assert_eq!(sql_type_to_teo(&S::DoublePrecision).unwrap(), TeoDataType::Float64);
    }

    #[test]
    fn unsupported_type_returns_error() {
        assert!(sql_type_to_teo(&S::Regclass).is_err());
    }
}
