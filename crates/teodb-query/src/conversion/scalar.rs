use std::sync::Arc;

use datafusion_common::ScalarValue;
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::scalar::TeoScalar;

/// Convert a TeoDB scalar into a DataFusion scalar.
pub fn teo_scalar_to_scalar_value(scalar: &TeoScalar) -> TeoDBResult<ScalarValue> {
    Ok(match scalar {
        TeoScalar::Null => ScalarValue::Null,
        TeoScalar::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        TeoScalar::Int8(value) => ScalarValue::Int8(Some(*value)),
        TeoScalar::Int16(value) => ScalarValue::Int16(Some(*value)),
        TeoScalar::Int32(value) => ScalarValue::Int32(Some(*value)),
        TeoScalar::Int64(value) => ScalarValue::Int64(Some(*value)),
        TeoScalar::UInt8(value) => ScalarValue::UInt8(Some(*value)),
        TeoScalar::UInt16(value) => ScalarValue::UInt16(Some(*value)),
        TeoScalar::UInt32(value) => ScalarValue::UInt32(Some(*value)),
        TeoScalar::UInt64(value) => ScalarValue::UInt64(Some(*value)),
        TeoScalar::Float32(value) => ScalarValue::Float32(Some(*value)),
        TeoScalar::Float64(value) => ScalarValue::Float64(Some(*value)),
        TeoScalar::Decimal128 {
            value,
            precision,
            scale,
        } => ScalarValue::Decimal128(Some(*value), *precision, *scale),
        TeoScalar::Date32(value) => ScalarValue::Date32(Some(*value)),
        TeoScalar::TimestampMicros { value, tz } => {
            ScalarValue::TimestampMicrosecond(Some(*value), tz.as_ref().map(|tz| Arc::from(tz.as_str())))
        }
        TeoScalar::Time64Micros(value) => ScalarValue::Time64Microsecond(Some(*value)),
        TeoScalar::Utf8(value) => ScalarValue::Utf8(Some(value.clone())),
        TeoScalar::Binary(value) => ScalarValue::Binary(Some(value.clone())),
        TeoScalar::FixedSizeBinary(value) => ScalarValue::FixedSizeBinary(value.len() as i32, Some(value.clone())),
    })
}

/// Convert a DataFusion scalar into a TeoDB scalar.
pub fn scalar_value_to_teo_scalar(scalar: &ScalarValue) -> TeoDBResult<TeoScalar> {
    Ok(match scalar {
        ScalarValue::Null => TeoScalar::Null,
        ScalarValue::Boolean(Some(value)) => TeoScalar::Boolean(*value),
        ScalarValue::Int8(Some(value)) => TeoScalar::Int8(*value),
        ScalarValue::Int16(Some(value)) => TeoScalar::Int16(*value),
        ScalarValue::Int32(Some(value)) => TeoScalar::Int32(*value),
        ScalarValue::Int64(Some(value)) => TeoScalar::Int64(*value),
        ScalarValue::UInt8(Some(value)) => TeoScalar::UInt8(*value),
        ScalarValue::UInt16(Some(value)) => TeoScalar::UInt16(*value),
        ScalarValue::UInt32(Some(value)) => TeoScalar::UInt32(*value),
        ScalarValue::UInt64(Some(value)) => TeoScalar::UInt64(*value),
        ScalarValue::Float32(Some(value)) => TeoScalar::Float32(*value),
        ScalarValue::Float64(Some(value)) => TeoScalar::Float64(*value),
        ScalarValue::Decimal128(Some(value), precision, scale) => TeoScalar::Decimal128 {
            value: *value,
            precision: *precision,
            scale: *scale,
        },
        ScalarValue::Date32(Some(value)) => TeoScalar::Date32(*value),
        ScalarValue::TimestampMicrosecond(Some(value), tz) => TeoScalar::TimestampMicros {
            value: *value,
            tz: tz.as_ref().map(ToString::to_string),
        },
        ScalarValue::Time64Microsecond(Some(value)) => TeoScalar::Time64Micros(*value),
        ScalarValue::Utf8(Some(value)) => TeoScalar::Utf8(value.clone()),
        ScalarValue::Binary(Some(value)) => TeoScalar::Binary(value.clone()),
        ScalarValue::FixedSizeBinary(_, Some(value)) => TeoScalar::FixedSizeBinary(value.clone()),
        ScalarValue::Boolean(None)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Decimal128(None, _, _)
        | ScalarValue::Date32(None)
        | ScalarValue::TimestampMicrosecond(None, _)
        | ScalarValue::Time64Microsecond(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::Binary(None)
        | ScalarValue::FixedSizeBinary(_, None) => TeoScalar::Null,
        unsupported => {
            return Err(TeoDBError::InvalidArgument {
                field: "scalar_value".into(),
                message: format!("unsupported ScalarValue variant: {unsupported:?}"),
            });
        }
    })
}
