use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use teodb_core::schema::TeoDataType;

pub fn teo_to_arrow_type(data_type: &TeoDataType) -> ArrowDataType {
    match data_type {
        TeoDataType::Boolean => ArrowDataType::Boolean,
        TeoDataType::Int8 => ArrowDataType::Int8,
        TeoDataType::Int16 => ArrowDataType::Int16,
        TeoDataType::Int32 => ArrowDataType::Int32,
        TeoDataType::Int64 => ArrowDataType::Int64,
        TeoDataType::UInt8 => ArrowDataType::UInt8,
        TeoDataType::UInt16 => ArrowDataType::UInt16,
        TeoDataType::UInt32 => ArrowDataType::UInt32,
        TeoDataType::UInt64 => ArrowDataType::UInt64,
        TeoDataType::Float32 => ArrowDataType::Float32,
        TeoDataType::Float64 => ArrowDataType::Float64,
        TeoDataType::Decimal128 { precision, scale } => ArrowDataType::Decimal128(*precision, *scale),
        TeoDataType::Date32 => ArrowDataType::Date32,
        TeoDataType::TimestampMicros { tz } => ArrowDataType::Timestamp(
            TimeUnit::Microsecond,
            tz.as_ref().map(|value| Arc::from(value.as_str())),
        ),
        TeoDataType::Time64Micros => ArrowDataType::Time64(TimeUnit::Microsecond),
        TeoDataType::Utf8 => ArrowDataType::Utf8,
        TeoDataType::Binary => ArrowDataType::Binary,
        TeoDataType::FixedSizeBinary(size) => ArrowDataType::FixedSizeBinary(*size),
    }
}
