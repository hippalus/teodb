//! Iceberg Datum ↔ TeoDB TeoScalar conversions.

use std::collections::HashMap;

use iceberg::spec::PrimitiveLiteral;

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::FieldId;
use teodb_core::scalar::TeoScalar;
use teodb_core::schema::TeoDataType;

pub fn datum_to_teo_scalar(datum: &iceberg::spec::Datum, dt: &TeoDataType) -> TeoDBResult<TeoScalar> {
    primitive_literal_to_teo_scalar(datum.literal(), dt)
}

pub(crate) fn primitive_literal_to_teo_scalar(literal: &PrimitiveLiteral, dt: &TeoDataType) -> TeoDBResult<TeoScalar> {
    match (dt, literal) {
        (TeoDataType::Boolean, PrimitiveLiteral::Boolean(v)) => Ok(TeoScalar::Boolean(*v)),
        (TeoDataType::Int8, PrimitiveLiteral::Int(v)) => Ok(TeoScalar::Int8(*v as i8)),
        (TeoDataType::Int16, PrimitiveLiteral::Int(v)) => Ok(TeoScalar::Int16(*v as i16)),
        (TeoDataType::Int32, PrimitiveLiteral::Int(v)) => Ok(TeoScalar::Int32(*v)),
        (TeoDataType::UInt8, PrimitiveLiteral::Int(v)) => Ok(TeoScalar::UInt8(*v as u8)),
        (TeoDataType::UInt16, PrimitiveLiteral::Int(v)) => Ok(TeoScalar::UInt16(*v as u16)),
        (TeoDataType::UInt32, PrimitiveLiteral::Int(v)) => Ok(TeoScalar::UInt32(*v as u32)),
        (TeoDataType::Int64, PrimitiveLiteral::Long(v)) => Ok(TeoScalar::Int64(*v)),
        (TeoDataType::UInt64, PrimitiveLiteral::Long(v)) => Ok(TeoScalar::UInt64(*v as u64)),
        (TeoDataType::Float32, PrimitiveLiteral::Float(v)) => Ok(TeoScalar::Float32(v.0)),
        (TeoDataType::Float64, PrimitiveLiteral::Double(v)) => Ok(TeoScalar::Float64(v.0)),
        (TeoDataType::Date32, PrimitiveLiteral::Int(v)) => Ok(TeoScalar::Date32(*v)),
        (TeoDataType::Time64Micros, PrimitiveLiteral::Long(v)) => Ok(TeoScalar::Time64Micros(*v)),
        (TeoDataType::TimestampMicros { tz }, PrimitiveLiteral::Long(v)) => Ok(TeoScalar::TimestampMicros {
            value: *v,
            tz: tz.clone(),
        }),
        (TeoDataType::Utf8, PrimitiveLiteral::String(v)) => Ok(TeoScalar::Utf8(v.clone())),
        (TeoDataType::Binary, PrimitiveLiteral::Binary(v)) => Ok(TeoScalar::Binary(v.clone())),
        (TeoDataType::FixedSizeBinary(16), PrimitiveLiteral::UInt128(v)) => {
            Ok(TeoScalar::FixedSizeBinary(v.to_be_bytes().to_vec()))
        }
        (TeoDataType::FixedSizeBinary(_), PrimitiveLiteral::Binary(v)) => Ok(TeoScalar::FixedSizeBinary(v.clone())),
        (TeoDataType::Decimal128 { precision, scale }, PrimitiveLiteral::Int128(v)) => Ok(TeoScalar::Decimal128 {
            value: *v,
            precision: *precision,
            scale: *scale,
        }),
        (_, PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin) => Err(TeoDBError::Catalog(format!(
            "cannot convert iceberg sentinel literal {literal:?} to {dt:?}"
        ))),
        _ => Err(TeoDBError::Catalog(format!(
            "cannot convert iceberg literal {literal:?} to {dt:?}"
        ))),
    }
}

pub fn teo_scalar_to_datum(scalar: &TeoScalar) -> TeoDBResult<iceberg::spec::Datum> {
    use iceberg::spec::Datum;
    match scalar {
        TeoScalar::Null => Err(TeoDBError::InvalidArgument {
            field: "scalar".into(),
            message: "cannot convert Null TeoScalar to Datum".into(),
        }),
        TeoScalar::Boolean(v) => Ok(Datum::bool(*v)),
        TeoScalar::Int8(v) => Ok(Datum::int(*v as i32)),
        TeoScalar::Int16(v) => Ok(Datum::int(*v as i32)),
        TeoScalar::Int32(v) => Ok(Datum::int(*v)),
        TeoScalar::UInt8(v) => Ok(Datum::int(*v as i32)),
        TeoScalar::UInt16(v) => Ok(Datum::int(*v as i32)),
        TeoScalar::UInt32(v) => Ok(Datum::long(*v as i64)),
        TeoScalar::Int64(v) => Ok(Datum::long(*v)),
        TeoScalar::UInt64(v) => Ok(Datum::long(*v as i64)),
        TeoScalar::Float32(v) => Ok(Datum::float(*v)),
        TeoScalar::Float64(v) => Ok(Datum::double(*v)),
        TeoScalar::Date32(v) => Ok(Datum::date(*v)),
        TeoScalar::Time64Micros(v) => {
            Datum::time_micros(*v).map_err(|e| TeoDBError::Catalog(format!("invalid time value: {e}")))
        }
        TeoScalar::TimestampMicros { value, tz } => {
            if tz.is_some() {
                Ok(Datum::timestamptz_micros(*value))
            } else {
                Ok(Datum::timestamp_micros(*value))
            }
        }
        TeoScalar::Utf8(v) => Ok(Datum::string(v)),
        TeoScalar::Binary(v) => Ok(Datum::binary(v.iter().copied())),
        TeoScalar::FixedSizeBinary(v) => Ok(Datum::fixed(v.iter().copied())),
        TeoScalar::Decimal128 {
            value,
            precision,
            scale,
        } => {
            let decimal_str = format_i128_decimal(*value, *scale);
            iceberg::spec::Datum::decimal_from_str(&decimal_str)
                .map_err(|e| TeoDBError::Catalog(format!("decimal conversion error (p={precision},s={scale}): {e}")))
        }
    }
}

/// Format an i128 value as a decimal string with the given scale.
fn format_i128_decimal(value: i128, scale: i8) -> String {
    if scale <= 0 {
        return format!("{value}");
    }
    let scale = scale as u32;
    let divisor = 10i128.pow(scale);
    let whole = value / divisor;
    let frac = (value % divisor).unsigned_abs();
    format!("{whole}.{frac:0>width$}", width = scale as usize)
}

pub(crate) fn convert_teo_scalar_bounds(
    bounds: &HashMap<FieldId, TeoScalar>,
) -> TeoDBResult<HashMap<i32, iceberg::spec::Datum>> {
    bounds
        .iter()
        .map(|(&field_id, scalar)| {
            let datum = teo_scalar_to_datum(scalar)?;
            Ok((field_id, datum))
        })
        .collect()
}

pub(crate) fn convert_datum_bounds(
    bounds: &HashMap<i32, iceberg::spec::Datum>,
    type_lookup: &super::TypeLookup,
) -> TeoDBResult<HashMap<FieldId, TeoScalar>> {
    let mut result = HashMap::new();
    for (&field_id, datum) in bounds {
        let Some(dt) = type_lookup.get(&field_id) else {
            continue;
        };
        let scalar = datum_to_teo_scalar(datum, dt)?;
        result.insert(field_id, scalar);
    }
    Ok(result)
}
