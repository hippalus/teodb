use super::*;

use teodb_core::scalar::TeoScalar;
use teodb_core::schema::{ColumnMeta, SchemaDefinition, TeoDataType};

#[test]
fn scalar_roundtrip() {
    let scalars = vec![
        TeoScalar::Null,
        TeoScalar::Boolean(true),
        TeoScalar::Int32(42),
        TeoScalar::Int64(-100),
        TeoScalar::Float64(1.23),
        TeoScalar::Utf8("hello".into()),
        TeoScalar::Binary(vec![0xDE, 0xAD]),
        TeoScalar::Date32(19000),
        TeoScalar::TimestampMicros {
            value: 1_000_000,
            tz: Some("UTC".into()),
        },
        TeoScalar::TimestampMicros {
            value: 2_000_000,
            tz: None,
        },
        TeoScalar::Time64Micros(86_400_000_000),
        TeoScalar::Decimal128 {
            value: 12345,
            precision: 18,
            scale: 2,
        },
        TeoScalar::FixedSizeBinary(vec![1, 2, 3, 4]),
    ];

    for scalar in &scalars {
        let datafusion = teo_scalar_to_scalar_value(scalar).unwrap();
        let roundtrip = scalar_value_to_teo_scalar(&datafusion).unwrap();
        assert_eq!(scalar, &roundtrip, "roundtrip failed for {scalar:?}");
    }
}

#[test]
fn schema_conversion() {
    let schema = SchemaDefinition {
        schema_id: 0,
        columns: vec![
            ColumnMeta {
                id: 1,
                name: "id".into(),
                data_type: TeoDataType::Int64,
                nullable: false,
                doc: None,
            },
            ColumnMeta {
                id: 2,
                name: "ts".into(),
                data_type: TeoDataType::TimestampMicros { tz: Some("UTC".into()) },
                nullable: true,
                doc: None,
            },
        ],
        identifier_field_ids: vec![1],
    };

    let arrow = schema_to_arrow(&schema);
    assert_eq!(arrow.fields().len(), 2);
    assert_eq!(field_id_from_arrow_field(arrow.field(0)), Some(1));
    assert_eq!(field_id_from_arrow_field(arrow.field(1)), Some(2));
}
