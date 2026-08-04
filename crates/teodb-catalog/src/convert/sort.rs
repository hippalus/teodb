//! Iceberg → TeoDB sort order conversions.

use teodb_core::error::TeoDBResult;
use teodb_core::schema::{NullOrder, SortDirection, SortField, SortOrder};

use super::partition::iceberg_transform_to_teodb;

pub fn iceberg_sort_order_to_teodb(order: &iceberg::spec::SortOrder) -> TeoDBResult<SortOrder> {
    let fields = order
        .fields
        .iter()
        .map(|f| {
            let direction = match f.direction {
                iceberg::spec::SortDirection::Ascending => SortDirection::Asc,
                iceberg::spec::SortDirection::Descending => SortDirection::Desc,
            };
            let null_order = match f.null_order {
                iceberg::spec::NullOrder::First => NullOrder::NullsFirst,
                iceberg::spec::NullOrder::Last => NullOrder::NullsLast,
            };
            Ok(SortField {
                source_id: f.source_id,
                transform: iceberg_transform_to_teodb(&f.transform)?,
                direction,
                null_order,
            })
        })
        .collect::<TeoDBResult<Vec<_>>>()?;

    Ok(SortOrder {
        order_id: order.order_id as i32,
        fields,
    })
}

pub fn teodb_sort_order_to_iceberg(order: &SortOrder) -> iceberg::spec::SortOrder {
    let fields = order
        .fields
        .iter()
        .map(|field| iceberg::spec::SortField {
            source_id: field.source_id,
            transform: super::partition::teodb_partition_transform_to_iceberg(&field.transform),
            direction: match field.direction {
                SortDirection::Asc => iceberg::spec::SortDirection::Ascending,
                SortDirection::Desc => iceberg::spec::SortDirection::Descending,
            },
            null_order: match field.null_order {
                NullOrder::NullsFirst => iceberg::spec::NullOrder::First,
                NullOrder::NullsLast => iceberg::spec::NullOrder::Last,
            },
        })
        .collect();

    iceberg::spec::SortOrder {
        order_id: i64::from(order.order_id),
        fields,
    }
}
