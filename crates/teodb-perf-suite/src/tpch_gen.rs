//! TPC-H data generator for the TeoDB performance suite.
//!
//! Generates small-scale TPC-H data directly as Parquet files.
//! Default scale factor 0.01 produces ~60 K lineitem rows — fast enough
//! for CI smoke tests while exercising the full 22-query workload.

use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use color_eyre::eyre::{Result, WrapErr, eyre};
use parquet::arrow::ArrowWriter;
use rand::{Rng, RngExt};

// Reference data

const REGIONS: [&str; 5] = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];

const NATIONS: [(&str, i32); 25] = [
    ("ALGERIA", 0),
    ("ARGENTINA", 1),
    ("BRAZIL", 1),
    ("CANADA", 1),
    ("EGYPT", 4),
    ("ETHIOPIA", 0),
    ("FRANCE", 3),
    ("GERMANY", 3),
    ("INDIA", 2),
    ("INDONESIA", 2),
    ("IRAN", 4),
    ("IRAQ", 4),
    ("JAPAN", 2),
    ("JORDAN", 4),
    ("KENYA", 0),
    ("MOROCCO", 0),
    ("MOZAMBIQUE", 0),
    ("PERU", 1),
    ("CHINA", 2),
    ("ROMANIA", 3),
    ("SAUDI ARABIA", 4),
    ("VIETNAM", 2),
    ("RUSSIA", 3),
    ("UNITED KINGDOM", 3),
    ("UNITED STATES", 1),
];

const MKT_SEGMENTS: [&str; 5] = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"];
const ORDER_PRIORITIES: [&str; 5] = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
const SHIP_INSTRUCTS: [&str; 4] = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"];
const SHIP_MODES: [&str; 7] = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];
const CONTAINERS: [&str; 20] = [
    "SM CASE",
    "SM BOX",
    "SM PACK",
    "SM PKG",
    "SM BAG",
    "MED CASE",
    "MED BOX",
    "MED PACK",
    "MED PKG",
    "MED BAG",
    "LG CASE",
    "LG BOX",
    "LG PACK",
    "LG PKG",
    "LG BAG",
    "JUMBO CASE",
    "JUMBO BOX",
    "JUMBO PACK",
    "JUMBO PKG",
    "JUMBO BAG",
];
const BRANDS: [&str; 5] = ["Brand#12", "Brand#23", "Brand#34", "Brand#45", "Brand#51"];
const TYPES_PREFIX: [&str; 6] = [
    "STANDARD",
    "SMALL",
    "MEDIUM POLISHED",
    "LARGE",
    "ECONOMY ANODIZED",
    "PROMO",
];
const TYPES_SUFFIX: [&str; 5] = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"];

/// Epoch day for 1992-01-01.
const DATE_LO: i32 = 8035;
/// Epoch day for 1998-11-01.
const DATE_HI: i32 = 10531;

// Public API

/// Generate all eight TPC-H tables as Parquet files under `output_dir`.
pub fn generate(output_dir: &Path, scale_factor: f64) -> Result<Vec<String>> {
    fs::create_dir_all(output_dir)?;
    let mut rng = rand::rng();

    let n_supplier = (scale_factor * 10_000.0).max(10.0) as usize;
    let n_customer = (scale_factor * 150_000.0).max(50.0) as usize;
    let n_part = (scale_factor * 200_000.0).max(50.0) as usize;
    let n_partsupp = n_part * 4;
    let n_orders = (scale_factor * 1_500_000.0).max(100.0) as usize;
    let n_lineitem = (scale_factor * 6_000_000.0).max(200.0) as usize;

    let tables = vec![
        write_region(output_dir)?,
        write_nation(output_dir)?,
        write_supplier(output_dir, n_supplier, &mut rng)?,
        write_customer(output_dir, n_customer, &mut rng)?,
        write_part(output_dir, n_part, &mut rng)?,
        write_partsupp(output_dir, n_partsupp, n_part, n_supplier, &mut rng)?,
        write_orders(output_dir, n_orders, n_customer, &mut rng)?,
        write_lineitem(output_dir, n_lineitem, n_orders, n_part, n_supplier, &mut rng)?,
    ];

    Ok(tables)
}

/// Returns CREATE TABLE DDL statements for all TPC-H tables using
/// standard SQL types, qualified with `tpch.` namespace and `IF NOT EXISTS`.
pub fn create_table_ddl() -> Vec<(&'static str, String)> {
    vec![
        (
            "region",
            "CREATE TABLE IF NOT EXISTS tpch.region (\
            r_regionkey INTEGER NOT NULL, \
            r_name VARCHAR(25) NOT NULL, \
            r_comment VARCHAR(152)\
            )"
            .to_string(),
        ),
        (
            "nation",
            "CREATE TABLE IF NOT EXISTS tpch.nation (\
            n_nationkey INTEGER NOT NULL, \
            n_name VARCHAR(25) NOT NULL, \
            n_regionkey INTEGER NOT NULL, \
            n_comment VARCHAR(152)\
            )"
            .to_string(),
        ),
        (
            "supplier",
            "CREATE TABLE IF NOT EXISTS tpch.supplier (\
            s_suppkey INTEGER NOT NULL, \
            s_name VARCHAR(25) NOT NULL, \
            s_address VARCHAR(40) NOT NULL, \
            s_nationkey INTEGER NOT NULL, \
            s_phone VARCHAR(15) NOT NULL, \
            s_acctbal DECIMAL(15,2) NOT NULL, \
            s_comment VARCHAR(101)\
            )"
            .to_string(),
        ),
        (
            "customer",
            "CREATE TABLE IF NOT EXISTS tpch.customer (\
            c_custkey INTEGER NOT NULL, \
            c_name VARCHAR(25) NOT NULL, \
            c_address VARCHAR(40) NOT NULL, \
            c_nationkey INTEGER NOT NULL, \
            c_phone VARCHAR(15) NOT NULL, \
            c_acctbal DECIMAL(15,2) NOT NULL, \
            c_mktsegment VARCHAR(10) NOT NULL, \
            c_comment VARCHAR(117)\
            )"
            .to_string(),
        ),
        (
            "part",
            "CREATE TABLE IF NOT EXISTS tpch.part (\
            p_partkey INTEGER NOT NULL, \
            p_name VARCHAR(55) NOT NULL, \
            p_mfgr VARCHAR(25) NOT NULL, \
            p_brand VARCHAR(10) NOT NULL, \
            p_type VARCHAR(25) NOT NULL, \
            p_size INTEGER NOT NULL, \
            p_container VARCHAR(10) NOT NULL, \
            p_retailprice DECIMAL(15,2) NOT NULL, \
            p_comment VARCHAR(23)\
            )"
            .to_string(),
        ),
        (
            "partsupp",
            "CREATE TABLE IF NOT EXISTS tpch.partsupp (\
            ps_partkey INTEGER NOT NULL, \
            ps_suppkey INTEGER NOT NULL, \
            ps_availqty INTEGER NOT NULL, \
            ps_supplycost DECIMAL(15,2) NOT NULL, \
            ps_comment VARCHAR(199)\
            )"
            .to_string(),
        ),
        (
            "orders",
            "CREATE TABLE IF NOT EXISTS tpch.orders (\
            o_orderkey INTEGER NOT NULL, \
            o_custkey INTEGER NOT NULL, \
            o_orderstatus VARCHAR(1) NOT NULL, \
            o_totalprice DECIMAL(15,2) NOT NULL, \
            o_orderdate DATE NOT NULL, \
            o_orderpriority VARCHAR(15) NOT NULL, \
            o_clerk VARCHAR(15) NOT NULL, \
            o_shippriority INTEGER NOT NULL, \
            o_comment VARCHAR(79)\
            )"
            .to_string(),
        ),
        (
            "lineitem",
            "CREATE TABLE IF NOT EXISTS tpch.lineitem (\
            l_orderkey INTEGER NOT NULL, \
            l_partkey INTEGER NOT NULL, \
            l_suppkey INTEGER NOT NULL, \
            l_linenumber INTEGER NOT NULL, \
            l_quantity DECIMAL(15,2) NOT NULL, \
            l_extendedprice DECIMAL(15,2) NOT NULL, \
            l_discount DECIMAL(15,2) NOT NULL, \
            l_tax DECIMAL(15,2) NOT NULL, \
            l_returnflag VARCHAR(1) NOT NULL, \
            l_linestatus VARCHAR(1) NOT NULL, \
            l_shipdate DATE NOT NULL, \
            l_commitdate DATE NOT NULL, \
            l_receiptdate DATE NOT NULL, \
            l_shipinstruct VARCHAR(25) NOT NULL, \
            l_shipmode VARCHAR(10) NOT NULL, \
            l_comment VARCHAR(44)\
            )"
            .to_string(),
        ),
    ]
}

/// Read a Parquet file and convert rows to JSON objects for ingest.
pub fn parquet_to_json_batches(
    _table_name: &str,
    path: &std::path::Path,
    batch_size: usize,
) -> Result<Vec<serde_json::Value>> {
    use arrow::util::display::array_value_to_string;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path).wrap_err_with(|| format!("opening {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .wrap_err("building parquet reader")?
        .with_batch_size(batch_size)
        .build()
        .wrap_err("creating parquet batch reader")?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.wrap_err("reading parquet batch")?;
        let schema = batch.schema();
        let mut rows = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let mut map = serde_json::Map::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    continue;
                }
                let value = match field.data_type() {
                    DataType::Int32 => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .ok_or_else(|| eyre!("expected Int32Array for column '{}'", field.name()))?;
                        serde_json::Value::Number(arr.value(row_idx).into())
                    }
                    DataType::Int64 => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| eyre!("expected Int64Array for column '{}'", field.name()))?;
                        serde_json::Value::Number(arr.value(row_idx).into())
                    }
                    DataType::Float64 => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| eyre!("expected Float64Array for column '{}'", field.name()))?;
                        let v = arr.value(row_idx);
                        serde_json::Value::Number(
                            serde_json::Number::from_f64(v).unwrap_or_else(|| serde_json::Number::from(0)),
                        )
                    }
                    DataType::Date32 => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<Date32Array>()
                            .ok_or_else(|| eyre!("expected Date32Array for column '{}'", field.name()))?;
                        let epoch_days = arr.value(row_idx);
                        let date = chrono::NaiveDate::from_num_days_from_ce_opt(epoch_days + 719_163)
                            .unwrap_or(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or_default());
                        serde_json::Value::String(date.format("%Y-%m-%d").to_string())
                    }
                    _ => {
                        let s = array_value_to_string(col.as_ref(), row_idx).unwrap_or_default();
                        serde_json::Value::String(s)
                    }
                };
                map.insert(field.name().clone(), value);
            }
            rows.push(serde_json::Value::Object(map));
        }
        batches.push(serde_json::Value::Array(rows));
    }
    Ok(batches)
}

/// Reads a Parquet file and produces batched `INSERT INTO ... VALUES` SQL statements.
///
/// Each statement batches up to `batch_size` rows into a single INSERT for efficiency.
/// Table names are qualified with `tpch.` namespace.
pub fn parquet_to_insert_statements(
    table_name: &str,
    path: &std::path::Path,
    batch_size: usize,
) -> Result<Vec<String>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path).wrap_err_with(|| format!("opening {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .wrap_err("building parquet reader")?
        .with_batch_size(batch_size)
        .build()
        .wrap_err("creating parquet batch reader")?;

    let mut statements = Vec::new();

    for batch_result in reader {
        let batch = batch_result.wrap_err("reading parquet batch")?;
        let schema = batch.schema();

        let col_names: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let columns_csv = col_names.join(", ");

        let mut value_rows = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let mut vals = Vec::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    vals.push("NULL".to_string());
                    continue;
                }
                let sql_val = match field.data_type() {
                    DataType::Int32 => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .ok_or_else(|| eyre!("expected Int32Array"))?;
                        arr.value(row_idx).to_string()
                    }
                    DataType::Int64 => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| eyre!("expected Int64Array"))?;
                        arr.value(row_idx).to_string()
                    }
                    DataType::Float64 => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| eyre!("expected Float64Array"))?;
                        format!("{}", arr.value(row_idx))
                    }
                    DataType::Date32 => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<Date32Array>()
                            .ok_or_else(|| eyre!("expected Date32Array"))?;
                        let epoch_days = arr.value(row_idx);
                        let date = chrono::NaiveDate::from_num_days_from_ce_opt(epoch_days + 719_163)
                            .unwrap_or(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or_default());
                        format!("'{}'", date.format("%Y-%m-%d"))
                    }
                    _ => {
                        let s = arrow::util::display::array_value_to_string(col.as_ref(), row_idx).unwrap_or_default();
                        format!("'{}'", s.replace('\'', "''"))
                    }
                };
                vals.push(sql_val);
            }
            value_rows.push(format!("({})", vals.join(", ")));
        }

        if !value_rows.is_empty() {
            let stmt = format!(
                "INSERT INTO tpch.{} ({}) VALUES {}",
                table_name,
                columns_csv,
                value_rows.join(", ")
            );
            statements.push(stmt);
        }
    }

    Ok(statements)
}

// Table generators

fn write_region(dir: &Path) -> Result<String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("r_regionkey", DataType::Int32, false),
        Field::new("r_name", DataType::Utf8, false),
        Field::new("r_comment", DataType::Utf8, true),
    ]));
    let keys: Vec<i32> = (0..5).collect();
    let names: Vec<&str> = REGIONS.to_vec();
    let comments: Vec<&str> = REGIONS.to_vec();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(comments)),
        ],
    )?;
    write_parquet(dir, "region", &schema, &[batch])
}

fn write_nation(dir: &Path) -> Result<String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int32, false),
        Field::new("n_name", DataType::Utf8, false),
        Field::new("n_regionkey", DataType::Int32, false),
        Field::new("n_comment", DataType::Utf8, true),
    ]));
    let keys: Vec<i32> = (0..25).collect();
    let names: Vec<&str> = NATIONS.iter().map(|(n, _)| *n).collect();
    let rkeys: Vec<i32> = NATIONS.iter().map(|(_, r)| *r).collect();
    let comments: Vec<&str> = names.clone();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(rkeys)),
            Arc::new(StringArray::from(comments)),
        ],
    )?;
    write_parquet(dir, "nation", &schema, &[batch])
}

fn write_supplier(dir: &Path, n: usize, rng: &mut impl Rng) -> Result<String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int32, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, false),
        Field::new("s_nationkey", DataType::Int32, false),
        Field::new("s_phone", DataType::Utf8, false),
        Field::new("s_acctbal", DataType::Float64, false),
        Field::new("s_comment", DataType::Utf8, true),
    ]));
    let keys: Vec<i32> = (1..=n as i32).collect();
    let names: Vec<String> = (1..=n)
        .map(|i| format!("Supplier#{i:09}"))
        .collect();
    let addrs: Vec<String> = (0..n).map(|i| format!("Addr-{i}")).collect();
    let nkeys: Vec<i32> = (0..n).map(|_| rng.random_range(0..25)).collect();
    let phones: Vec<String> = nkeys
        .iter()
        .enumerate()
        .map(|(i, nk)| {
            format!(
                "{}-{:03}-{:03}-{:04}",
                10 + nk,
                rng.random_range(100..1000_i32),
                rng.random_range(100..1000_i32),
                i % 10000
            )
        })
        .collect();
    let bals: Vec<f64> = (0..n)
        .map(|_| (rng.random_range(-99_900..999_999_i64) as f64) / 100.0)
        .collect();
    let comments: Vec<String> = (0..n)
        .map(|i| {
            if i % 50 == 7 {
                "Customer Complaints noted".to_string()
            } else {
                format!("supplier comment {i}")
            }
        })
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(StringArray::from(
                names
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                addrs
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(nkeys)),
            Arc::new(StringArray::from(
                phones
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(bals)),
            Arc::new(StringArray::from(
                comments
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    write_parquet(dir, "supplier", &schema, &[batch])
}

fn write_customer(dir: &Path, n: usize, rng: &mut impl Rng) -> Result<String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int32, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int32, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_acctbal", DataType::Float64, false),
        Field::new("c_mktsegment", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, true),
    ]));

    let keys: Vec<i32> = (1..=n as i32).collect();
    let names: Vec<String> = (1..=n)
        .map(|i| format!("Customer#{i:09}"))
        .collect();
    let addrs: Vec<String> = (0..n).map(|i| format!("CustAddr-{i}")).collect();
    let nkeys: Vec<i32> = (0..n).map(|_| rng.random_range(0..25)).collect();
    let phones: Vec<String> = nkeys
        .iter()
        .map(|nk| {
            format!(
                "{}-{:03}-{:03}-{:04}",
                10 + nk,
                rng.random_range(100..1000_i32),
                rng.random_range(100..1000_i32),
                rng.random_range(0..10000_i32),
            )
        })
        .collect();
    let bals: Vec<f64> = (0..n)
        .map(|_| (rng.random_range(-99_900..999_999_i64) as f64) / 100.0)
        .collect();
    let segs: Vec<&str> = (0..n)
        .map(|_| MKT_SEGMENTS[rng.random_range(0..5)])
        .collect();
    let comments: Vec<String> = (0..n)
        .map(|i| format!("cust comment {i}"))
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(StringArray::from(
                names
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                addrs
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(nkeys)),
            Arc::new(StringArray::from(
                phones
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(bals)),
            Arc::new(StringArray::from(segs)),
            Arc::new(StringArray::from(
                comments
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    write_parquet(dir, "customer", &schema, &[batch])
}

fn write_part(dir: &Path, n: usize, rng: &mut impl Rng) -> Result<String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int32, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8, false),
        Field::new("p_retailprice", DataType::Float64, false),
        Field::new("p_comment", DataType::Utf8, true),
    ]));

    let keys: Vec<i32> = (1..=n as i32).collect();
    let names: Vec<String> = (1..=n)
        .map(|i| {
            let color = match i % 5 {
                0 => "green",
                1 => "blue",
                2 => "red",
                3 => "forest",
                _ => "white",
            };
            format!("{color} part {i}")
        })
        .collect();
    let mfgrs: Vec<String> = (0..n)
        .map(|i| format!("Manufacturer#{}", 1 + i % 5))
        .collect();
    let brands: Vec<&str> = (0..n).map(|i| BRANDS[i % BRANDS.len()]).collect();
    let types: Vec<String> = (0..n)
        .map(|i| {
            format!(
                "{} {}",
                TYPES_PREFIX[i % TYPES_PREFIX.len()],
                TYPES_SUFFIX[i % TYPES_SUFFIX.len()]
            )
        })
        .collect();
    let sizes: Vec<i32> = (0..n).map(|_| rng.random_range(1..51)).collect();
    let containers: Vec<&str> = (0..n)
        .map(|i| CONTAINERS[i % CONTAINERS.len()])
        .collect();
    let prices: Vec<f64> = (1..=n)
        .map(|i| 900.0 + (i as f64 / 10.0) + (i % 1000) as f64 * 0.01)
        .collect();
    let comments: Vec<String> = (0..n)
        .map(|i| format!("part comment {i}"))
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(StringArray::from(
                names
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                mfgrs
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(brands)),
            Arc::new(StringArray::from(
                types
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(sizes)),
            Arc::new(StringArray::from(containers)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(StringArray::from(
                comments
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    write_parquet(dir, "part", &schema, &[batch])
}

fn write_partsupp(dir: &Path, n: usize, n_part: usize, n_supplier: usize, rng: &mut impl Rng) -> Result<String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ps_partkey", DataType::Int32, false),
        Field::new("ps_suppkey", DataType::Int32, false),
        Field::new("ps_availqty", DataType::Int32, false),
        Field::new("ps_supplycost", DataType::Float64, false),
        Field::new("ps_comment", DataType::Utf8, true),
    ]));

    let mut ps_partkey = Vec::with_capacity(n);
    let mut ps_suppkey = Vec::with_capacity(n);
    let mut ps_availqty = Vec::with_capacity(n);
    let mut ps_supplycost = Vec::with_capacity(n);
    let mut ps_comment = Vec::with_capacity(n);

    for i in 0..n {
        let pk = (i % n_part) as i32 + 1;
        let sk = (i / n_part) as i32 % n_supplier as i32 + 1;
        ps_partkey.push(pk);
        ps_suppkey.push(sk);
        ps_availqty.push(rng.random_range(1..9999));
        ps_supplycost.push((rng.random_range(100..100000_i32) as f64) / 100.0);
        ps_comment.push(format!("ps comment {i}"));
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ps_partkey)),
            Arc::new(Int32Array::from(ps_suppkey)),
            Arc::new(Int32Array::from(ps_availqty)),
            Arc::new(Float64Array::from(ps_supplycost)),
            Arc::new(StringArray::from(
                ps_comment
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    write_parquet(dir, "partsupp", &schema, &[batch])
}

fn write_orders(dir: &Path, n: usize, n_customer: usize, rng: &mut impl Rng) -> Result<String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int32, false),
        Field::new("o_custkey", DataType::Int32, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_clerk", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8, true),
    ]));

    let statuses = ["F", "O", "P"];
    let mut okey = Vec::with_capacity(n);
    let mut ckey = Vec::with_capacity(n);
    let mut ostatus = Vec::with_capacity(n);
    let mut oprice = Vec::with_capacity(n);
    let mut odate = Vec::with_capacity(n);
    let mut oprio = Vec::with_capacity(n);
    let mut oclerk = Vec::with_capacity(n);
    let mut oship = Vec::with_capacity(n);
    let mut ocomment = Vec::with_capacity(n);

    for i in 0..n {
        okey.push((i as i32 + 1) * 4);
        ckey.push(rng.random_range(1..=n_customer as i32));
        ostatus.push(statuses[rng.random_range(0..3_usize)]);
        oprice.push((rng.random_range(100_000..50_000_000_i64) as f64) / 100.0);
        odate.push(rng.random_range(DATE_LO..DATE_HI));
        oprio.push(ORDER_PRIORITIES[rng.random_range(0..5_usize)]);
        oclerk.push(format!("Clerk#{:09}", rng.random_range(1..1000_i32)));
        oship.push(0_i32);
        ocomment.push(if i % 100 == 42 {
            "special requests pending".to_string()
        } else {
            format!("order comment {i}")
        });
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(okey)),
            Arc::new(Int32Array::from(ckey)),
            Arc::new(StringArray::from(ostatus)),
            Arc::new(Float64Array::from(oprice)),
            Arc::new(Date32Array::from(odate)),
            Arc::new(StringArray::from(oprio)),
            Arc::new(StringArray::from(
                oclerk
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(oship)),
            Arc::new(StringArray::from(
                ocomment
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    write_parquet(dir, "orders", &schema, &[batch])
}

#[allow(clippy::too_many_arguments)]
fn write_lineitem(
    dir: &Path,
    n: usize,
    n_orders: usize,
    n_part: usize,
    n_supplier: usize,
    rng: &mut impl Rng,
) -> Result<String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int32, false),
        Field::new("l_partkey", DataType::Int32, false),
        Field::new("l_suppkey", DataType::Int32, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, true),
    ]));

    let flags = ["A", "N", "R"];
    let line_statuses = ["F", "O"];

    let mut l_orderkey = Vec::with_capacity(n);
    let mut l_partkey = Vec::with_capacity(n);
    let mut l_suppkey = Vec::with_capacity(n);
    let mut l_linenumber = Vec::with_capacity(n);
    let mut l_quantity = Vec::with_capacity(n);
    let mut l_extendedprice = Vec::with_capacity(n);
    let mut l_discount = Vec::with_capacity(n);
    let mut l_tax = Vec::with_capacity(n);
    let mut l_returnflag = Vec::with_capacity(n);
    let mut l_linestatus = Vec::with_capacity(n);
    let mut l_shipdate = Vec::with_capacity(n);
    let mut l_commitdate = Vec::with_capacity(n);
    let mut l_receiptdate = Vec::with_capacity(n);
    let mut l_shipinstruct = Vec::with_capacity(n);
    let mut l_shipmode = Vec::with_capacity(n);
    let mut l_comment = Vec::with_capacity(n);

    let mut line_within_order = 1_i32;
    let mut prev_order = 0_usize;

    for i in 0..n {
        let order_idx = i % n_orders;
        if order_idx != prev_order {
            line_within_order = 1;
            prev_order = order_idx;
        } else {
            line_within_order += 1;
        }

        let okey = (order_idx as i32 + 1) * 4;
        let pk = rng.random_range(1..=n_part as i32);
        let sk = rng.random_range(1..=n_supplier as i32);
        let qty = rng.random_range(1..51) as f64;
        let price = 900.0 + (pk as f64 / 10.0);
        let disc = (rng.random_range(0..11) as f64) / 100.0;
        let tax = (rng.random_range(0..9) as f64) / 100.0;

        let ship_d = rng.random_range(DATE_LO..DATE_HI);
        let commit_d = ship_d - rng.random_range(0..60);
        let receipt_d = ship_d + rng.random_range(1..30);

        l_orderkey.push(okey);
        l_partkey.push(pk);
        l_suppkey.push(sk);
        l_linenumber.push(line_within_order);
        l_quantity.push(qty);
        l_extendedprice.push(qty * price);
        l_discount.push(disc);
        l_tax.push(tax);
        l_returnflag.push(flags[rng.random_range(0..3_usize)]);
        l_linestatus.push(line_statuses[rng.random_range(0..2_usize)]);
        l_shipdate.push(ship_d);
        l_commitdate.push(commit_d);
        l_receiptdate.push(receipt_d);
        l_shipinstruct.push(SHIP_INSTRUCTS[rng.random_range(0..4_usize)]);
        l_shipmode.push(SHIP_MODES[rng.random_range(0..7_usize)]);
        l_comment.push(format!("li comment {i}"));
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(l_orderkey)),
            Arc::new(Int32Array::from(l_partkey)),
            Arc::new(Int32Array::from(l_suppkey)),
            Arc::new(Int32Array::from(l_linenumber)),
            Arc::new(Float64Array::from(l_quantity)),
            Arc::new(Float64Array::from(l_extendedprice)),
            Arc::new(Float64Array::from(l_discount)),
            Arc::new(Float64Array::from(l_tax)),
            Arc::new(StringArray::from(l_returnflag)),
            Arc::new(StringArray::from(l_linestatus)),
            Arc::new(Date32Array::from(l_shipdate)),
            Arc::new(Date32Array::from(l_commitdate)),
            Arc::new(Date32Array::from(l_receiptdate)),
            Arc::new(StringArray::from(l_shipinstruct)),
            Arc::new(StringArray::from(l_shipmode)),
            Arc::new(StringArray::from(
                l_comment
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    write_parquet(dir, "lineitem", &schema, &[batch])
}

// Helpers

fn write_parquet(dir: &Path, table: &str, schema: &Arc<Schema>, batches: &[RecordBatch]) -> Result<String> {
    let path = dir.join(format!("{table}.parquet"));
    let file = File::create(&path).wrap_err_with(|| format!("creating {}", path.display()))?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(table.to_string())
}
