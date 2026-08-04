//! Benchmark: API REST query-result JSON serialization (D2 / F-31).
//!
//! Compares the pre-D2 pipeline (batches → NDJSON text → per-line
//! `serde_json` re-parse → null-fill → serialize the collected
//! `Vec<Map>` again for the response body) against the post-D2 pipeline
//! (batches → arrow-json `ArrayWriter` with explicit nulls → `RawValue`
//! spliced into the body). Both ends produce the response-body bytes for
//! the `rows` field, so the measured work matches what the handler does.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::json::WriterBuilder;
use arrow::json::writer::JsonArray;
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

const BATCHES: usize = 64;
const ROWS_PER_BATCH: usize = 1024;

fn make_batches() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, false),
    ]));
    (0..BATCHES)
        .map(|b| {
            let ids: Vec<i64> = (0..ROWS_PER_BATCH as i64)
                .map(|i| b as i64 * 1024 + i)
                .collect();
            // ~10% nulls so the null-handling paths do real work.
            let names: Vec<Option<String>> = (0..ROWS_PER_BATCH)
                .map(|i| (i % 10 != 0).then(|| format!("name-{b}-{i}")))
                .collect();
            let values: Vec<f64> = (0..ROWS_PER_BATCH)
                .map(|i| i as f64 * 1.5)
                .collect();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(names)),
                    Arc::new(Float64Array::from(values)),
                ],
            )
            .unwrap()
        })
        .collect()
}

/// Pre-D2: NDJSON text, per-line re-parse, null-fill, then serialize the
/// collected maps again (as axum's `Json` did for the response body).
fn ndjson_reparse(batches: &[RecordBatch], col_names: &[&str]) -> Vec<u8> {
    let mut rows: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
    for batch in batches {
        let mut buf = Vec::new();
        {
            let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
            writer.write(batch).unwrap();
            writer.finish().unwrap();
        }
        let json_str = String::from_utf8_lossy(&buf);
        for line in json_str.lines() {
            if let Ok(serde_json::Value::Object(mut map)) = serde_json::from_str::<serde_json::Value>(line) {
                for col in col_names {
                    if !map.contains_key(*col) {
                        map.insert((*col).to_string(), serde_json::Value::Null);
                    }
                }
                rows.push(map);
            }
        }
    }
    serde_json::to_vec(&rows).unwrap()
}

/// Post-D2: one arrow-json pass with explicit nulls, spliced as `RawValue`.
fn arrow_direct(batches: &[RecordBatch]) -> Vec<u8> {
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(Vec::new());
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    let json = String::from_utf8(writer.into_inner()).unwrap();
    let raw = serde_json::value::RawValue::from_string(json).unwrap();
    serde_json::to_vec(&raw).unwrap()
}

fn bench_query_json(c: &mut Criterion) {
    let batches = make_batches();
    let col_names = ["id", "name", "value"];

    // Both pipelines must produce semantically identical rows.
    let a: serde_json::Value = serde_json::from_slice(&ndjson_reparse(&batches, &col_names)).unwrap();
    let b: serde_json::Value = serde_json::from_slice(&arrow_direct(&batches)).unwrap();
    assert_eq!(a, b, "old and new pipelines must agree");

    let mut group = c.benchmark_group("query_json");
    group.throughput(Throughput::Elements((BATCHES * ROWS_PER_BATCH) as u64));
    group.bench_function("ndjson_reparse", |bench| {
        bench.iter(|| black_box(ndjson_reparse(black_box(&batches), &col_names)));
    });
    group.bench_function("arrow_direct", |bench| {
        bench.iter(|| black_box(arrow_direct(black_box(&batches))));
    });
    group.finish();
}

criterion_group!(benches, bench_query_json);
criterion_main!(benches);
