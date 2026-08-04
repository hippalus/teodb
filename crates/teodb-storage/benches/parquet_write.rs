//! Benchmark: Parquet write throughput.

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

fn make_batch(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("value", DataType::Float64, false),
        Field::new("tag", DataType::Utf8, true),
    ]));

    let ids: Vec<i64> = (0..num_rows as i64).collect();
    let ts: Vec<i64> = (0..num_rows as i64)
        .map(|i| 1_700_000_000_000_000 + i * 1_000_000)
        .collect();
    let vals: Vec<f64> = (0..num_rows).map(|i| i as f64 * 0.1).collect();
    let tags: Vec<Option<&str>> = (0..num_rows)
        .map(|i| if i % 10 == 0 { None } else { Some("sensor-alpha") })
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
            Arc::new(Float64Array::from(vals)),
            Arc::new(StringArray::from(tags)),
        ],
    )
    .unwrap()
}

fn bench_parquet_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("parquet_write");

    for size in [1_000, 10_000, 100_000] {
        let batch = make_batch(size);
        group.bench_with_input(BenchmarkId::new("rows", size), &batch, |b, batch| {
            b.iter(|| {
                let mut buf = Vec::with_capacity(size * 100);
                let props = parquet::file::properties::WriterProperties::builder()
                    .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
                    .build();
                let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
                writer.write(batch).unwrap();
                writer.close().unwrap();
                buf
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_parquet_write);
criterion_main!(benches);
