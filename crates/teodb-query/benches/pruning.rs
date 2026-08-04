//! Benchmark: Statistics pruning throughput.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::common::ScalarValue;

fn bench_pruning(c: &mut Criterion) {
    let mut group = c.benchmark_group("pruning");

    for num_files in [100, 1_000, 10_000] {
        group.bench_with_input(BenchmarkId::new("files", num_files), &num_files, |b, &n| {
            let max_vals: Vec<ScalarValue> = (0..n)
                .map(|i| ScalarValue::Int64(Some((i + 1) * 1000 - 1)))
                .collect();

            b.iter(|| {
                let threshold = 5000i64;
                let mut kept = 0u64;
                for max_val in &max_vals {
                    if let ScalarValue::Int64(Some(max)) = max_val
                        && *max > threshold
                    {
                        kept += 1;
                    }
                }
                kept
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_pruning);
criterion_main!(benches);
