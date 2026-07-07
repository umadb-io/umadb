use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use std::collections::{BTreeMap, HashMap};
use std::hint::black_box;

fn metadata_pairs(size: usize) -> Vec<(String, String)> {
    (0..size)
        .map(|i| (format!("metadata_key_{i}"), format!("metadata_value_{i}")))
        .collect()
}

fn metadata_container_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_container_build");

    for &entry_count in &[0usize, 5, 20] {
        let pairs = metadata_pairs(entry_count);

        group.bench_function(BenchmarkId::new("vec_tuple", entry_count), |b| {
            b.iter_batched(
                || pairs.clone(),
                |pairs| {
                    let mut data: Vec<(String, String)> = Vec::with_capacity(entry_count);
                    for (key, value) in pairs {
                        data.push((key, value));
                    }
                    black_box(data);
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_function(BenchmarkId::new("hash_map", entry_count), |b| {
            b.iter_batched(
                || pairs.clone(),
                |pairs| {
                    let mut data: HashMap<String, String> = HashMap::with_capacity(entry_count);
                    for (key, value) in pairs {
                        data.insert(key, value);
                    }
                    black_box(data);
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_function(BenchmarkId::new("btree_map", entry_count), |b| {
            b.iter_batched(
                || pairs.clone(),
                |pairs| {
                    let mut data: BTreeMap<String, String> = BTreeMap::new();
                    for (key, value) in pairs {
                        data.insert(key, value);
                    }
                    black_box(data);
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

criterion_group!(benches, metadata_container_benchmark);
criterion_main!(benches);
