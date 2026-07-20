use ahash::RandomState;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustc_hash::FxHashSet;
use std::collections::HashSet;
use std::hint::black_box;

fn generate_data(size: usize) -> Vec<(String, String)> {
    (0..size)
        .map(|i| (format!("key_{i}"), format!("value_{i}")))
        .collect()
}

fn has_duplicates_brute_force(items: &[(String, String)]) -> bool {
    for i in 0..items.len() {
        for j in i + 1..items.len() {
            if items[i].0 == items[j].0 {
                return true;
            }
        }
    }
    false
}

fn has_duplicates_fxhashset(items: &[(String, String)]) -> bool {
    let mut set = FxHashSet::with_capacity_and_hasher(items.len(), Default::default());
    for item in items {
        if !set.insert(&item.0) {
            return true;
        }
    }
    false
}

// fn has_duplicates_ahash(items: &[(String, String)]) -> bool {
//     let mut set = HashSet::with_capacity_and_hasher(items.len(), RandomState::default());
//     for item in items {
//         if !set.insert(&item.0) {
//             return true;
//         }
//     }
//     false
// }

fn duplicate_check_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("duplicate_check");
    // let sizes = [0, 1, 2, 4, 5, 6, 7, 8, 16, 32, 64, 128, 256, 512, 1024];
    let sizes = [0, 1, 2, 4, 5, 6, 7, 8, 16, 32, 64];

    for &size in &sizes {
        let data = generate_data(size);

        group.bench_with_input(BenchmarkId::new("brute_force", size), &data, |b, data| {
            b.iter(|| black_box(has_duplicates_brute_force(data)))
        });

        group.bench_with_input(BenchmarkId::new("fxhashset", size), &data, |b, data| {
            b.iter(|| black_box(has_duplicates_fxhashset(data)))
        });

        // group.bench_with_input(BenchmarkId::new("ahash", size), &data, |b, data| {
        //     b.iter(|| black_box(has_duplicates_ahash(data)))
        // });

        group.bench_with_input(
            BenchmarkId::new("umadb_core_has_duplicate", size),
            &data,
            |b, data| {
                b.iter(|| {
                    black_box(umadb_core::events_tree_nodes::has_duplicate_metadata_key(
                        data,
                    ))
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, duplicate_check_benchmark);
criterion_main!(benches);
