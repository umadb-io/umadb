use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustc_hash::FxHashSet;
use std::hint::black_box;
use uuid::Uuid;

fn generate_data(size: usize) -> Vec<(String, String)> {
    (0..size)
        .map(|_| (Uuid::new_v4().to_string(), Uuid::new_v4().to_string()))
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

fn has_duplicates_allocate_and_sort(items: &[(String, String)]) -> bool {
    let mut keys: Vec<&str> = items.iter().map(|(k, _)| k.as_str()).collect();
    keys.sort_unstable();
    keys.windows(2).any(|w| w[0] == w[1])
}

//         let mut keys: Vec<&str> = v.iter().map(|(k, _)| k.as_str()).collect();
//         keys.sort_unstable();
//         keys.windows(2).any(|w| w[0] == w[1])

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
    let sizes = [0, 1, 2, 4, 5, 6, 7, 8, 16, 32, 64, 128, 256, 512, 1024];
    // let sizes = [0, 1, 2, 4, 5, 6, 7, 8, 16, 32, 64];

    for &size in &sizes {
        let data = generate_data(size);

        group.bench_with_input(BenchmarkId::new("nested", size), &data, |b, data| {
            b.iter(|| black_box(has_duplicates_brute_force(data)))
        });

        group.bench_with_input(BenchmarkId::new("hashed", size), &data, |b, data| {
            b.iter(|| black_box(has_duplicates_fxhashset(data)))
        });

        group.bench_with_input(BenchmarkId::new("sorted", size), &data, |b, data| {
            b.iter(|| black_box(has_duplicates_allocate_and_sort(data)))
        });

        // group.bench_with_input(BenchmarkId::new("ahash", size), &data, |b, data| {
        //     b.iter(|| black_box(has_duplicates_ahash(data)))
        // });

        group.bench_with_input(
            BenchmarkId::new("umadb_validator", size),
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
