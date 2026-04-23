use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use umadb_benches::bench_api::comprehensive_full_page_serialized_samples;
use umadb_core::page::Page;

fn page_deserialize_benchmark(c: &mut Criterion) {
    let serialized_pages = comprehensive_full_page_serialized_samples();

    let mut group = c.benchmark_group("page_deserialize");
    group.sample_size(500);
    group.throughput(Throughput::Elements(serialized_pages.len() as u64));

    group.bench_function(
        BenchmarkId::new("deserialize_full_pages", serialized_pages.len()),
        |b| {
            b.iter(|| {
                let mut accum = 0usize;
                for (page_id, bytes) in &serialized_pages {
                    let page = black_box(Page::deserialize(*page_id, bytes).expect("deserialize ok"));
                    accum = accum.wrapping_add(black_box(page.calc_serialized_size()));
                }
                black_box(accum)
            })
        },
    );

    group.finish();
}

criterion_group!(benches, page_deserialize_benchmark);
criterion_main!(benches);