use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use umadb_benches::bench_api::comprehensive_full_page_samples;
use umadb_core::page::page_approx_deserialized_bytes;

fn page_approx_deserialized_bytes_benchmark(c: &mut Criterion) {
    let full_pages = comprehensive_full_page_samples();

    let mut group = c.benchmark_group("page_approx_deserialized_bytes");
    group.sample_size(500);
    group.throughput(Throughput::Elements(full_pages.len() as u64));

    group.bench_function(
        BenchmarkId::new("introspected_full_pages", full_pages.len()),
        |b| {
            b.iter(|| {
                let mut accum = 0usize;
                for page in &full_pages {
                    accum = accum.wrapping_add(black_box(page_approx_deserialized_bytes(page)));
                }
                black_box(accum)
            })
        },
    );

    group.finish();
}

criterion_group!(benches, page_approx_deserialized_bytes_benchmark);
criterion_main!(benches);