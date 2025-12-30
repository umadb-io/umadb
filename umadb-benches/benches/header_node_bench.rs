use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use umadb_core::common::{PageID, Position, Tsn};
use umadb_core::header_node::HeaderNode;

// Build the sample header once, outside of the measured benchmark closures
static HEADER: HeaderNode = HeaderNode {
    tsn: Tsn(42),
    next_page_id: PageID(123),
    free_lists_tree_root_id: PageID(456),
    events_tree_root_id: PageID(789),
    tags_tree_root_id: PageID(321),
    next_position: Position(9876543210),
    schema_version: umadb_core::db::DB_SCHEMA_VERSION,
};

pub fn header_node_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("header_node");

    // Known constant sizes for header serialization
    let header_size_bytes: u64 = 52;
    group.throughput(Throughput::Bytes(header_size_bytes));

    // Benchmark serialization (alloc + encode)
    group.bench_function(BenchmarkId::new("serialize", header_size_bytes), |b| {
        b.iter(|| {
            let mut v = black_box(Vec::<u8>::with_capacity(52));
            // ensure vec has length 52 so we can serialize into it
            unsafe {
                v.set_len(52);
            }
            black_box(&HEADER).serialize_into(black_box(&mut v));
            black_box(v)
        })
    });

    // Allocation-only: separate the Vec allocation cost
    group.bench_function(BenchmarkId::new("alloc_only", header_size_bytes), |b| {
        b.iter(|| {
            let v = black_box(Vec::<u8>::with_capacity(48));
            black_box(v)
        })
    });

    // Allocation-free serialization into a fixed stack buffer
    group.bench_function(
        BenchmarkId::new("serialize_into_stack", header_size_bytes),
        |b| {
            let mut buf = [0u8; 52];
            b.iter(|| {
                black_box(&HEADER).serialize_into(black_box(&mut buf));
                black_box(&buf);
            })
        },
    );

    // Prepare serialized bytes once for deserialization benchmark (outside iter)
    let mut serialized = [0u8; 52];
    HEADER.serialize_into(&mut serialized);

    // Benchmark deserialization reusing the same bytes each iteration (pure from_slice; no cloning/allocation)
    group.bench_function(
        BenchmarkId::new("deserialize_reuse", header_size_bytes),
        |b| {
            b.iter(|| {
                let node =
                    HeaderNode::from_slice(black_box(&serialized)).expect("valid header bytes");
                black_box(node)
            })
        },
    );

    // Benchmark deserialization immediately after a fresh serialize (setup does serialize; measure only from_slice)
    group.bench_function(
        BenchmarkId::new("deserialize_after_serialize", header_size_bytes),
        |b| {
            b.iter_batched(
                || {
                    let mut buf = [0u8; 52];
                    HEADER.serialize_into(&mut buf);
                    buf
                },
                |bytes: [u8; 52]| {
                    let node =
                        HeaderNode::from_slice(black_box(&bytes)).expect("valid header bytes");
                    black_box(node)
                },
                criterion::BatchSize::SmallInput,
            )
        },
    );

    // Allocation-free round trip using a reusable stack buffer
    group.bench_function(
        BenchmarkId::new("round_trip_no_alloc", header_size_bytes),
        |b| {
            let mut buf = [0u8; 52];
            b.iter(|| {
                let header = black_box(&HEADER);
                header.serialize_into(black_box(&mut buf));
                let node = HeaderNode::from_slice(black_box(&buf)).unwrap();
                black_box(node)
            })
        },
    );

    // Benchmark serialize + deserialize round trip (alloc + encode + decode)
    group.bench_function(BenchmarkId::new("round_trip", header_size_bytes), |b| {
        b.iter(|| {
            let mut bytes = [0u8; 52];
            black_box(&HEADER).serialize_into(black_box(&mut bytes));
            // Black-box the bytes to prevent the compiler from fusing serialize+deserialize
            let node = HeaderNode::from_slice(black_box(&bytes)).unwrap();
            black_box(node)
        })
    });

    group.finish();
}

criterion_group!(benches, header_node_benchmarks);
criterion_main!(benches);
