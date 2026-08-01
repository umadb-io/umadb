use std::hint::black_box;
use criterion::{criterion_group, criterion_main, Criterion};
use umadb_core::common::{PageID, Position, Tsn};
use umadb_core::header_node::HeaderNode;
use umadb_core::node::Node;
use umadb_core::page::serialize_page_into;
use umadb_core::mvcc::DEFAULT_PAGE_SIZE;

pub fn bench_serialize_header(c: &mut Criterion) {
    let header_node = HeaderNode {
        tsn: Tsn(1),
        free_lists_tree_root_id: PageID(2),
        events_tree_root_id: PageID(3),
        tags_tree_root_id: PageID(4),
        next_page_id: PageID(5),
        next_position: Position(6),
        schema_version: 1,
        tracking_tree_root_id: PageID(7),
    };
    let node = Node::Header(header_node);
    let mut buf = vec![0u8; DEFAULT_PAGE_SIZE];

    let mut group = c.benchmark_group("serialize_header");

    group.bench_function("with_zero_fill", |b| {
        b.iter(|| {
            serialize_page_into(black_box(&mut buf), black_box(&node), black_box(true)).unwrap();
        })
    });

    group.bench_function("without_zero_fill", |b| {
        b.iter(|| {
            serialize_page_into(black_box(&mut buf), black_box(&node), black_box(false)).unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, bench_serialize_header);
criterion_main!(benches);
