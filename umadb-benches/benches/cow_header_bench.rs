use criterion::{Criterion, criterion_group, criterion_main};
use rustc_hash::FxHashMap;
use std::collections::VecDeque;
use std::hint::black_box;
use std::sync::Arc;
use umadb_core::common::{PageID, Position, Tsn};
use umadb_core::header_node::HeaderNode;
use umadb_core::node::Node;
use umadb_core::page::Page;

const HEADER_PAGE_ID_0: PageID = PageID(0);
const HEADER_PAGE_ID_1: PageID = PageID(1);


// Writer transaction
pub struct Writer {
    pub header_page: Arc<Page>,
    pub tsn: Tsn,
    pub next_page_id: PageID,
    pub free_lists_tree_root_id: PageID,
    pub events_tree_root_id: PageID,
    pub tags_tree_root_id: PageID,
    pub tracking_tree_root_id: PageID,
    pub next_position: Position,
    pub reusable_page_ids: VecDeque<(PageID, Tsn)>,
    pub freed_page_ids: VecDeque<PageID>,
    pub deserialized: FxHashMap<PageID, Arc<Page>>,
    pub dirty: FxHashMap<PageID, Page>,
    pub reused_page_ids: VecDeque<(PageID, Tsn)>,
    pub verbose: bool,
    pub page_size: usize,
    pub max_node_size: usize,
}

impl Writer {
    pub fn update_header_in_place(&self, page: &mut Page) {
        let alternate_header_page_id = if self.header_page.page_id == HEADER_PAGE_ID_0 {
            HEADER_PAGE_ID_1
        } else {
            HEADER_PAGE_ID_0
        };

        let schema_version = if let Node::Header(ref h) = self.header_page.node {
            h.schema_version
        } else {
            0
        };

        page.page_id = alternate_header_page_id;
        if let Node::Header(ref mut h) = page.node {
            h.tsn = self.tsn;
            h.free_lists_tree_root_id = self.free_lists_tree_root_id;
            h.events_tree_root_id = self.events_tree_root_id;
            h.tags_tree_root_id = self.tags_tree_root_id;
            h.next_page_id = self.next_page_id;
            h.next_position = self.next_position;
            h.schema_version = schema_version;
            h.tracking_tree_root_id = self.tracking_tree_root_id;
        } else {
            page.node = Node::Header(HeaderNode {
                tsn: self.tsn,
                free_lists_tree_root_id: self.free_lists_tree_root_id,
                events_tree_root_id: self.events_tree_root_id,
                tags_tree_root_id: self.tags_tree_root_id,
                next_page_id: self.next_page_id,
                next_position: self.next_position,
                schema_version,
                tracking_tree_root_id: self.tracking_tree_root_id,
            });
        }
    }

    pub fn cow_header_page(&self) -> Page {
        let alternate_header_page_id = if self.header_page.page_id == HEADER_PAGE_ID_0 {
            HEADER_PAGE_ID_1
        } else {
            HEADER_PAGE_ID_0
        };

        let schema_version = if let Node::Header(ref h) = self.header_page.node {
            h.schema_version
        } else {
            0
        };

        Page {
            page_id: alternate_header_page_id,
            node: Node::Header(HeaderNode {
                tsn: self.tsn,
                free_lists_tree_root_id: self.free_lists_tree_root_id,
                events_tree_root_id: self.events_tree_root_id,
                tags_tree_root_id: self.tags_tree_root_id,
                next_page_id: self.next_page_id,
                next_position: self.next_position,
                schema_version,
                tracking_tree_root_id: self.tracking_tree_root_id,
            }),
        }
    }
}

pub fn bench_cow_header(c: &mut Criterion) {
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

    let header_page = Arc::new(Page {
        page_id: HEADER_PAGE_ID_0,
        node: Node::Header(header_node),
    });

    let writer = Writer {
        header_page: header_page.clone(),
        tsn: Tsn(10),
        next_page_id: PageID(20),
        free_lists_tree_root_id: PageID(30),
        events_tree_root_id: PageID(40),
        tags_tree_root_id: PageID(50),
        tracking_tree_root_id: PageID(60),
        next_position: Position(70),
        reusable_page_ids: VecDeque::new(),
        freed_page_ids: VecDeque::new(),
        deserialized: FxHashMap::default(),
        dirty: FxHashMap::default(),
        reused_page_ids: VecDeque::new(),
        verbose: false,
        page_size: 4096,
        max_node_size: 1024,
    };

    let mut preallocated_page = Arc::new(Page {
        page_id: HEADER_PAGE_ID_1,
        node: Node::Header(HeaderNode::default()),
    });

    let mut group = c.benchmark_group("writer_cow_header");

    group.bench_function("cow_strategy", |b| {
        b.iter(|| {
            let res = black_box(&writer).cow_header_page();
            black_box(res);
        })
    });

    group.bench_function("in_place_update", |b| {
        b.iter(|| {
            black_box(&writer).update_header_in_place(black_box(Arc::get_mut(&mut preallocated_page).unwrap()));
        })
    });

    group.finish();
}

criterion_group!(benches, bench_cow_header);
criterion_main!(benches);
