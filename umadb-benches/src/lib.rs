// Public bench helpers to exercise internal APIs without exposing them in the public surface
pub mod server_helper;
pub mod bench_api {
    use std::path::Path;
    use umadb_core::common::{PageID, Position};
    use umadb_core::db::DEFAULT_PAGE_SIZE;
    use umadb_core::events_tree_nodes::{EventLeafNode, EventRecord, EventValue};
    use umadb_core::mvcc::{Mvcc, Writer};
    use umadb_core::node::Node;
    use umadb_core::page::{PAGE_HEADER_SIZE, Page};
    use umadb_dcb::DcbResult;

    /// Minimal public wrapper to allow Criterion benches to measure commit paths
    pub struct BenchDb {
        pub mvcc: Mvcc,
    }

    impl BenchDb {
        pub fn new(path: &Path, page_size: usize) -> DcbResult<Self> {
            let mvcc = Mvcc::new(path, page_size, false)?;
            Ok(BenchDb { mvcc })
        }

        /// Commit with no dirty pages: exercises header write + flush.
        pub fn commit_empty(&self) -> DcbResult<()> {
            let mut w = self.mvcc.writer()?;
            self.mvcc.commit(&mut w)
        }

        pub fn writer(&self) -> Writer {
            self.mvcc.writer().unwrap()
        }

        pub fn insert_dirty_pages(&self, w: &mut Writer, n: usize) -> DcbResult<()> {
            // Populate each dirty page with an EventLeaf that has many keys/values
            const KEYS_PER_LEAF: usize = 70; // "lots" of keys/values per leaf
            const TAGS_PER: usize = 3;
            const DATA_LEN: u64 = 1024; // pretend payload size for overflow metadata

            w.dirty.clear();
            for i in 0..n {
                let id = PageID(i as u64);

                // Build keys [0..KEYS_PER_LEAF)
                let keys: Vec<Position> = (0..KEYS_PER_LEAF).map(|k| Position(k as u64)).collect();

                // Build many values; use Overflow to avoid allocating large inline payloads
                let tags: Vec<String> = (0..TAGS_PER).map(|t| format!("tag-{t}")).collect();
                let mut values = Vec::with_capacity(KEYS_PER_LEAF);
                for k in 0..KEYS_PER_LEAF {
                    // Derive a synthetic, unique-ish root_id for the overflow chain
                    let root_id = PageID(1 + (i as u64) * (KEYS_PER_LEAF as u64) + (k as u64));
                    values.push(EventValue::Overflow {
                        event_type: "ev".to_string(),
                        data_len: DATA_LEN,
                        tags: tags.clone(),
                        root_id,
                        uuid: None,
                    });
                }

                let node = Node::EventLeaf(EventLeafNode { keys, values });
                let node_size = node.calc_serialized_size();
                let max_node_size = DEFAULT_PAGE_SIZE - PAGE_HEADER_SIZE;
                if node_size > max_node_size {
                    panic!("The node {node_size} is too big (max: {max_node_size:?})");
                }
                let page = Page::new(id, node);
                w.insert_dirty(page)?;
            }
            Ok(())
        }

        pub fn commit_with_dirty(&self, w: &mut Writer) -> DcbResult<()> {
            self.mvcc.commit(w)
        }
    }

    /// Helper for Criterion to benchmark EventLeafNode inline (in-page) serialization/deserialization.
    pub struct BenchEventLeafInline {
        node: EventLeafNode,
        buf: Vec<u8>,
        last_size: usize,
    }

    impl BenchEventLeafInline {
        pub fn new(keys: usize, payload_size: usize, tags_per: usize) -> Self {
            let mut values = Vec::with_capacity(keys);
            let data = vec![0xAB; payload_size];
            let tags: Vec<String> = (0..tags_per).map(|t| format!("tag-{t}")).collect();
            for _ in 0..keys {
                values.push(EventValue::Inline(EventRecord {
                    event_type: "ev".to_string(),
                    data: data.clone(),
                    tags: tags.clone(),
                    uuid: None,
                }));
            }
            let keys_vec: Vec<Position> = (0..keys).map(|i| Position(i as u64)).collect();
            let node = EventLeafNode {
                keys: keys_vec,
                values,
            };
            let cap = node.calc_serialized_size();
            BenchEventLeafInline {
                node,
                buf: vec![0u8; cap],
                last_size: 0,
            }
        }

        pub fn serialize(&mut self) -> usize {
            let need = self.node.calc_serialized_size();
            if self.buf.len() < need {
                self.buf.resize(need, 0);
            }
            self.last_size = self.node.serialize_into(&mut self.buf);
            self.last_size
        }

        pub fn deserialize_check(&self) -> DcbResult<EventLeafNode> {
            let size = self.last_size.min(self.buf.len());
            let out = EventLeafNode::from_slice(&self.buf[..size])?;
            Ok(out)
        }
    }

    /// Helper for Criterion to benchmark EventLeafNode overflow (out-of-page) metadata serde.
    pub struct BenchEventLeafOverflow {
        node: EventLeafNode,
        buf: Vec<u8>,
        last_size: usize,
    }

    impl BenchEventLeafOverflow {
        pub fn new(keys: usize, data_len: usize, tags_per: usize) -> Self {
            let mut values = Vec::with_capacity(keys);
            let tags: Vec<String> = (0..tags_per).map(|t| format!("tag-{t}")).collect();
            for i in 0..keys {
                values.push(EventValue::Overflow {
                    event_type: "ev".to_string(),
                    data_len: data_len as u64,
                    tags: tags.clone(),
                    root_id: PageID(1 + i as u64),
                    uuid: None,
                });
            }
            let keys_vec: Vec<Position> = (0..keys).map(|i| Position(i as u64)).collect();
            let node = EventLeafNode {
                keys: keys_vec,
                values,
            };
            let cap = node.calc_serialized_size();
            BenchEventLeafOverflow {
                node,
                buf: vec![0u8; cap],
                last_size: 0,
            }
        }

        pub fn serialize(&mut self) -> usize {
            let need = self.node.calc_serialized_size();
            if self.buf.len() < need {
                self.buf.resize(need, 0);
            }
            self.last_size = self.node.serialize_into(&mut self.buf);
            self.last_size
        }

        pub fn deserialize_check(&self) -> DcbResult<EventLeafNode> {
            let size = self.last_size.min(self.buf.len());
            let out = EventLeafNode::from_slice(&self.buf[..size])?;
            Ok(out)
        }
    }
}
