// Public bench helpers to exercise internal APIs without exposing them in the public surface
pub mod server_helper;
pub mod bench_api {
    use std::path::Path;
    use umadb_core::common::{PageID, Position, Tsn};
    use umadb_core::db::DEFAULT_PAGE_SIZE;
    use umadb_core::events_tree_nodes::{
        EventInternalNode, EventLeafNode, EventOverflowNode, EventRecord, EventValue,
    };
    use umadb_core::free_lists_tree_nodes::{
        FreeListInternalNode, FreeListLeafNode, FreeListLeafValue, FreeListTsnInternalNode,
        FreeListTsnLeafNode,
    };
    use umadb_core::header_node::HeaderNode;
    use umadb_core::mvcc::{Mvcc, Writer};
    use umadb_core::node::Node;
    use umadb_core::page::{PAGE_HEADER_SIZE, Page};
    use umadb_core::tags_tree_nodes::{TagInternalNode, TagLeafNode, TagsInternalNode, TagsLeafNode, TagsLeafValue};
    use umadb_core::tracking_tree_nodes::{TrackingInternalNode, TrackingLeafNode};
    use umadb_dcb::DcbResult;
    use uuid::Uuid;

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

    pub fn comprehensive_page_samples() -> Vec<Page> {
        vec![
            Page::new(PageID(100), Node::Header(HeaderNode::default())),
            Page::new(
                PageID(101),
                Node::FreeListLeaf(FreeListLeafNode {
                    keys: vec![Tsn(10), Tsn(20)],
                    values: vec![
                        FreeListLeafValue {
                            page_ids: vec![PageID(1), PageID(2)],
                            root_id: PageID(0),
                        },
                        FreeListLeafValue {
                            page_ids: vec![PageID(3)],
                            root_id: PageID(99),
                        },
                    ],
                }),
            ),
            Page::new(
                PageID(102),
                Node::FreeListInternal(FreeListInternalNode {
                    keys: vec![Tsn(100), Tsn(200), Tsn(300)],
                    child_ids: vec![PageID(11), PageID(12), PageID(13), PageID(14)],
                }),
            ),
            Page::new(
                PageID(103),
                Node::EventLeaf(EventLeafNode {
                    keys: vec![Position(1), Position(2)],
                    values: vec![
                        EventValue::Inline(EventRecord {
                            event_type: "user.created".to_string(),
                            data: vec![1, 2, 3, 4, 5, 6],
                            tags: vec!["user".to_string(), "create".to_string()],
                            uuid: None,
                        }),
                        EventValue::Overflow {
                            event_type: "blob.uploaded".to_string(),
                            data_len: 8192,
                            tags: vec!["blob".to_string(), "upload".to_string()],
                            root_id: PageID(500),
                            uuid: Some(Uuid::nil()),
                        },
                    ],
                }),
            ),
            Page::new(
                PageID(104),
                Node::EventInternal(EventInternalNode {
                    keys: vec![Position(10), Position(20)],
                    child_ids: vec![PageID(21), PageID(22), PageID(23)],
                }),
            ),
            Page::new(
                PageID(105),
                Node::EventOverflow(EventOverflowNode {
                    next: PageID(0),
                    data: (0u8..64).collect(),
                }),
            ),
            Page::new(
                PageID(106),
                Node::TagsLeaf(TagsLeafNode {
                    keys: vec![[1u8; 16], [2u8; 16]],
                    values: vec![
                        TagsLeafValue {
                            root_id: PageID(0),
                            positions: vec![Position(7), Position(8), Position(9)],
                        },
                        TagsLeafValue {
                            root_id: PageID(700),
                            positions: vec![],
                        },
                    ],
                }),
            ),
            Page::new(
                PageID(107),
                Node::TagsInternal(TagsInternalNode {
                    keys: vec![[3u8; 16], [4u8; 16]],
                    child_ids: vec![PageID(31), PageID(32), PageID(33)],
                }),
            ),
            Page::new(
                PageID(108),
                Node::TagLeaf(TagLeafNode {
                    positions: vec![Position(101), Position(102), Position(103)],
                }),
            ),
            Page::new(
                PageID(109),
                Node::TagInternal(TagInternalNode {
                    keys: vec![Position(111), Position(222)],
                    child_ids: vec![PageID(41), PageID(42), PageID(43)],
                }),
            ),
            Page::new(
                PageID(110),
                Node::FreeListTsnLeaf(FreeListTsnLeafNode {
                    page_ids: vec![PageID(51), PageID(52), PageID(53)],
                }),
            ),
            Page::new(
                PageID(111),
                Node::FreeListTsnInternal(FreeListTsnInternalNode {
                    keys: vec![PageID(1), PageID(2)],
                    child_ids: vec![PageID(61), PageID(62), PageID(63)],
                }),
            ),
            Page::new(
                PageID(112),
                Node::TrackingLeaf(TrackingLeafNode {
                    keys: vec!["alpha".to_string(), "beta.source".to_string()],
                    values: vec![Position(1000), Position(1001)],
                }),
            ),
            Page::new(
                PageID(113),
                Node::TrackingInternal(TrackingInternalNode {
                    keys: vec!["m".to_string(), "t".to_string()],
                    child_ids: vec![PageID(71), PageID(72), PageID(73)],
                }),
            ),
            Page::new(
                PageID(114),
                Node::FreeListLeaf(FreeListLeafNode {
                    keys: vec![Tsn(100), Tsn(200), Tsn(300), Tsn(400), Tsn(500), Tsn(600)],
                    values: vec![
                        FreeListLeafValue {
                            page_ids: vec![
                                PageID(1001),
                                PageID(1002),
                                PageID(1003),
                                PageID(1004),
                                PageID(1005),
                                PageID(1006),
                            ],
                            root_id: PageID(900),
                        },
                        FreeListLeafValue {
                            page_ids: vec![
                                PageID(1011),
                                PageID(1012),
                                PageID(1013),
                                PageID(1014),
                                PageID(1015),
                            ],
                            root_id: PageID(901),
                        },
                        FreeListLeafValue {
                            page_ids: vec![PageID(1021), PageID(1022), PageID(1023)],
                            root_id: PageID(902),
                        },
                        FreeListLeafValue {
                            page_ids: vec![PageID(1031), PageID(1032)],
                            root_id: PageID(903),
                        },
                        FreeListLeafValue {
                            page_ids: vec![PageID(1041)],
                            root_id: PageID(904),
                        },
                        FreeListLeafValue {
                            page_ids: vec![PageID(1051), PageID(1052), PageID(1053), PageID(1054)],
                            root_id: PageID(905),
                        },
                    ],
                }),
            ),
            Page::new(
                PageID(115),
                Node::EventLeaf(EventLeafNode {
                    keys: vec![
                        Position(2001),
                        Position(2002),
                        Position(2003),
                        Position(2004),
                        Position(2005),
                    ],
                    values: vec![
                        EventValue::Inline(EventRecord {
                            event_type: "device.telemetry".to_string(),
                            data: (0u8..96).collect(),
                            tags: vec![
                                "iot".to_string(),
                                "telemetry".to_string(),
                                "sensor".to_string(),
                                "prod".to_string(),
                            ],
                            uuid: Some(Uuid::nil()),
                        }),
                        EventValue::Inline(EventRecord {
                            event_type: "user.session.updated".to_string(),
                            data: (100u8..180).collect(),
                            tags: vec!["user".to_string(), "session".to_string(), "update".to_string()],
                            uuid: None,
                        }),
                        EventValue::Overflow {
                            event_type: "blob.chunk.indexed".to_string(),
                            data_len: 16384,
                            tags: vec![
                                "blob".to_string(),
                                "chunk".to_string(),
                                "index".to_string(),
                                "cold-storage".to_string(),
                            ],
                            root_id: PageID(1500),
                            uuid: Some(Uuid::nil()),
                        },
                        EventValue::Inline(EventRecord {
                            event_type: "audit.log".to_string(),
                            data: (10u8..70).collect(),
                            tags: vec!["audit".to_string(), "security".to_string(), "critical".to_string()],
                            uuid: Some(Uuid::nil()),
                        }),
                        EventValue::Overflow {
                            event_type: "ml.feature.vector".to_string(),
                            data_len: 24000,
                            tags: vec!["ml".to_string(), "feature".to_string(), "vector".to_string()],
                            root_id: PageID(1501),
                            uuid: None,
                        },
                    ],
                }),
            ),
            Page::new(
                PageID(116),
                Node::TagsLeaf(TagsLeafNode {
                    keys: vec![[11u8; 16], [12u8; 16], [13u8; 16], [14u8; 16], [15u8; 16]],
                    values: vec![
                        TagsLeafValue {
                            root_id: PageID(2000),
                            positions: vec![
                                Position(3001),
                                Position(3002),
                                Position(3003),
                                Position(3004),
                                Position(3005),
                            ],
                        },
                        TagsLeafValue {
                            root_id: PageID(2001),
                            positions: vec![Position(3011), Position(3012), Position(3013), Position(3014)],
                        },
                        TagsLeafValue {
                            root_id: PageID(2002),
                            positions: vec![Position(3021), Position(3022), Position(3023)],
                        },
                        TagsLeafValue {
                            root_id: PageID(2003),
                            positions: vec![Position(3031), Position(3032)],
                        },
                        TagsLeafValue {
                            root_id: PageID(2004),
                            positions: vec![Position(3041), Position(3042), Position(3043), Position(3044)],
                        },
                    ],
                }),
            ),
            Page::new(
                PageID(117),
                Node::TrackingLeaf(TrackingLeafNode {
                    keys: vec![
                        "alpha.device.ingest.pipeline.stage.1".to_string(),
                        "beta.device.ingest.pipeline.stage.2".to_string(),
                        "gamma.device.ingest.pipeline.stage.3".to_string(),
                        "delta.device.ingest.pipeline.stage.4".to_string(),
                        "epsilon.device.ingest.pipeline.stage.5".to_string(),
                        "zeta.device.ingest.pipeline.stage.6".to_string(),
                    ],
                    values: vec![
                        Position(4001),
                        Position(4002),
                        Position(4003),
                        Position(4004),
                        Position(4005),
                        Position(4006),
                    ],
                }),
            ),
            Page::new(
                PageID(118),
                Node::FreeListTsnLeaf(FreeListTsnLeafNode {
                    page_ids: vec![
                        PageID(5001),
                        PageID(5002),
                        PageID(5003),
                        PageID(5004),
                        PageID(5005),
                        PageID(5006),
                        PageID(5007),
                        PageID(5008),
                        PageID(5009),
                        PageID(5010),
                    ],
                }),
            ),
        ]
    }

    pub fn comprehensive_full_page_samples() -> Vec<Page> {
        let pages = vec![
            Page::new(
                PageID(1000),
                Node::EventOverflow(EventOverflowNode {
                    next: PageID(0),
                    data: vec![0xAB; DEFAULT_PAGE_SIZE + 512],
                }),
            ),
            Page::new(
                PageID(1001),
                Node::EventLeaf(EventLeafNode {
                    keys: (0..80).map(|i| Position(i as u64)).collect(),
                    values: (0..80)
                        .map(|i| {
                            EventValue::Inline(EventRecord {
                                event_type: format!("event.type.{i}"),
                                data: vec![0xCD; 80],
                                tags: vec![
                                    "alpha".to_string(),
                                    "beta".to_string(),
                                    "gamma".to_string(),
                                ],
                                uuid: None,
                            })
                        })
                        .collect(),
                }),
            ),
            Page::new(
                PageID(1002),
                Node::FreeListLeaf(FreeListLeafNode {
                    keys: (0..128).map(|i| Tsn(i as u64)).collect(),
                    values: (0..128)
                        .map(|i| FreeListLeafValue {
                            page_ids: (0..8).map(|j| PageID((i * 8 + j) as u64)).collect(),
                            root_id: PageID(i as u64),
                        })
                        .collect(),
                }),
            ),
            Page::new(
                PageID(1003),
                Node::TagsLeaf(TagsLeafNode {
                    keys: (0..96).map(|i| [i as u8; 16]).collect(),
                    values: (0..96)
                        .map(|i| TagsLeafValue {
                            root_id: PageID(i as u64),
                            positions: (0..12)
                                .map(|j| Position((i * 100 + j) as u64))
                                .collect(),
                        })
                        .collect(),
                }),
            ),
            Page::new(
                PageID(1004),
                Node::TrackingLeaf(TrackingLeafNode {
                    keys: (0..96)
                        .map(|i| format!("tracking.key.{i}.segment.segment.segment.segment"))
                        .collect(),
                    values: (0..96).map(|i| Position(i as u64)).collect(),
                }),
            ),
            Page::new(
                PageID(1005),
                Node::TrackingInternal(TrackingInternalNode {
                    keys: (0..96)
                        .map(|i| format!("branch.key.{i}.segment.segment.segment.segment"))
                        .collect(),
                    child_ids: (0..97).map(|i| PageID(i as u64)).collect(),
                }),
            ),
        ];

        assert!(
            pages
                .iter()
                .all(|page| page.calc_serialized_size() >= DEFAULT_PAGE_SIZE),
            "Expected comprehensive full pages (>= page size)"
        );

        pages
    }

    pub fn comprehensive_full_page_serialized_samples() -> Vec<(PageID, Vec<u8>)> {
        comprehensive_full_page_samples()
            .into_iter()
            .map(|page| {
                let page_id = page.page_id;
                let mut buf = vec![0u8; page.calc_serialized_size()];
                page
                    .serialize_into_with_zero_fill(&mut buf, false)
                    .expect("Failed to serialize comprehensive full page sample");
                (page_id, buf)
            })
            .collect()
    }
}

#[cfg(test)]
mod memory_tests {
    use super::bench_api::{comprehensive_full_page_samples, comprehensive_page_samples};
    use memory_stats::memory_stats;
    use std::cmp::Ordering;
    use std::collections::HashMap;
    use std::env;
    use std::hint::black_box;
    use std::process::Command;
    use std::str;
    use std::sync::Arc;
    use sysinfo::{Pid, Process, ProcessesToUpdate, System};
    use umadb_core::page::page_approx_deserialized_bytes;

    struct PerTypeMemoryRow {
        kind: String,
        copies: usize,
        actual_extra_bytes: usize,
        approx_total_bytes: usize,
        ratio: f64,
    }

    fn mib(bytes: usize) -> f64 {
        bytes as f64 / (1024.0 * 1024.0)
    }

    fn ratio_status(ratio: f64) -> &'static str {
        if ratio <= 1.25 {
            "OK"
        } else if ratio <= 1.75 {
            "WARN"
        } else {
            "HIGH"
        }
    }

    fn get_process_memory(process: &Process) -> u64 {
        // If the process is the current one, use memory_stats for better accuracy
        if process.pid().as_u32() == std::process::id() {
            if let Some(usage) = memory_stats() {
                return usage.physical_mem as u64;
            }
        }
        process.memory()
    }

    fn measure_clone_memory_ratio(
        page: &umadb_core::page::Page,
        copies: usize,
        retained_clones: &mut Vec<Arc<umadb_core::page::Page>>,
        sys: &mut System,
        pid: Pid,
    ) -> (usize, usize, f64) {
        sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        let process_before = sys.process(pid).expect("current process not found");
        let mem_before = get_process_memory(process_before);

        let approx_one = page_approx_deserialized_bytes(page);
        let approx_total = approx_one.saturating_mul(copies);

        retained_clones.reserve(copies);
        for _ in 0..copies {
            retained_clones.push(Arc::new(page.clone()));
        }
        black_box(retained_clones);

        sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        let process_after = sys.process(pid).expect("current process not found after clone");
        let mem_after = get_process_memory(process_after);

        let actual_extra = mem_after.saturating_sub(mem_before) as usize;
        let ratio = if approx_total > 0 {
            actual_extra as f64 / approx_total as f64
        } else {
            0.0
        };

        (actual_extra, approx_total, ratio)
    }

    fn representative_page_by_type() -> Vec<umadb_core::page::Page> {
        let mut pages_by_type: HashMap<&'static str, umadb_core::page::Page> = HashMap::new();

        for page in comprehensive_page_samples() {
            let key = page.node.type_name();
            let page_approx = page_approx_deserialized_bytes(&page);
            let keep_existing = pages_by_type
                .get(key)
                .map(|existing| page_approx_deserialized_bytes(existing) >= page_approx)
                .unwrap_or(false);

            if !keep_existing {
                pages_by_type.insert(key, page);
            }
        }

        let mut pages: Vec<_> = pages_by_type.into_values().collect();
        pages.sort_by_key(|page| page.node.type_name());
        pages
    }

    fn representative_page_for_type(kind: &str) -> Option<umadb_core::page::Page> {
        representative_page_by_type()
            .into_iter()
            .find(|page| page.node.type_name() == kind)
    }

    fn target_copies_for_page(page: &umadb_core::page::Page) -> usize {
        let approx_one = page_approx_deserialized_bytes(page).max(1);
        let target_total_approx_bytes = 64usize * 1024 * 1024;
        (target_total_approx_bytes / approx_one).clamp(4_000, 120_000)
    }

    fn parse_per_type_child_output(stdout: &str) -> Option<PerTypeMemoryRow> {
        const PREFIX: &str = "UMA_PER_TYPE_RESULT\t";
        let line = stdout
            .lines()
            .find(|line| line.starts_with(PREFIX))?
            .trim();
        let mut parts = line.split('\t');

        let _prefix = parts.next()?;
        let kind = parts.next()?;
        let copies = parts.next()?.parse::<usize>().ok()?;
        let actual_extra_bytes = parts.next()?.parse::<usize>().ok()?;
        let approx_total_bytes = parts.next()?.parse::<usize>().ok()?;
        let ratio = parts.next()?.parse::<f64>().ok()?;

        Some(PerTypeMemoryRow {
            kind: kind.to_string(),
            copies,
            actual_extra_bytes,
            approx_total_bytes,
            ratio,
        })
    }

    fn measure_one_type_in_subprocess(kind: &'static str) -> PerTypeMemoryRow {
        let current_exe = env::current_exe().expect("failed to resolve current test binary path");
        let output = Command::new(current_exe)
            .arg("--exact")
            .arg("memory_tests::compare_actual_vs_approx_memory_per_page_type")
            .arg("--nocapture")
            .env("UMA_MEMORY_TEST_KIND", kind)
            .output()
            .expect("failed to spawn per-type memory test subprocess");

        assert!(
            output.status.success(),
            "per-type subprocess failed for {}:\nstdout:\n{}\nstderr:\n{}",
            kind,
            str::from_utf8(&output.stdout).unwrap_or("<non-utf8 stdout>"),
            str::from_utf8(&output.stderr).unwrap_or("<non-utf8 stderr>")
        );

        let stdout = str::from_utf8(&output.stdout).unwrap_or("");
        parse_per_type_child_output(stdout).unwrap_or_else(|| {
            panic!(
                "failed to parse per-type subprocess output for {}. stdout:\n{}\nstderr:\n{}",
                kind,
                stdout,
                str::from_utf8(&output.stderr).unwrap_or("<non-utf8 stderr>")
            )
        })
    }

    #[test]
    fn compare_actual_vs_approx_memory_for_1000_page_clones() {
        let mut sys = System::new_all();
        let pid = Pid::from_u32(std::process::id());

        sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        let process_before = sys.process(pid).expect("current process not found");
        let mem_before = get_process_memory(process_before);

        let base_pages = comprehensive_full_page_samples();
        let copies_per_page = 1000usize;
        let total_instances = base_pages.len() * copies_per_page;

        let mut clones = Vec::with_capacity(total_instances);
        let mut approx_total_bytes = 0usize;

        for page in &base_pages {
            let approx_one = page_approx_deserialized_bytes(page);
            approx_total_bytes = approx_total_bytes.saturating_add(approx_one * copies_per_page);

            for _ in 0..copies_per_page {
                clones.push(page.clone());
            }
        }

        // Keep clones alive and prevent optimization
        black_box(&clones);

        sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        let process_after = sys
            .process(pid)
            .expect("current process not found after clone");
        let mem_after = get_process_memory(process_after);

        let actual_extra_bytes = mem_after.saturating_sub(mem_before) as usize;
        let diff = actual_extra_bytes.abs_diff(approx_total_bytes);
        let ratio = if approx_total_bytes > 0 {
            actual_extra_bytes as f64 / approx_total_bytes as f64
        } else {
            0.0
        };

        println!(
            "pages={}, copies_per_page={}, total_instances={}\nactual_extra_bytes={}\napprox_total_bytes={}\ndiff_bytes={}\nactual/approx={:.3}",
            base_pages.len(),
            copies_per_page,
            total_instances,
            actual_extra_bytes,
            approx_total_bytes,
            diff,
            ratio
        );

        assert!(
            actual_extra_bytes > 0,
            "expected memory increase after cloning pages"
        );
    }

    #[test]
    fn compare_actual_vs_approx_memory_per_page_type() {
        if let Ok(kind) = env::var("UMA_MEMORY_TEST_KIND") {
            let page = representative_page_for_type(&kind)
                .unwrap_or_else(|| panic!("unknown page type requested: {}", kind));
            let copies = target_copies_for_page(&page);
            let mut retained_clones: Vec<Arc<umadb_core::page::Page>> = Vec::new();
            let mut sys = System::new_all();
            let pid = Pid::from_u32(std::process::id());
            let (actual_extra_bytes, approx_total_bytes, ratio) =
                measure_clone_memory_ratio(&page, copies, &mut retained_clones, &mut sys, pid);

            println!(
                "UMA_PER_TYPE_RESULT\t{}\t{}\t{}\t{}\t{}",
                page.node.type_name(),
                copies,
                actual_extra_bytes,
                approx_total_bytes,
                ratio
            );

            assert!(
                approx_total_bytes > 0,
                "expected non-zero approx bytes for type {}",
                page.node.type_name()
            );
            assert!(
                ratio.is_finite() && ratio < 12.0,
                "unexpected actual/approx ratio for {}: {:.3}",
                page.node.type_name(),
                ratio
            );
            return;
        }

        let pages = representative_page_by_type();
        assert!(!pages.is_empty(), "expected representative pages for all node types");

        let mut rows: Vec<PerTypeMemoryRow> = Vec::with_capacity(pages.len());

        let mut non_zero_increase_types = 0usize;
        let mut measured_types = 0usize;

        for page in pages {
            let row = measure_one_type_in_subprocess(page.node.type_name());
            rows.push(row);

            let latest_row = rows.last().expect("row just pushed must exist");

            measured_types += 1;
            if latest_row.actual_extra_bytes > 0 {
                non_zero_increase_types += 1;
            }
        }

        rows.sort_by(|a, b| b.ratio.partial_cmp(&a.ratio).unwrap_or(Ordering::Equal));

        println!(
            "{:<20} {:>8} {:>12} {:>12} {:>8} {:>8}",
            "Type", "Copies", "Actual MiB", "Approx MiB", "Ratio", "Status"
        );
        println!("{}", "-".repeat(78));
        for row in &rows {
            println!(
                "{:<20} {:>8} {:>12.2} {:>12.2} {:>7.2}x {:>8}",
                row.kind,
                row.copies,
                mib(row.actual_extra_bytes),
                mib(row.approx_total_bytes),
                row.ratio,
                ratio_status(row.ratio)
            );
        }

        let mut sorted_ratios: Vec<f64> = rows.iter().map(|r| r.ratio).collect();
        sorted_ratios.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        let median_ratio = if sorted_ratios.is_empty() {
            0.0
        } else if sorted_ratios.len() % 2 == 0 {
            let right = sorted_ratios.len() / 2;
            let left = right - 1;
            (sorted_ratios[left] + sorted_ratios[right]) / 2.0
        } else {
            sorted_ratios[sorted_ratios.len() / 2]
        };
        let avg_ratio = if rows.is_empty() {
            0.0
        } else {
            rows.iter().map(|r| r.ratio).sum::<f64>() / rows.len() as f64
        };
        let high_count = rows.iter().filter(|r| ratio_status(r.ratio) == "HIGH").count();
        if let Some(worst) = rows.first() {
            println!(
                "summary: worst={} ({:.2}x), median={:.2}x, avg={:.2}x, high={}/{}",
                worst.kind,
                worst.ratio,
                median_ratio,
                avg_ratio,
                high_count,
                rows.len()
            );
            for (i, row) in rows.iter().take(3).enumerate() {
                println!(
                    "top{}: {} ratio={:.2}x actual={:.2}MiB approx={:.2}MiB status={}",
                    i + 1,
                    row.kind,
                    row.ratio,
                    mib(row.actual_extra_bytes),
                    mib(row.approx_total_bytes),
                    ratio_status(row.ratio)
                );
            }
        }

        assert!(
            non_zero_increase_types >= measured_types / 2,
            "expected measurable memory increase for most page types; got {non_zero_increase_types}/{measured_types}"
        );
    }
}
