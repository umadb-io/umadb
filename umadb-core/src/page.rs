use crate::common::PageID;
use crate::events_tree_nodes::EventValue;
use crate::header_node::HeaderNode;
use crate::node::Node;
use std::mem;
use std::ops::Range;
use umadb_dcb::{DcbError, DcbResult};

// Page structure
#[derive(Debug, Clone)]
pub struct Page {
    pub page_id: PageID,
    pub node: Node,
}

// Page header format: type(1) + crc(4) + len(4)
pub const PAGE_HEADER_SIZE: usize = 9;
const HEADER_LAYOUT_NODE_TYPE_BYTE: usize = 0;
const HEADER_LAYOUT_CRC_BYTES: Range<usize> = 1..5;
const HEADER_LAYOUT_BODY_LEN_BYTES: Range<usize> = 5..9;

// Implementation for Page
impl Page {
    pub fn new(page_id: PageID, node: Node) -> Self {
        Self { page_id, node }
    }

    #[inline]
    pub fn calc_serialized_size(&self) -> usize {
        // The total serialized size is the size of the page header plus the size of the serialized node
        PAGE_HEADER_SIZE + self.node.calc_serialized_size()
    }

    /// Serialized page (header + body) into `buf`, optionally zero-filling unused tail bytes.
    pub fn serialize_into_with_zero_fill(
        &self,
        buf: &mut [u8],
        zero_fill_remainder: bool,
    ) -> DcbResult<()> {
        serialize_page_into(buf, &self.node, zero_fill_remainder)?;
        Ok(())
    }

    #[inline]
    pub fn deserialize(page_id: PageID, page_data: &[u8]) -> DcbResult<Self> {
        if page_data.len() < PAGE_HEADER_SIZE {
            return Err(DcbError::DatabaseCorrupted(
                "Page data too short".to_string(),
            ));
        }

        // Extract header information with minimal bounds checks
        let header = &page_data[..PAGE_HEADER_SIZE];
        let node_type = header[HEADER_LAYOUT_NODE_TYPE_BYTE];
        let crc = u32::from_le_bytes(header[HEADER_LAYOUT_CRC_BYTES].try_into().unwrap());
        let data_len =
            u32::from_le_bytes(header[HEADER_LAYOUT_BODY_LEN_BYTES].try_into().unwrap()) as usize;

        if PAGE_HEADER_SIZE + data_len > page_data.len() {
            return Err(DcbError::DatabaseCorrupted(
                "Page data length mismatch".to_string(),
            ));
        }

        // Extract the data
        let data = &page_data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + data_len];

        // Verify CRC
        let calculated_crc = calc_crc(data);

        if calculated_crc != crc {
            return Err(DcbError::DatabaseCorrupted(format!(
                "CRC mismatch (page ID: {page_id:?})"
            )));
        }

        // Deserialize the node
        let node = Node::deserialize(node_type, data)?;

        Ok(Self { page_id, node })
    }

    #[inline]
    pub fn as_header_node(&self) -> DcbResult<&HeaderNode> {
        match &self.node {
            Node::Header(node) => Ok(node),
            _ => Err(DcbError::DatabaseCorrupted(format!(
                "Invalid header node type for page {:?}: {}",
                self.page_id,
                self.node.type_name()
            ))),
        }
    }

}

#[inline]
pub fn page_as_header_node(page: &Page) -> DcbResult<&HeaderNode> {
    page.as_header_node()
}

#[inline]
fn string_heap_bytes(s: &String) -> usize {
    s.capacity()
}

#[inline]
fn vec_heap_bytes<T>(v: &Vec<T>) -> usize {
    v.capacity() * mem::size_of::<T>()
}

#[inline]
fn event_value_approx_heap_bytes(value: &EventValue) -> usize {
    match value {
        EventValue::Inline(rec) => {
            string_heap_bytes(&rec.event_type)
                + vec_heap_bytes(&rec.data)
                + vec_heap_bytes(&rec.tags)
                + rec.tags.iter().map(|tag| string_heap_bytes(tag)).sum::<usize>()
        }
        EventValue::Overflow {
            event_type, tags, ..
        } => {
            string_heap_bytes(event_type)
                + vec_heap_bytes(tags)
                + tags.iter().map(|tag| string_heap_bytes(tag)).sum::<usize>()
        }
    }
}

#[inline]
fn node_approx_heap_bytes(node: &Node) -> usize {
    match node {
        Node::Header(_) => 0,
        Node::FreeListLeaf(n) => {
            vec_heap_bytes(&n.keys)
                + vec_heap_bytes(&n.values)
                + n.values
                    .iter()
                    .map(|v| vec_heap_bytes(&v.page_ids))
                    .sum::<usize>()
        }
        Node::FreeListInternal(n) => vec_heap_bytes(&n.keys) + vec_heap_bytes(&n.child_ids),
        Node::EventLeaf(n) => {
            vec_heap_bytes(&n.keys)
                + vec_heap_bytes(&n.values)
                + n.values
                    .iter()
                    .map(event_value_approx_heap_bytes)
                    .sum::<usize>()
        }
        Node::EventInternal(n) => vec_heap_bytes(&n.keys) + vec_heap_bytes(&n.child_ids),
        Node::EventOverflow(n) => vec_heap_bytes(&n.data),
        Node::TagsLeaf(n) => {
            vec_heap_bytes(&n.keys)
                + vec_heap_bytes(&n.values)
                + n.values
                    .iter()
                    .map(|v| vec_heap_bytes(&v.positions))
                    .sum::<usize>()
        }
        Node::TagsInternal(n) => vec_heap_bytes(&n.keys) + vec_heap_bytes(&n.child_ids),
        Node::TagLeaf(n) => vec_heap_bytes(&n.positions),
        Node::TagInternal(n) => vec_heap_bytes(&n.keys) + vec_heap_bytes(&n.child_ids),
        Node::FreeListTsnLeaf(n) => vec_heap_bytes(&n.page_ids),
        Node::FreeListTsnInternal(n) => vec_heap_bytes(&n.keys) + vec_heap_bytes(&n.child_ids),
        Node::TrackingLeaf(n) => {
            vec_heap_bytes(&n.keys)
                + n.keys.iter().map(|k| string_heap_bytes(k)).sum::<usize>()
                + vec_heap_bytes(&n.values)
        }
        Node::TrackingInternal(n) => {
            vec_heap_bytes(&n.keys)
                + n.keys.iter().map(|k| string_heap_bytes(k)).sum::<usize>()
                + vec_heap_bytes(&n.child_ids)
        }
    }
}

#[inline]
pub fn page_approx_deserialized_bytes(page: &Page) -> usize {
    mem::size_of::<Page>() + mem::size_of::<Node>() + node_approx_heap_bytes(&page.node)
}

pub fn serialize_page_into(
    buf: &mut [u8],
    node_ref: &Node,
    zero_fill_remainder: bool,
) -> DcbResult<()> {
    let body_len = serialize_page_node_into(buf, node_ref, zero_fill_remainder)?;
    serialize_page_header_into(buf, body_len, node_ref.get_type_byte());
    Ok(())
}

#[inline(always)]
fn serialize_page_node_into(
    buf: &mut [u8],
    node_ref: &Node,
    zero_fill_remainder: bool,
) -> DcbResult<usize> {
    // Serialize body into the front of the body region using the space after header
    let body_len = {
        let body_slice = &mut buf[PAGE_HEADER_SIZE..];
        node_ref.serialize_into(body_slice)?
    };

    // Zero-fill the remainder of the page after the serialized body
    let tail_start = PAGE_HEADER_SIZE + body_len;
    if zero_fill_remainder && tail_start < buf.len() {
        buf[tail_start..].fill(0);
    }
    Ok(body_len)
}

#[inline(always)]
fn serialize_page_header_into(buf: &mut [u8], body_len: usize, node_type_byte: u8) {
    // Compute CRC over the actual body bytes
    let crc = calc_crc(&buf[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + body_len]);

    // Fill page header
    buf[HEADER_LAYOUT_NODE_TYPE_BYTE] = node_type_byte;
    buf[HEADER_LAYOUT_CRC_BYTES].copy_from_slice(&crc.to_le_bytes());
    buf[HEADER_LAYOUT_BODY_LEN_BYTES].copy_from_slice(&(body_len as u32).to_le_bytes());
}

/// Calculate CRC32 checksum for data
#[inline(always)]
pub fn calc_crc(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Position;
    use crate::common::{PageID, Tsn};
    use crate::events_tree_nodes::{EventInternalNode, EventLeafNode, EventOverflowNode, EventRecord, EventValue};
    use crate::free_lists_tree_nodes::{FreeListInternalNode, FreeListLeafNode, FreeListLeafValue, FreeListTsnInternalNode, FreeListTsnLeafNode};
    use crate::header_node::HeaderNode;
    use crate::node::Node;
    use crate::tags_tree_nodes::{TagInternalNode, TagLeafNode, TagsInternalNode, TagsLeafNode, TagsLeafValue};
    use crate::tracking_tree_nodes::{TrackingInternalNode, TrackingLeafNode};
    use uuid::Uuid;

    #[test]
    fn test_page_serialization_and_size() {
        // Create a HeaderNode (simplest node type)
        let node = Node::Header(HeaderNode {
            tsn: Tsn(42),
            next_page_id: PageID(123),
            free_lists_tree_root_id: PageID(456),
            events_tree_root_id: PageID(789),
            tags_tree_root_id: PageID(1011),
            next_position: Position(1234),
            schema_version: crate::db::DB_SCHEMA_VERSION,
            tracking_root_page_id: PageID(0),
        });

        // Create a Page with the node
        let page_id = PageID(1);
        let page = Page::new(page_id, node);

        // Calculate the serialized size
        let calculated_size = page.calc_serialized_size();

        // Serialize the page into a fixed-size buffer using serialize_into_vec
        let mut page_buf = vec![0u8; crate::db::DEFAULT_PAGE_SIZE];
        serialize_page_into(&mut page_buf, &page.node, true)
            .expect("Failed to serialize page into buffer");

        // Check that the effective serialized data size (header + body_len from header) matches the calculated size
        let body_len = u32::from_le_bytes(page_buf[5..9].try_into().unwrap()) as usize;
        let effective_len = PAGE_HEADER_SIZE + body_len;
        assert_eq!(
            calculated_size, effective_len,
            "Calculated size {} should match effective serialized size {}",
            calculated_size, effective_len
        );

        // Deserialize the serialized data from the full page buffer
        let deserialized =
            Page::deserialize(page_id, &page_buf).expect("Failed to deserialize page");

        // Check that the deserialized page is the same as the original
        assert_eq!(
            page.page_id, deserialized.page_id,
            "Original page_id {:?} should match deserialized page_id {:?}",
            page.page_id, deserialized.page_id
        );
        assert_eq!(
            page.node, deserialized.node,
            "Original node {:?} should match deserialized node {:?}",
            page.node, deserialized.node
        );
    }

    fn sample_pages() -> Vec<Page> {
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
                            tags: vec![
                                "user".to_string(),
                                "session".to_string(),
                                "update".to_string(),
                            ],
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
                            tags: vec![
                                "audit".to_string(),
                                "security".to_string(),
                                "critical".to_string(),
                            ],
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

    #[test]
    fn print_page_serialized_vs_deserialized_sizes_under_4k() {
        println!("kind,serialized_bytes,approx_deserialized_bytes,ratio_deserialized_to_serialized");

        let mut sample_count = 0usize;
        for page in sample_pages() {
            let serialized = page.calc_serialized_size();
            if serialized >= 4096 {
                continue;
            }

            let approx_deserialized = page_approx_deserialized_bytes(&page);
            let ratio = approx_deserialized as f64 / serialized as f64;
            println!(
                "{},{},{},{}",
                page.node.type_name(),
                serialized,
                approx_deserialized,
                ratio
            );
            sample_count += 1;
        }

        assert!(sample_count > 0, "Expected at least one sample page under 4KiB");
    }

}
