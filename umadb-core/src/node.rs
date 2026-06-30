use crate::events_tree_nodes::{EventInternalNode, EventLeafNode, EventOverflowNode};
use crate::free_lists_tree_nodes::{
    FreeListInternalNode, FreeListLeafNode, FreeListTsnInternalNode, FreeListTsnLeafNode,
};
use crate::header_node::HeaderNode;
use crate::tags_tree_nodes::{TagInternalNode, TagLeafNode, TagsInternalNode, TagsLeafNode};
use crate::tracking_tree_nodes::{TrackingInternalNode, TrackingLeafNode};
use umadb_dcb::{DcbError, DcbResult};

// Constants for serialization
const PAGE_TYPE_HEADER: u8 = b'1';
const PAGE_TYPE_FREELIST_LEAF: u8 = b'2';
const PAGE_TYPE_FREELIST_INTERNAL: u8 = b'3';
const PAGE_TYPE_EVENT_LEAF: u8 = b'4';
const PAGE_TYPE_EVENT_INTERNAL: u8 = b'5';
const PAGE_TYPE_TAGS_LEAF: u8 = b'6';
const PAGE_TYPE_TAGS_INTERNAL: u8 = b'7';
const PAGE_TYPE_TAG_LEAF: u8 = b'8';
const PAGE_TYPE_TAG_INTERNAL: u8 = b'9';
const PAGE_TYPE_EVENT_OVERFLOW: u8 = b'a';
const PAGE_TYPE_FREELIST_TSN_LEAF: u8 = b'b';
const PAGE_TYPE_FREELIST_TSN_INTERNAL: u8 = b'c';
const PAGE_TYPE_TRACKING_LEAF: u8 = b'd';
const PAGE_TYPE_TRACKING_INTERNAL: u8 = b'e';

// Enum to represent different node types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    Header(HeaderNode),
    FreeListLeaf(FreeListLeafNode),
    FreeListInternal(FreeListInternalNode),
    EventLeaf(EventLeafNode),
    EventInternal(EventInternalNode),
    EventOverflow(EventOverflowNode),
    TagsLeaf(TagsLeafNode),
    TagsInternal(TagsInternalNode),
    TagLeaf(TagLeafNode),
    TagInternal(TagInternalNode),
    FreeListTsnLeaf(FreeListTsnLeafNode),
    FreeListTsnInternal(FreeListTsnInternalNode),
    TrackingLeaf(TrackingLeafNode),
    TrackingInternal(TrackingInternalNode),
}

impl Node {
    pub fn get_type_byte(&self) -> u8 {
        match self {
            Node::Header(_) => PAGE_TYPE_HEADER,
            Node::FreeListLeaf(_) => PAGE_TYPE_FREELIST_LEAF,
            Node::FreeListInternal(_) => PAGE_TYPE_FREELIST_INTERNAL,
            Node::EventLeaf(_) => PAGE_TYPE_EVENT_LEAF,
            Node::EventInternal(_) => PAGE_TYPE_EVENT_INTERNAL,
            Node::EventOverflow(_) => PAGE_TYPE_EVENT_OVERFLOW,
            Node::TagsLeaf(_) => PAGE_TYPE_TAGS_LEAF,
            Node::TagsInternal(_) => PAGE_TYPE_TAGS_INTERNAL,
            Node::TagLeaf(_) => PAGE_TYPE_TAG_LEAF,
            Node::TagInternal(_) => PAGE_TYPE_TAG_INTERNAL,
            Node::FreeListTsnLeaf(_) => PAGE_TYPE_FREELIST_TSN_LEAF,
            Node::FreeListTsnInternal(_) => PAGE_TYPE_FREELIST_TSN_INTERNAL,
            Node::TrackingLeaf(_) => PAGE_TYPE_TRACKING_LEAF,
            Node::TrackingInternal(_) => PAGE_TYPE_TRACKING_INTERNAL,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Node::Header(_) => "Header",
            Node::FreeListLeaf(_) => "FreeListLeaf",
            Node::FreeListInternal(_) => "FreeListInternal",
            Node::EventLeaf(_) => "EventLeaf",
            Node::EventInternal(_) => "EventInternal",
            Node::EventOverflow(_) => "EventOverflow",
            Node::TagsLeaf(_) => "TagsLeaf",
            Node::TagsInternal(_) => "TagsInternal",
            Node::TagLeaf(_) => "TagLeaf",
            Node::TagInternal(_) => "TagInternal",
            Node::FreeListTsnLeaf(_) => "FreeListTsnLeaf",
            Node::FreeListTsnInternal(_) => "FreeListTsnInternal",
            Node::TrackingLeaf(_) => "TrackingLeaf",
            Node::TrackingInternal(_) => "TrackingInternal",
        }
    }

    pub fn calc_serialized_size(&self) -> usize {
        match self {
            Node::Header(node) => node.calc_serialized_size(),
            Node::FreeListLeaf(node) => node.calc_serialized_size(),
            Node::FreeListInternal(node) => node.calc_serialized_size(),
            Node::EventLeaf(node) => node.calc_serialized_size(),
            Node::EventInternal(node) => node.calc_serialized_size(),
            Node::EventOverflow(node) => node.calc_serialized_size(),
            Node::TagsLeaf(node) => node.calc_serialized_size(),
            Node::TagsInternal(node) => node.calc_serialized_size(),
            Node::TagLeaf(node) => node.calc_serialized_size(),
            Node::TagInternal(node) => node.calc_serialized_size(),
            Node::FreeListTsnLeaf(node) => node.calc_serialized_size(),
            Node::FreeListTsnInternal(node) => node.calc_serialized_size(),
            Node::TrackingLeaf(node) => node.calc_serialized_size(),
            Node::TrackingInternal(node) => node.calc_serialized_size(),
        }
    }

    /// No-allocation serialization into a provided buffer slice.
    /// Returns the number of bytes written.
    /// Implemented for key node types; for others it falls back to allocate-and-copy.
    pub fn serialize_into(&self, buf: &mut [u8]) -> DcbResult<usize> {
        match self {
            Node::Header(node) => node.serialize_into(buf),
            Node::FreeListLeaf(node) => node.serialize_into(buf),
            Node::FreeListInternal(node) => node.serialize_into(buf),
            Node::EventLeaf(node) => node.serialize_into(buf),
            Node::EventInternal(node) => node.serialize_into(buf),
            Node::EventOverflow(node) => node.serialize_into(buf),
            Node::TagsLeaf(node) => node.serialize_into(buf),
            Node::TagsInternal(node) => node.serialize_into(buf),
            Node::TagLeaf(node) => node.serialize_into(buf),
            Node::TagInternal(node) => node.serialize_into(buf),
            Node::FreeListTsnLeaf(node) => node.serialize_into(buf),
            Node::FreeListTsnInternal(node) => node.serialize_into(buf),
            Node::TrackingLeaf(node) => node.serialize_into(buf),
            Node::TrackingInternal(node) => node.serialize_into(buf),
        }
    }

    pub fn deserialize(node_type: u8, data: &[u8]) -> DcbResult<Self> {
        match node_type {
            PAGE_TYPE_HEADER => {
                let node = HeaderNode::from_slice(data)?;
                Ok(Node::Header(node))
            }
            PAGE_TYPE_FREELIST_LEAF => {
                let node = FreeListLeafNode::from_slice(data)?;
                Ok(Node::FreeListLeaf(node))
            }
            PAGE_TYPE_FREELIST_INTERNAL => {
                let node = FreeListInternalNode::from_slice(data)?;
                Ok(Node::FreeListInternal(node))
            }
            PAGE_TYPE_EVENT_LEAF => {
                let node = EventLeafNode::from_slice(data)?;
                Ok(Node::EventLeaf(node))
            }
            PAGE_TYPE_EVENT_INTERNAL => {
                let node = EventInternalNode::from_slice(data)?;
                Ok(Node::EventInternal(node))
            }
            PAGE_TYPE_EVENT_OVERFLOW => {
                let node = EventOverflowNode::from_slice(data)?;
                Ok(Node::EventOverflow(node))
            }
            PAGE_TYPE_TAGS_LEAF => {
                let node = TagsLeafNode::from_slice(data)?;
                Ok(Node::TagsLeaf(node))
            }
            PAGE_TYPE_TAGS_INTERNAL => {
                let node = TagsInternalNode::from_slice(data)?;
                Ok(Node::TagsInternal(node))
            }
            PAGE_TYPE_TAG_LEAF => {
                let node = TagLeafNode::from_slice(data)?;
                Ok(Node::TagLeaf(node))
            }
            PAGE_TYPE_TAG_INTERNAL => {
                let node = TagInternalNode::from_slice(data)?;
                Ok(Node::TagInternal(node))
            }
            PAGE_TYPE_FREELIST_TSN_LEAF => {
                let node = FreeListTsnLeafNode::from_slice(data)?;
                Ok(Node::FreeListTsnLeaf(node))
            }
            PAGE_TYPE_FREELIST_TSN_INTERNAL => {
                let node = FreeListTsnInternalNode::from_slice(data)?;
                Ok(Node::FreeListTsnInternal(node))
            }
            PAGE_TYPE_TRACKING_LEAF => {
                let node = TrackingLeafNode::from_slice(data)?;
                Ok(Node::TrackingLeaf(node))
            }
            PAGE_TYPE_TRACKING_INTERNAL => {
                let node = TrackingInternalNode::from_slice(data)?;
                Ok(Node::TrackingInternal(node))
            }
            _ => Err(DcbError::DatabaseCorrupted(format!(
                "Invalid node type: {node_type}"
            ))),
        }
    }
}
