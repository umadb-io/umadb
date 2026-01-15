use crate::common::{PageID, Position};
use byteorder::{ByteOrder, LittleEndian};
use umadb_dcb::{DCBError, DCBResult};

/// A simple leaf-only node for the tracking tree.
/// Keys are UTF-8 source strings; values are positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackingLeafNode {
    pub keys: Vec<String>,
    pub values: Vec<Position>,
}

impl TrackingLeafNode {
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn calc_serialized_size(&self) -> usize {
        // layout (v1 and newer on write):
        // u8: node version (1 for sorted keys)
        // u16: key_count
        // repeated key_count times: u16 key_len + [u8; key_len]
        // repeated key_count times: u64 position
        let mut size = 1 + 2; // version + count(u16)
        for k in &self.keys {
            size += 2 + k.as_bytes().len();
        }
        size += 8 * self.values.len();
        size
    }

    pub fn serialize_into(&self, buf: &mut [u8]) -> usize {
        let needed = self.calc_serialized_size();
        assert!(buf.len() >= needed, "buffer too small for TrackingLeafNode");
        buf[0] = 1; // version >=1 means keys are sorted
        LittleEndian::write_u16(&mut buf[1..3], self.keys.len() as u16);
        let mut off = 3;
        // Keys are expected to be maintained in sorted order by the node
        for k in &self.keys {
            let kb = k.as_bytes();
            LittleEndian::write_u16(&mut buf[off..off + 2], kb.len() as u16);
            off += 2;
            buf[off..off + kb.len()].copy_from_slice(kb);
            off += kb.len();
        }
        for v in &self.values {
            LittleEndian::write_u64(&mut buf[off..off + 8], v.0);
            off += 8;
        }
        needed
    }

    pub fn from_slice(slice: &[u8]) -> DCBResult<Self> {
        if slice.len() < 1 {
            return Err(DCBError::DeserializationError(
                "tracking leaf too small".to_string(),
            ));
        }
        let ver = slice[0];
        let (mut off, count): (usize, usize) = if ver == 0 {
            if slice.len() < 5 {
                return Err(DCBError::DeserializationError(
                    "tracking leaf too small (v0)".to_string(),
                ));
            }
            let cnt_u32 = LittleEndian::read_u32(&slice[1..5]);
            if cnt_u32 > u16::MAX as u32 {
                return Err(DCBError::DeserializationError(
                    "v0 tracking leaf count exceeds u16".to_string(),
                ));
            }
            (5, cnt_u32 as usize)
        } else {
            if slice.len() < 3 {
                return Err(DCBError::DeserializationError(
                    "tracking leaf too small (v1)".to_string(),
                ));
            }
            (3, LittleEndian::read_u16(&slice[1..3]) as usize)
        };
        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            if ver == 0 {
                if off + 4 > slice.len() {
                    return Err(DCBError::DeserializationError(
                        "tracking leaf truncated klen (v0)".to_string(),
                    ));
                }
                let klen_u32 = LittleEndian::read_u32(&slice[off..off + 4]);
                if klen_u32 > u16::MAX as u32 {
                    return Err(DCBError::DeserializationError(
                        "v0 tracking leaf key length exceeds u16".to_string(),
                    ));
                }
                let klen = klen_u32 as usize;
                off += 4;
                if off + klen > slice.len() {
                    return Err(DCBError::DeserializationError(
                        "tracking leaf truncated key (v0)".to_string(),
                    ));
                }
                let k = std::str::from_utf8(&slice[off..off + klen])
                    .map_err(|e| DCBError::DeserializationError(format!("invalid utf8: {e}")))?
                    .to_string();
                off += klen;
                keys.push(k);
            } else {
                if off + 2 > slice.len() {
                    return Err(DCBError::DeserializationError(
                        "tracking leaf truncated klen (v1)".to_string(),
                    ));
                }
                let klen = LittleEndian::read_u16(&slice[off..off + 2]) as usize;
                off += 2;
                if off + klen > slice.len() {
                    return Err(DCBError::DeserializationError(
                        "tracking leaf truncated key (v1)".to_string(),
                    ));
                }
                let k = std::str::from_utf8(&slice[off..off + klen])
                    .map_err(|e| DCBError::DeserializationError(format!("invalid utf8: {e}")))?
                    .to_string();
                off += klen;
                keys.push(k);
            }
        }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            if off + 8 > slice.len() {
                return Err(DCBError::DeserializationError(
                    "tracking leaf truncated value".to_string(),
                ));
            }
            let v = LittleEndian::read_u64(&slice[off..off + 8]);
            off += 8;
            values.push(Position(v));
        }
        if ver == 0 {
            // Old format: keys may be unsorted. Sort keys and keep values aligned.
            let mut pairs: Vec<(String, Position)> = keys.into_iter().zip(values.into_iter()).collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            let (new_keys, new_vals): (Vec<String>, Vec<Position>) = pairs.into_iter().unzip();
            Ok(Self { keys: new_keys, values: new_vals })
        } else {
            // v>=1: keys are already sorted
            Ok(Self { keys, values })
        }
    }

    pub fn get(&self, source: &str) -> Option<Position> {
        match self
            .keys
            .binary_search_by(|k| k.as_str().cmp(source))
        {
            Ok(i) => Some(self.values[i]),
            Err(_) => None,
        }
    }

    /// Insert or update a key with value while enforcing page size limit (no splitting).
    /// Returns Err(InternalError("not implemented: tracking split")) if it would exceed page capacity.
    pub fn upsert_no_split(
        &mut self,
        source: &str,
        pos: Position,
        page_body_capacity: usize,
    ) -> DCBResult<()> {
        match self.keys.binary_search_by(|k| k.as_str().cmp(source)) {
            Ok(idx) => {
                // Key exists; update value in place
                self.values[idx] = pos;
                Ok(())
            }
            Err(ins_idx) => {
                // Key not found; check capacity before inserting
                let key_len = source.as_bytes().len();
                // Check free space as key bytes length + 8 for position (as requested)
                let additional = key_len + 8;
                let current = self.calc_serialized_size();
                if current + additional > page_body_capacity {
                    return Err(DCBError::InternalError(
                        "not implemented: tracking split".to_string(),
                    ));
                }
                self.keys.insert(ins_idx, source.to_string());
                self.values.insert(ins_idx, pos);
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackingInternalNode {
    pub keys: Vec<String>,
    pub child_ids: Vec<PageID>,
}

impl TrackingInternalNode {
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            child_ids: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn calc_serialized_size(&self) -> usize {
        // layout (v1 write):
        // u8: version (1)
        // u16: key_count
        // repeated key_count times: u16 key_len + [u8; key_len]
        // repeated (key_count + 1) times: u64 child_page_id
        let mut size = 1 + 2; // version + count(u16)
        for k in &self.keys {
            size += 2 + k.as_bytes().len();
        }
        size += 8 * (self.keys.len() + 1);
        size
    }

    pub fn serialize_into(&self, buf: &mut [u8]) -> usize {
        let needed = self.calc_serialized_size();
        assert!(buf.len() >= needed, "buffer too small for TrackingInternalNode");
        buf[0] = 1; // version 1
        LittleEndian::write_u16(&mut buf[1..3], self.keys.len() as u16);
        let mut off = 3;
        for k in &self.keys {
            let kb = k.as_bytes();
            LittleEndian::write_u16(&mut buf[off..off + 2], kb.len() as u16);
            off += 2;
            buf[off..off + kb.len()].copy_from_slice(kb);
            off += kb.len();
        }
        // children count is implied as keys.len() + 1
        for id in &self.child_ids {
            LittleEndian::write_u64(&mut buf[off..off + 8], id.0);
            off += 8;
        }
        needed
    }

    pub fn from_slice(slice: &[u8]) -> DCBResult<Self> {
        if slice.len() < 1 {
            return Err(DCBError::DeserializationError(
                "tracking internal too small".to_string(),
            ));
        }
        let ver = slice[0];
        if ver == 0 {
            return Err(DCBError::DeserializationError(
                "unsupported tracking internal version 0".to_string(),
            ));
        }
        if slice.len() < 3 {
            return Err(DCBError::DeserializationError(
                "tracking internal too small (v1)".to_string(),
            ));
        }
        let count = LittleEndian::read_u16(&slice[1..3]) as usize;
        let mut off = 3;
        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            if off + 2 > slice.len() {
                return Err(DCBError::DeserializationError(
                    "tracking internal truncated klen (v1)".to_string(),
                ));
            }
            let klen = LittleEndian::read_u16(&slice[off..off + 2]) as usize;
            off += 2;
            if off + klen > slice.len() {
                return Err(DCBError::DeserializationError(
                    "tracking internal truncated key (v1)".to_string(),
                ));
            }
            let k = std::str::from_utf8(&slice[off..off + klen])
                .map_err(|e| DCBError::DeserializationError(format!("invalid utf8: {e}")))?
                .to_string();
            off += klen;
            keys.push(k);
        }
        // Now children: implied count = keys.len() + 1
        let child_count = keys.len() + 1;
        let need = off + 8 * child_count;
        if slice.len() < need {
            return Err(DCBError::DeserializationError(
                "tracking internal truncated children".to_string(),
            ));
        }
        let mut child_ids = Vec::with_capacity(child_count);
        for _ in 0..child_count {
            let v = LittleEndian::read_u64(&slice[off..off + 8]);
            off += 8;
            child_ids.push(PageID(v));
        }
        Ok(Self { keys, child_ids })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracking_leaf_roundtrip() {
        let mut node = TrackingLeafNode::new();
        node.keys = vec!["a".to_string(), "b".to_string()];
        node.values = vec![Position(1), Position(2)];
        let mut buf = vec![0u8; node.calc_serialized_size()];
        let n = node.serialize_into(&mut buf);
        assert_eq!(n, buf.len());
        let dec = TrackingLeafNode::from_slice(&buf).unwrap();
        assert_eq!(node, dec);
        assert_eq!(dec.get("a"), Some(Position(1)));
        assert_eq!(dec.get("z"), None);
    }

    #[test]
    fn test_deserialize_v0_unsorted_sorts_and_aligns() {
        // Manually craft a v0 buffer with unsorted keys: ["b", "c", "a"] and values [2, 3, 1]
        let keys = vec!["b", "c", "a"];
        let values = vec![Position(2), Position(3), Position(1)];
        // compute size for v0: 1 (ver) + 4 (count) + sum(4 + klen) + 8*count
        let key_bytes: Vec<Vec<u8>> = keys.iter().map(|k| k.as_bytes().to_vec()).collect();
        let mut size = 1 + 4;
        for kb in &key_bytes {
            size += 4 + kb.len();
        }
        size += 8 * values.len();
        let mut buf = vec![0u8; size];
        buf[0] = 0; // version 0
        LittleEndian::write_u32(&mut buf[1..5], keys.len() as u32);
        let mut off = 5;
        for kb in &key_bytes {
            LittleEndian::write_u32(&mut buf[off..off + 4], kb.len() as u32);
            off += 4;
            buf[off..off + kb.len()].copy_from_slice(kb);
            off += kb.len();
        }
        for v in &values {
            LittleEndian::write_u64(&mut buf[off..off + 8], v.0);
            off += 8;
        }
        // Now deserialize; it should sort keys and align values accordingly
        let dec = TrackingLeafNode::from_slice(&buf).unwrap();
        assert_eq!(dec.keys, vec!["a", "b", "c"]);
        assert_eq!(dec.values, vec![Position(1), Position(2), Position(3)]);
        // Binary search based get should work
        assert_eq!(dec.get("a"), Some(Position(1)));
        assert_eq!(dec.get("c"), Some(Position(3)));
        assert_eq!(dec.get("z"), None);
    }

    #[test]
    fn test_upsert_maintains_sorted_and_capacity_check() {
        let mut node = TrackingLeafNode::new();
        // small capacity; account for version+count and future entries
        let capacity = 1 + 4 + (4 + 1) + (4 + 1) + (4 + 1) + 8 * 3; // rough upper bound; we pre-check len+8 anyway
        node.upsert_no_split("b", Position(2), capacity).unwrap();
        node.upsert_no_split("a", Position(1), capacity).unwrap();
        node.upsert_no_split("c", Position(3), capacity).unwrap();
        assert_eq!(node.keys, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        assert_eq!(node.get("b"), Some(Position(2)));
        // Now try to insert with too small capacity
        let mut node2 = TrackingLeafNode::new();
        let small_capacity = 1 + 4; // too small to accept any key/value
        let err = node2.upsert_no_split("x", Position(9), small_capacity).unwrap_err();
        match err {
            DCBError::InternalError(s) => assert!(s.contains("tracking split")),
            _ => panic!("unexpected error type"),
        }
    }

    #[test]
    fn test_tracking_internal_roundtrip() {
        let node = TrackingInternalNode {
            keys: vec!["alpha".into(), "beta".into(), "gamma".into()],
            child_ids: vec![PageID(10), PageID(20), PageID(30), PageID(40)],
        };
        let mut buf = vec![0u8; node.calc_serialized_size()];
        let n = node.serialize_into(&mut buf);
        assert_eq!(n, buf.len());
        let dec = TrackingInternalNode::from_slice(&buf).unwrap();
        assert_eq!(dec, node);
    }

    #[test]
    fn test_tracking_internal_empty_keys_one_child_roundtrip() {
        let node = TrackingInternalNode {
            keys: vec![],
            child_ids: vec![PageID(123)],
        };
        let mut buf = vec![0u8; node.calc_serialized_size()];
        let n = node.serialize_into(&mut buf);
        assert_eq!(n, buf.len());
        let dec = TrackingInternalNode::from_slice(&buf).unwrap();
        assert_eq!(dec, node);
    }

    #[test]
    fn test_tracking_internal_from_slice_truncated_children_err() {
        // Build a valid buffer then truncate last 8 bytes to simulate missing child
        let node = TrackingInternalNode {
            keys: vec!["k1".into()],
            child_ids: vec![PageID(1), PageID(2)],
        };
        let mut buf = vec![0u8; node.calc_serialized_size()];
        let _ = node.serialize_into(&mut buf);
        buf.truncate(buf.len() - 4); // remove less than 8 to force error path
        let err = TrackingInternalNode::from_slice(&buf).unwrap_err();
        match err {
            DCBError::DeserializationError(_) => {}
            _ => panic!("expected DeserializationError"),
        }
    }
}
