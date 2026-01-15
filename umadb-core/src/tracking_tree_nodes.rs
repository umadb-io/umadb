use crate::common::Position;
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
        // layout:
        // u8: node version (1 for sorted keys)
        // u32: key_count
        // repeated key_count times: u32 key_len + [u8; key_len]
        // repeated key_count times: u64 position
        let mut size = 1 + 4; // version + count
        for k in &self.keys {
            size += 4 + k.as_bytes().len();
        }
        size += 8 * self.values.len();
        size
    }

    pub fn serialize_into(&self, buf: &mut [u8]) -> usize {
        let needed = self.calc_serialized_size();
        assert!(buf.len() >= needed, "buffer too small for TrackingLeafNode");
        buf[0] = 1; // version >=1 means keys are sorted
        LittleEndian::write_u32(&mut buf[1..5], self.keys.len() as u32);
        let mut off = 5;
        // Keys are expected to be maintained in sorted order by the node
        for k in &self.keys {
            let kb = k.as_bytes();
            LittleEndian::write_u32(&mut buf[off..off + 4], kb.len() as u32);
            off += 4;
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
        if slice.len() < 5 {
            return Err(DCBError::DeserializationError(
                "tracking leaf too small".to_string(),
            ));
        }
        let ver = slice[0];
        let count = LittleEndian::read_u32(&slice[1..5]) as usize;
        let mut off = 5;
        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            if off + 4 > slice.len() {
                return Err(DCBError::DeserializationError(
                    "tracking leaf truncated klen".to_string(),
                ));
            }
            let klen = LittleEndian::read_u32(&slice[off..off + 4]) as usize;
            off += 4;
            if off + klen > slice.len() {
                return Err(DCBError::DeserializationError(
                    "tracking leaf truncated key".to_string(),
                ));
            }
            let k = std::str::from_utf8(&slice[off..off + klen])
                .map_err(|e| DCBError::DeserializationError(format!("invalid utf8: {e}")))?
                .to_string();
            off += klen;
            keys.push(k);
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
}
