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
        // u8: node version (0)
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
        buf[0] = 0; // version
        LittleEndian::write_u32(&mut buf[1..5], self.keys.len() as u32);
        let mut off = 5;
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
        let _ver = slice[0];
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
        Ok(Self { keys, values })
    }

    pub fn get(&self, source: &str) -> Option<Position> {
        self.keys
            .iter()
            .position(|k| k == source)
            .map(|i| self.values[i])
    }

    /// Insert or update a key with value while enforcing page size limit (no splitting).
    /// Returns Err(InternalError("not implemented: tracking split")) if it would exceed page capacity.
    pub fn upsert_no_split(
        &mut self,
        source: &str,
        pos: Position,
        page_body_capacity: usize,
    ) -> DCBResult<()> {
        if let Some(idx) = self.keys.iter().position(|k| k == source) {
            self.values[idx] = pos;
            return Ok(());
        }
        // Try appending and check size
        self.keys.push(source.to_string());
        self.values.push(pos);
        let size = self.calc_serialized_size();
        if size > page_body_capacity {
            // revert
            self.keys.pop();
            self.values.pop();
            return Err(DCBError::InternalError(
                "not implemented: tracking split".to_string(),
            ));
        }
        Ok(())
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
}
