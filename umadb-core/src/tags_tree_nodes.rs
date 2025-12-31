use crate::common::{PageID, Position};
use byteorder::{ByteOrder, LittleEndian};
use std::sync::atomic::{AtomicUsize, Ordering};
use umadb_dcb::{DCBError, DCBResult};

/// Length in bytes of the hashed tag key used in-memory for TagHash
pub const TAG_HASH_LEN: usize = 16;

/// Alias for the fixed-size tag hash
pub type TagHash = [u8; TAG_HASH_LEN];

// Runtime-configurable on-disk tag key width (in bytes). Defaults to 16.
static TAG_KEY_WIDTH_BYTES: AtomicUsize = AtomicUsize::new(TAG_HASH_LEN);

#[inline]
pub fn get_tag_key_width() -> usize {
    TAG_KEY_WIDTH_BYTES.load(Ordering::Relaxed)
}

#[inline]
pub fn set_tag_key_width(width: usize) {
    // Allowed values: 4 (legacy schema 0) or 16 (current). Others are accepted but unsupported.
    TAG_KEY_WIDTH_BYTES.store(width, Ordering::Relaxed);
}

#[inline]
pub fn normalize_tag_hash_for_current_width(mut hash: TagHash) -> TagHash {
    let keyw = get_tag_key_width();
    if keyw < TAG_HASH_LEN {
        for b in &mut hash[keyw..] {
            *b = 0;
        }
    }
    hash
}

// ========================= Tag Index (by tag-hash) =========================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagsLeafValue {
    // PageID(0) indicates there is no per-tag position tree for this tag
    pub root_id: PageID,
    // Positions stored directly in the leaf when no tree is present
    pub positions: Vec<Position>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagsLeafNode {
    pub keys: Vec<TagHash>,
    pub values: Vec<TagsLeafValue>,
}

impl TagsLeafNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len + keys + values
        let keyw = get_tag_key_width();
        let mut total = 2 + self.keys.len() * keyw;
        for v in &self.values {
            // root_id (8 bytes)
            total += 8;
            total += 2; // positions len
            total += v.positions.len() * 8; // each Position is 8 bytes
        }
        total
    }

    /// No-allocation serialization into the provided buffer. Returns bytes written.
    pub fn serialize_into(&self, buf: &mut [u8]) -> usize {
        let mut i = 0usize;
        // keys_len
        let klen = self.keys.len() as u16;
        buf[i..i + 2].copy_from_slice(&klen.to_le_bytes());
        i += 2;
        // keys
        let keyw = get_tag_key_width();
        for key in &self.keys {
            buf[i..i + keyw].copy_from_slice(&key[..keyw]);
            i += keyw;
        }
        // values
        for v in &self.values {
            // root_id
            buf[i..i + 8].copy_from_slice(&v.root_id.0.to_le_bytes());
            i += 8;
            // positions length
            let plen = v.positions.len() as u16;
            buf[i..i + 2].copy_from_slice(&plen.to_le_bytes());
            i += 2;
            // positions
            for pos in &v.positions {
                buf[i..i + 8].copy_from_slice(&pos.0.to_le_bytes());
                i += 8;
            }
        }
        i
    }

    pub fn from_slice(slice: &[u8]) -> DCBResult<Self> {
        if slice.len() < 2 {
            return Err(DCBError::DeserializationError(format!(
                "Expected at least 2 bytes, got {}",
                slice.len()
            )));
        }

        // keys_len
        let keys_len = LittleEndian::read_u16(&slice[0..2]) as usize;

        // keys (runtime width)
        let keyw = get_tag_key_width();
        let keys_bytes = 2 + keys_len * keyw;
        if slice.len() < keys_bytes {
            return Err(DCBError::DeserializationError(format!(
                "Expected at least {} bytes for keys, got {}",
                keys_bytes,
                slice.len()
            )));
        }

        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let start = 2 + i * keyw;
            let mut key = [0u8; TAG_HASH_LEN];
            // copy the on-disk width and leave the rest as zeros (legacy schema 0)
            key[..keyw].copy_from_slice(&slice[start..start + keyw]);
            keys.push(key);
        }

        // values
        let mut values = Vec::with_capacity(keys_len);
        let mut offset = keys_bytes;
        for _ in 0..keys_len {
            if offset + 10 > slice.len() {
                return Err(DCBError::DeserializationError(
                    "Unexpected end of data while reading value header".to_string(),
                ));
            }
            // root_id (8 bytes)
            let root_id_u64 = LittleEndian::read_u64(&slice[offset..offset + 8]);
            let root_id = PageID(root_id_u64);
            offset += 8;
            // positions len
            let positions_len = LittleEndian::read_u16(&slice[offset..offset + 2]) as usize;
            offset += 2;

            // positions
            let need = positions_len * 8;
            if offset + need > slice.len() {
                return Err(DCBError::DeserializationError(
                    "Unexpected end of data while reading positions".to_string(),
                ));
            }
            let mut positions = Vec::with_capacity(positions_len);
            for i in 0..positions_len {
                let p = offset + i * 8;
                let pos = LittleEndian::read_u64(&slice[p..p + 8]);
                positions.push(Position(pos));
            }
            offset += need;

            values.push(TagsLeafValue { root_id, positions });
        }

        Ok(TagsLeafNode { keys, values })
    }

    pub fn pop_last_key_and_value(&mut self) -> DCBResult<(TagHash, TagsLeafValue)> {
        let last_key = self
            .keys
            .pop()
            .ok_or_else(|| DCBError::DeserializationError("No keys to pop".to_string()))?;
        let last_value = self
            .values
            .pop()
            .ok_or_else(|| DCBError::DeserializationError("No values to pop".to_string()))?;
        Ok((last_key, last_value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagsInternalNode {
    pub keys: Vec<TagHash>,
    pub child_ids: Vec<PageID>,
}

impl TagsInternalNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len + keys + child_ids (no len field, keys_len+1 implied)
        let keyw = get_tag_key_width();
        2 + (self.keys.len() * keyw) + (self.child_ids.len() * 8)
    }

    /// No-allocation serialization into the provided buffer. Returns bytes written.
    pub fn serialize_into(&self, buf: &mut [u8]) -> usize {
        let mut i = 0usize;
        // keys_len
        let klen = self.keys.len() as u16;
        buf[i..i + 2].copy_from_slice(&klen.to_le_bytes());
        i += 2;
        // keys
        let keyw = get_tag_key_width();
        for key in &self.keys {
            buf[i..i + keyw].copy_from_slice(&key[..keyw]);
            i += keyw;
        }
        // child ids (len implied as keys_len + 1)
        for id in &self.child_ids {
            buf[i..i + 8].copy_from_slice(&id.0.to_le_bytes());
            i += 8;
        }
        i
    }

    pub fn from_slice(slice: &[u8]) -> DCBResult<Self> {
        if slice.len() < 2 {
            return Err(DCBError::DeserializationError(format!(
                "Expected at least 2 bytes, got {}",
                slice.len()
            )));
        }
        let keys_len = LittleEndian::read_u16(&slice[0..2]) as usize;
        let keyw = get_tag_key_width();
        let keys_bytes = 2 + keys_len * keyw;
        if slice.len() < keys_bytes {
            return Err(DCBError::DeserializationError(format!(
                "Expected at least {} bytes for keys, got {}",
                keys_bytes,
                slice.len()
            )));
        }
        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let start = 2 + i * keyw;
            let mut key = [0u8; TAG_HASH_LEN];
            key[..keyw].copy_from_slice(&slice[start..start + keyw]);
            keys.push(key);
        }

        let child_ids_len = keys_len + 1;
        let need = child_ids_len * 8;
        if slice.len() < keys_bytes + need {
            return Err(DCBError::DeserializationError(format!(
                "Expected at least {} bytes for child_ids, got {}",
                keys_bytes + need,
                slice.len()
            )));
        }
        let mut child_ids = Vec::with_capacity(child_ids_len);
        for i in 0..child_ids_len {
            let p = keys_bytes + i * 8;
            let id = LittleEndian::read_u64(&slice[p..p + 8]);
            child_ids.push(PageID(id));
        }

        Ok(TagsInternalNode { keys, child_ids })
    }

    pub fn replace_last_child_id(&mut self, old_id: PageID, new_id: PageID) -> DCBResult<()> {
        let last_idx = self.child_ids.len() - 1;
        if self.child_ids[last_idx] == old_id {
            self.child_ids[last_idx] = new_id;
            Ok(())
        } else {
            Err(DCBError::DatabaseCorrupted("Child ID mismatch".to_string()))
        }
    }

    pub fn append_promoted_key_and_page_id(
        &mut self,
        promoted_key: TagHash,
        promoted_page_id: PageID,
    ) -> DCBResult<()> {
        self.keys.push(promoted_key);
        self.child_ids.push(promoted_page_id);
        Ok(())
    }

    pub(crate) fn split_off(&mut self) -> DCBResult<(TagHash, Vec<TagHash>, Vec<PageID>)> {
        // Split by moving half of the child_ids to a new node.
        // Promote the separator key which is the minimum key of the new right subtree.
        let total_children = self.child_ids.len();
        if total_children < 4 || self.keys.len() + 1 != total_children {
            return Err(DCBError::DatabaseCorrupted(
                "Cannot split internal node with insufficient arity".to_string(),
            ));
        }
        let mid = total_children / 2; // number of children to keep on the left
        // The promoted key is the minimum key in the new right subtree, which is keys[mid - 1]
        let promoted_key = self.keys[mid - 1];
        // Right side keys: those corresponding to the right children (excluding the promoted key)
        let new_keys = self.keys.split_off(mid);
        // Now truncate left keys to exclude the promoted key and any moved to the right
        self.keys.truncate(mid - 1);
        // Split child IDs: move right half to the new node
        let new_child_ids = self.child_ids.split_off(mid);
        Ok((promoted_key, new_keys, new_child_ids))
    }
}

// ========================= Tag position subtree =========================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagLeafNode {
    pub positions: Vec<Position>,
}

impl TagLeafNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 2 for positions_len + positions
        2 + (self.positions.len() * 8)
    }

    /// No-allocation serialization into the provided buffer. Returns bytes written.
    pub fn serialize_into(&self, buf: &mut [u8]) -> usize {
        let mut i = 0usize;
        let plen = self.positions.len() as u16;
        buf[i..i + 2].copy_from_slice(&plen.to_le_bytes());
        i += 2;
        for pos in &self.positions {
            buf[i..i + 8].copy_from_slice(&pos.0.to_le_bytes());
            i += 8;
        }
        i
    }

    pub fn from_slice(slice: &[u8]) -> DCBResult<Self> {
        if slice.len() < 2 {
            return Err(DCBError::DeserializationError(format!(
                "Expected at least 2 bytes, got {}",
                slice.len()
            )));
        }
        // positions len (first 2 bytes)
        let positions_len = LittleEndian::read_u16(&slice[0..2]) as usize;
        let need = positions_len * 8;
        if slice.len() < 2 + need {
            return Err(DCBError::DeserializationError(format!(
                "Expected at least {} bytes for positions, got {}",
                2 + need,
                slice.len()
            )));
        }
        let mut positions = Vec::with_capacity(positions_len);
        for i in 0..positions_len {
            let p = 2 + i * 8;
            let v = LittleEndian::read_u64(&slice[p..p + 8]);
            positions.push(Position(v));
        }
        Ok(TagLeafNode { positions })
    }

    pub fn pop_last_position(&mut self) -> DCBResult<Position> {
        self.positions
            .pop()
            .ok_or_else(|| DCBError::DeserializationError("No positions to pop".to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagInternalNode {
    pub keys: Vec<Position>,
    pub child_ids: Vec<PageID>,
}

impl TagInternalNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len + 8 bytes per key + 8 bytes per child id (len implied)
        2 + (self.keys.len() * 8) + (self.child_ids.len() * 8)
    }

    /// No-allocation serialization into the provided buffer. Returns bytes written.
    pub fn serialize_into(&self, buf: &mut [u8]) -> usize {
        let mut i = 0usize;
        let klen = self.keys.len() as u16;
        buf[i..i + 2].copy_from_slice(&klen.to_le_bytes());
        i += 2;
        for k in &self.keys {
            buf[i..i + 8].copy_from_slice(&k.0.to_le_bytes());
            i += 8;
        }
        for id in &self.child_ids {
            buf[i..i + 8].copy_from_slice(&id.0.to_le_bytes());
            i += 8;
        }
        i
    }

    pub fn from_slice(slice: &[u8]) -> DCBResult<Self> {
        if slice.len() < 2 {
            return Err(DCBError::DeserializationError(format!(
                "Expected at least 2 bytes, got {}",
                slice.len()
            )));
        }
        let keys_len = LittleEndian::read_u16(&slice[0..2]) as usize;
        let keys_bytes = 2 + keys_len * 8;
        if slice.len() < keys_bytes {
            return Err(DCBError::DeserializationError(format!(
                "Expected at least {} bytes for keys, got {}",
                keys_bytes,
                slice.len()
            )));
        }
        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let p = 2 + i * 8;
            let v = LittleEndian::read_u64(&slice[p..p + 8]);
            keys.push(Position(v));
        }

        let child_ids_len = keys_len + 1;
        let need = child_ids_len * 8;
        if slice.len() < keys_bytes + need {
            return Err(DCBError::DeserializationError(format!(
                "Expected at least {} bytes for child_ids, got {}",
                keys_bytes + need,
                slice.len()
            )));
        }
        let mut child_ids = Vec::with_capacity(child_ids_len);
        for i in 0..child_ids_len {
            let p = keys_bytes + i * 8;
            let v = LittleEndian::read_u64(&slice[p..p + 8]);
            child_ids.push(PageID(v));
        }

        Ok(TagInternalNode { keys, child_ids })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Position, TAG_HASH_LEN, TagInternalNode, TagLeafNode, TagsInternalNode, TagsLeafNode,
        TagsLeafValue,
    };
    use crate::common::PageID;

    #[test]
    fn test_tag_leaf_node_serialize_roundtrip() {
        let leaf = TagLeafNode {
            positions: vec![Position(10), Position(20), Position(30)],
        };
        let mut ser = vec![0u8; leaf.calc_serialized_size()];
        leaf.serialize_into(&mut ser);
        let de = TagLeafNode::from_slice(&ser).unwrap();
        assert_eq!(leaf, de);
    }

    #[test]
    fn test_tag_internal_node_serialize_roundtrip() {
        let node = TagInternalNode {
            keys: vec![Position(5), Position(15), Position(25)],
            child_ids: vec![PageID(100), PageID(200), PageID(300), PageID(400)],
        };
        let mut ser = vec![0u8; node.calc_serialized_size()];
        node.serialize_into(&mut ser);
        let de = TagInternalNode::from_slice(&ser).unwrap();
        assert_eq!(node, de);
    }

    #[test]
    fn test_leaf_node_serialize_roundtrip() {
        let mut k1 = [0u8; TAG_HASH_LEN];
        let mut k2 = [0u8; TAG_HASH_LEN];
        let mut k3 = [0u8; TAG_HASH_LEN];
        k1.copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8]);
        k2.copy_from_slice(&[
            10, 20, 30, 40, 50, 60, 70, 80, 10, 20, 30, 40, 50, 60, 70, 80,
        ]);
        k3.copy_from_slice(&[
            11, 22, 33, 44, 55, 66, 77, 88, 11, 22, 33, 44, 55, 66, 77, 88,
        ]);

        let leaf = TagsLeafNode {
            keys: vec![k1, k2, k3],
            values: vec![
                TagsLeafValue {
                    root_id: PageID(0),
                    positions: vec![Position(1), Position(2), Position(3)],
                },
                TagsLeafValue {
                    root_id: PageID(123),
                    positions: vec![Position(100)],
                },
                TagsLeafValue {
                    root_id: PageID(0),
                    positions: vec![],
                },
            ],
        };

        let mut ser = vec![0u8; leaf.calc_serialized_size()];
        leaf.serialize_into(&mut ser);
        let de = TagsLeafNode::from_slice(&ser).unwrap();
        assert_eq!(leaf, de);
    }

    #[test]
    fn test_internal_node_serialize_roundtrip() {
        let mut k1 = [0u8; TAG_HASH_LEN];
        let mut k2 = [0u8; TAG_HASH_LEN];
        let mut k3 = [0u8; TAG_HASH_LEN];
        k1.copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8]);
        k2.copy_from_slice(&[
            10, 20, 30, 40, 50, 60, 70, 80, 10, 20, 30, 40, 50, 60, 70, 80,
        ]);
        k3.copy_from_slice(&[
            11, 22, 33, 44, 55, 66, 77, 88, 11, 22, 33, 44, 55, 66, 77, 88,
        ]);

        let node = TagsInternalNode {
            keys: vec![k1, k2, k3],
            child_ids: vec![PageID(100), PageID(200), PageID(300), PageID(400)],
        };

        let mut ser = vec![0u8; node.calc_serialized_size()];
        node.serialize_into(&mut ser);
        let de = TagsInternalNode::from_slice(&ser).unwrap();
        assert_eq!(node, de);
    }

    #[test]
    fn test_tag_leaf_node_empty_positions_roundtrip() {
        let leaf = TagLeafNode { positions: vec![] };
        let mut ser = vec![0u8; leaf.calc_serialized_size()];
        leaf.serialize_into(&mut ser);
        let de = TagLeafNode::from_slice(&ser).unwrap();
        assert_eq!(leaf, de);
    }

    #[test]
    fn test_tag_internal_node_empty_keys_one_child_roundtrip() {
        let node = TagInternalNode {
            keys: vec![],
            child_ids: vec![PageID(42)],
        };
        let mut ser = vec![0u8; node.calc_serialized_size()];
        node.serialize_into(&mut ser);
        let de = TagInternalNode::from_slice(&ser).unwrap();
        assert_eq!(node, de);
    }

    #[test]
    fn test_tag_leaf_node_from_slice_too_short_err() {
        // Less than 2 bytes should error
        assert!(TagLeafNode::from_slice(&[]).is_err());
        assert!(TagLeafNode::from_slice(&[0u8]).is_err());
    }

    #[test]
    fn test_tag_internal_node_from_slice_missing_children_err() {
        // keys_len = 1, provide one key but no child ids -> should error
        let mut buf = Vec::new();
        buf.extend_from_slice(&(1u16).to_le_bytes()); // keys_len = 1
        // one key (TAG_HASH_LEN bytes)
        buf.extend_from_slice(&[0u8; TAG_HASH_LEN]);
        // missing the two child ids (keys_len + 1 = 2)
        assert!(TagInternalNode::from_slice(&buf).is_err());
    }

    #[test]
    fn test_tag_leaf_node_non_empty_positions_roundtrip_and_size() {
        let leaf = TagLeafNode {
            positions: vec![Position(7), Position(9), Position(11)],
        };
        let mut ser = vec![0u8; leaf.calc_serialized_size()];
        let written = leaf.serialize_into(&mut ser);
        assert_eq!(written, leaf.calc_serialized_size());
        let de = TagLeafNode::from_slice(&ser).unwrap();
        assert_eq!(leaf, de);
    }

    #[test]
    fn test_tag_internal_node_non_empty_roundtrip_and_size() {
        let node = TagInternalNode {
            keys: vec![Position(1), Position(2), Position(3)],
            child_ids: vec![PageID(10), PageID(20), PageID(30), PageID(40)],
        };
        let mut ser = vec![0u8; node.calc_serialized_size()];
        let written = node.serialize_into(&mut ser);
        assert_eq!(written, node.calc_serialized_size());
        let de = TagInternalNode::from_slice(&ser).unwrap();
        assert_eq!(node, de);
    }
}

impl TagInternalNode {
    pub(crate) fn split_off(&mut self) -> DCBResult<(Position, Vec<Position>, Vec<PageID>)> {
        // Split by moving half of the child_ids to a new node.
        // Promote the separator key which is the minimum key of the new right subtree.
        let total_children = self.child_ids.len();
        if total_children < 4 || self.keys.len() + 1 != total_children {
            return Err(DCBError::DatabaseCorrupted(
                "Cannot split internal node with insufficient arity".to_string(),
            ));
        }
        let mid = total_children / 2; // number of children to keep on the left
        // The promoted key is the minimum key in the new right subtree, which is keys[mid - 1]
        let promoted_key = self.keys[mid - 1];
        // Right side keys: those corresponding to the right children (excluding the promoted key)
        let new_keys = self.keys.split_off(mid);
        // Now truncate left keys to exclude the promoted key and any moved to the right
        self.keys.truncate(mid - 1);
        // Split child IDs: move right half to the new node
        let new_child_ids = self.child_ids.split_off(mid);
        Ok((promoted_key, new_keys, new_child_ids))
    }
}
