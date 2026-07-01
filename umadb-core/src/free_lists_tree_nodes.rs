use crate::common::{PageID, Tsn};
use std::io::{Cursor, Write};
use umadb_dcb::{DcbError, DcbResult};
use crate::slice_reader::SliceReader;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FreeListLeafNode {
    pub keys: Vec<Tsn>,
    pub values: Vec<FreeListLeafValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FreeListLeafValue {
    pub page_ids: Vec<PageID>,
    pub root_id: PageID,
}

impl FreeListLeafNode {
    /// Calculates the size needed to serialize the FreeListLeafNode
    ///
    /// # Returns
    /// * `usize` - The size in bytes
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len
        let mut total_size = 2;

        // 8 bytes for each TSN in keys
        total_size += self.keys.len() * 8;

        // For each value:
        for value in &self.values {
            // 2 bytes for page_ids length
            total_size += 2;

            // 8 bytes for each PageID in page_ids
            total_size += value.page_ids.len() * 8;

            // 8 bytes for root_id (PageID), PageID(0) indicates no subtree
            total_size += 8;
        }

        total_size
    }

    pub fn serialize_into(&self, buf: &mut [u8]) -> DcbResult<usize> {
        let mut cursor = Cursor::new(buf);

        // Keys length
        let klen = self.keys.len() as u16;
        cursor.write_all(&klen.to_le_bytes())?;

        // Keys
        for key in &self.keys {
            cursor.write_all(&key.0.to_le_bytes())?;
        }

        // Values
        for value in &self.values {
            // Page IDs length
            let plen = value.page_ids.len() as u16;
            cursor.write_all(&plen.to_le_bytes())?;

            // Page IDs
            for page_id in &value.page_ids {
                cursor.write_all(&page_id.0.to_le_bytes())?;
            }

            // Root ID
            cursor.write_all(&value.root_id.0.to_le_bytes())?;
        }

        Ok(cursor.position() as usize)
    }

    /// Deserializes a `FreeListLeafNode` from a byte slice.
    ///
    /// # Byte Layout
    /// - `00..02`: `keys_len` (u16)
    /// - `02..??`: Array of `keys_len` keys (8 bytes each, mapped to `Tsn`)
    /// - `??..??`: Array of `keys_len` values, where each value consists of:
    ///   - `page_ids_len` (u16)
    ///   - Array of `page_ids_len` page IDs (8 bytes each, mapped to `PageID`)
    ///   - `root_id` (8 bytes, mapped to `PageID`; 0 indicates no subtree)
    ///
    /// # Arguments
    /// * `slice` - The byte slice containing the serialized free list leaf node data.
    ///
    /// # Errors
    /// Returns a `DcbError::DeserializationError` if the slice is truncated or too
    /// short to contain the expected lengths and fields at any point during parsing.
    pub fn from_slice(slice: &[u8]) -> DcbResult<Self> {
        let mut reader = SliceReader::new(slice);

        // Read keys length
        let keys_len = reader.read_u16()? as usize;

        // Read keys
        let mut keys = Vec::with_capacity(keys_len);
        for _ in 0..keys_len {
            keys.push(reader.read_tsn()?);
        }

        // Read values
        let mut values = Vec::with_capacity(keys_len);
        for _ in 0..keys_len {
            // Read page_ids for this value
            let page_ids_len = reader.read_u16()? as usize;
            let mut page_ids = Vec::with_capacity(page_ids_len);
            for _ in 0..page_ids_len {
                page_ids.push(reader.read_page_id()?);
            }

            // Read root_id for this value
            let root_id = reader.read_page_id()?;

            values.push(FreeListLeafValue { page_ids, root_id });
        }

        Ok(FreeListLeafNode { keys, values })
    }

    /// Returns true if calculated size with new (tsn -> [page_id]) doesn't exceed the given size
    pub fn would_fit_new_tsn_and_page_id(&self, max_node_size: usize) -> bool {
        // New pair increases size by: 8 (key) + 2 (len) + 8 (one page_id) + 8 (root_id) = 26 bytes
        self.calc_serialized_size() + 8 + 2 + 8 + 8 <= max_node_size
    }

    pub fn push_new_key_and_value(&mut self, tsn: Tsn, page_id: PageID) {
        // New TSN, add a new entry
        self.keys.push(tsn);
        self.values.push(FreeListLeafValue {
            page_ids: vec![page_id],
            root_id: PageID(0),
        });
    }

    /// Returns true if calculated size with new page_id doesn't exceed the given size
    pub fn would_fit_new_page_id(&self, max_node_size: usize) -> bool {
        // Only grows by 8 bytes for the extra PageID
        self.calc_serialized_size() + 8 <= max_node_size
    }

    pub fn push_new_page_id(&mut self, idx: usize, page_id: PageID) {
        self.values[idx].page_ids.push(page_id);
    }

    pub fn pop_last_key_and_value(&mut self) -> DcbResult<(Tsn, FreeListLeafValue)> {
        let last_key = self
            .keys
            .pop()
            .expect("FreeListLeafNode should have at least one key");
        let last_value = self
            .values
            .pop()
            .expect("FreeListLeafNode should have at least one value");
        Ok((last_key, last_value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreeListInternalNode {
    pub keys: Vec<Tsn>,
    pub child_ids: Vec<PageID>,
}

impl FreeListInternalNode {
    /// Calculates the size needed to serialize the FreeListInternalNode
    ///
    /// # Returns
    /// * `usize` - The size in bytes
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len
        let mut total_size = 2;

        // 8 bytes for each TSN in keys
        total_size += self.keys.len() * 8;

        // 2 bytes for child_ids length
        total_size += 2;

        // 8 bytes for each PageID in child_ids
        total_size += self.child_ids.len() * 8;

        total_size
    }

    pub fn serialize_into(&self, buf: &mut [u8]) -> DcbResult<usize> {
        let mut cursor = Cursor::new(buf);

        // Keys length
        let klen = self.keys.len() as u16;
        cursor.write_all(&klen.to_le_bytes())?;

        // Keys
        for key in &self.keys {
            cursor.write_all(&key.0.to_le_bytes())?;
        }

        // Child IDs length
        let clen = self.child_ids.len() as u16;
        cursor.write_all(&clen.to_le_bytes())?;

        // Child IDs
        for child_id in &self.child_ids {
            cursor.write_all(&child_id.0.to_le_bytes())?;
        }

        Ok(cursor.position() as usize)
    }
    /// Deserializes a `FreeListInternalNode` from a byte slice.
    ///
    /// # Byte Layout
    /// - `00..02`: `keys_len` (u16)
    /// - `02..??`: Array of `keys_len` keys (8 bytes each, mapped to `Tsn`)
    /// - `??..??`: `child_ids_len` (u16)
    /// - `??..??`: Array of `child_ids_len` child IDs (8 bytes each, mapped to `PageID`)
    ///
    /// # Arguments
    /// * `slice` - The byte slice containing the serialized free list internal node data.
    ///
    /// # Errors
    /// Returns a `DcbError::DeserializationError` if the slice is truncated or too
    /// short to contain the expected lengths and fields.
    pub fn from_slice(slice: &[u8]) -> DcbResult<Self> {
        let mut reader = SliceReader::new(slice);

        // Read keys length
        let keys_len = reader.read_u16()? as usize;

        // Read keys
        let mut keys = Vec::with_capacity(keys_len);
        for _ in 0..keys_len {
            keys.push(reader.read_tsn()?);
        }

        // Read child IDs length
        let child_ids_len = reader.read_u16()? as usize;

        // Read child IDs
        let mut child_ids = Vec::with_capacity(child_ids_len);
        for _ in 0..child_ids_len {
            child_ids.push(reader.read_page_id()?);
        }

        Ok(FreeListInternalNode { keys, child_ids })
    }

    pub fn replace_last_child_id(&mut self, old_id: PageID, new_id: PageID) -> DcbResult<()> {
        // Replace the last child ID.
        let last_idx = self.child_ids.len() - 1;
        if self.child_ids[last_idx] == old_id {
            self.child_ids[last_idx] = new_id;
        } else {
            return Err(DcbError::DatabaseCorrupted("Child ID mismatch".to_string()));
        }
        Ok(())
    }

    pub fn append_promoted_key_and_page_id(
        &mut self,
        promoted_key: Tsn,
        promoted_page_id: PageID,
    ) -> DcbResult<()> {
        self.keys.push(promoted_key);
        self.child_ids.push(promoted_page_id);
        Ok(())
    }

    pub(crate) fn split_off(&mut self) -> DcbResult<(Tsn, Vec<Tsn>, Vec<PageID>)> {
        let middle_idx = self.keys.len() - 2;
        let promoted_key = self.keys.remove(middle_idx);
        let new_keys = self.keys.split_off(middle_idx);
        let new_child_ids = self.child_ids.split_off(middle_idx + 1);
        Ok((promoted_key, new_keys, new_child_ids))
    }
}

// TSN-subtree: stores page IDs for a single TSN when inline list overflows
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreeListTsnLeafNode {
    pub page_ids: Vec<PageID>,
}

impl FreeListTsnLeafNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes for page_ids length + 8 bytes per page id
        2 + self.page_ids.len() * 8
    }

    pub fn serialize_into(&self, buf: &mut [u8]) -> DcbResult<usize> {
        let mut cursor = Cursor::new(buf);

        // Page IDs length
        let plen = self.page_ids.len() as u16;
        cursor.write_all(&plen.to_le_bytes())?;

        // Page IDs
        for page_id in &self.page_ids {
            cursor.write_all(&page_id.0.to_le_bytes())?;
        }

        Ok(cursor.position() as usize)
    }

    pub fn from_slice(slice: &[u8]) -> DcbResult<Self> {
        let mut reader = SliceReader::new(slice);

        // Read page_ids length
        let plen = reader.read_u16()? as usize;

        // Read page_ids (PageIDs)
        let mut page_ids = Vec::with_capacity(plen);
        for _ in 0..plen {
            page_ids.push(reader.read_page_id()?);
        }

        Ok(FreeListTsnLeafNode { page_ids })
    }

    // This isn't being used, but perhaps it could/should be...
    // /// Returns true if calculated size with new page_id doesn't exceed the given size
    // pub fn would_fit_new_page_id(&self, max_node_size: usize) -> bool {
    //     // Only grows by 8 bytes for the extra PageID
    //     self.calc_serialized_size() + 8 <= max_node_size
    // }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreeListTsnInternalNode {
    pub keys: Vec<PageID>,
    pub child_ids: Vec<PageID>,
}

impl FreeListTsnInternalNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes keys len + 8 per key + 2 bytes children len + 8 per child
        2 + self.keys.len() * 8 + 2 + self.child_ids.len() * 8
    }

    pub fn serialize_into(&self, buf: &mut [u8]) -> DcbResult<usize> {
        let mut cursor = Cursor::new(buf);

        // Keys length
        let klen = self.keys.len() as u16;
        cursor.write_all(&klen.to_le_bytes())?;

        // Keys
        for key in &self.keys {
            cursor.write_all(&key.0.to_le_bytes())?;
        }

        // Child IDs length
        let clen = self.child_ids.len() as u16;
        cursor.write_all(&clen.to_le_bytes())?;

        // Child IDs
        for child_id in &self.child_ids {
            cursor.write_all(&child_id.0.to_le_bytes())?;
        }

        Ok(cursor.position() as usize)
    }

    pub fn from_slice(slice: &[u8]) -> DcbResult<Self> {
        let mut reader = SliceReader::new(slice);

        // Read keys length
        let klen = reader.read_u16()? as usize;

        // Read keys (PageIDs)
        let mut keys = Vec::with_capacity(klen);
        for _ in 0..klen {
            keys.push(reader.read_page_id()?);
        }

        // Read child IDs length (explicitly stored for this node type)
        let clen = reader.read_u16()? as usize;

        // Read child IDs (PageIDs)
        let mut child_ids = Vec::with_capacity(clen);
        for _ in 0..clen {
            child_ids.push(reader.read_page_id()?);
        }

        Ok(FreeListTsnInternalNode { keys, child_ids })
    }

    /// Returns true if calculated size with new page_ids doesn't exceed the given size
    pub fn would_fit_new_key_and_child(&self, max_node_size: usize) -> bool {
        // Grows by 16 bytes for the extra two PageIDs
        self.calc_serialized_size() + 16 <= max_node_size
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{PageID, Tsn};
    use crate::free_lists_tree_nodes::{
        FreeListInternalNode, FreeListLeafNode, FreeListLeafValue, FreeListTsnInternalNode,
        FreeListTsnLeafNode,
    };

    #[test]
    fn test_freelist_leaf_serialize() {
        // Create a FreeListLeafNode with known values
        let leaf_node = FreeListLeafNode {
            keys: vec![Tsn(10), Tsn(20), Tsn(30)],
            values: vec![
                FreeListLeafValue {
                    page_ids: vec![PageID(100), PageID(101)],
                    root_id: PageID(200),
                },
                FreeListLeafValue {
                    page_ids: vec![PageID(102), PageID(103), PageID(104)],
                    root_id: PageID(0),
                },
                FreeListLeafValue {
                    page_ids: vec![PageID(105)],
                    root_id: PageID(300),
                },
            ],
        };

        // Serialize the FreeListLeafNode
        let mut serialized = vec![0u8; leaf_node.calc_serialized_size()];
        leaf_node.serialize_into(&mut serialized).unwrap();

        // Verify the serialized output is not empty
        assert!(!serialized.is_empty());

        // Deserialize back to a FreeListLeafNode using from_slice
        let deserialized = FreeListLeafNode::from_slice(&serialized)
            .expect("Failed to deserialize FreeListLeafNode");

        // Verify that the deserialized node matches the original
        assert_eq!(leaf_node, deserialized);

        // Verify specific properties
        assert_eq!(3, deserialized.keys.len());
        assert_eq!(3, deserialized.values.len());

        // Check keys
        assert_eq!(Tsn(10), deserialized.keys[0]);
        assert_eq!(Tsn(20), deserialized.keys[1]);
        assert_eq!(Tsn(30), deserialized.keys[2]);

        // Check first value
        assert_eq!(2, deserialized.values[0].page_ids.len());
        assert_eq!(PageID(100), deserialized.values[0].page_ids[0]);
        assert_eq!(PageID(101), deserialized.values[0].page_ids[1]);
        assert_eq!(PageID(200), deserialized.values[0].root_id);

        // Check second value
        assert_eq!(3, deserialized.values[1].page_ids.len());
        assert_eq!(PageID(102), deserialized.values[1].page_ids[0]);
        assert_eq!(PageID(103), deserialized.values[1].page_ids[1]);
        assert_eq!(PageID(104), deserialized.values[1].page_ids[2]);
        assert_eq!(PageID(0), deserialized.values[1].root_id);

        // Check third value
        assert_eq!(1, deserialized.values[2].page_ids.len());
        assert_eq!(PageID(105), deserialized.values[2].page_ids[0]);
        assert_eq!(PageID(300), deserialized.values[2].root_id);
    }

    #[test]
    fn test_freelist_internal_serialize() {
        // Create a FreeListInternalNode with known values
        let internal_node = FreeListInternalNode {
            keys: vec![Tsn(10), Tsn(20), Tsn(30)],
            child_ids: vec![PageID(100), PageID(200), PageID(300), PageID(400)],
        };

        // Serialize the FreeListInternalNode
        let mut serialized = vec![0u8; internal_node.calc_serialized_size()];
        internal_node.serialize_into(&mut serialized).unwrap();

        // Verify the serialized output is not empty
        assert!(!serialized.is_empty());

        // Verify the serialized output has the correct structure
        // First 2 bytes: keys_len (3) = [3, 0] in little-endian
        assert_eq!(&[3, 0], &serialized[0..2]);

        // Next 24 bytes: 3 TSNs (8 bytes each)
        assert_eq!(&10u64.to_le_bytes(), &serialized[2..10]);
        assert_eq!(&20u64.to_le_bytes(), &serialized[10..18]);
        assert_eq!(&30u64.to_le_bytes(), &serialized[18..26]);

        // Next 2 bytes: child_ids_len (4) = [4, 0] in little-endian
        assert_eq!(&[4, 0], &serialized[26..28]);

        // Next 32 bytes: 4 PageIDs (8 bytes each)
        assert_eq!(&100u64.to_le_bytes(), &serialized[28..36]);
        assert_eq!(&200u64.to_le_bytes(), &serialized[36..44]);
        assert_eq!(&300u64.to_le_bytes(), &serialized[44..52]);
        assert_eq!(&400u64.to_le_bytes(), &serialized[52..60]);

        // Deserialize back to a FreeListInternalNode using from_slice
        let deserialized = FreeListInternalNode::from_slice(&serialized)
            .expect("Failed to deserialize FreeListInternalNode");

        // Verify that the deserialized node matches the original
        assert_eq!(internal_node, deserialized);

        // Verify specific properties
        assert_eq!(3, deserialized.keys.len());
        assert_eq!(4, deserialized.child_ids.len());

        // Check keys
        assert_eq!(Tsn(10), deserialized.keys[0]);
        assert_eq!(Tsn(20), deserialized.keys[1]);
        assert_eq!(Tsn(30), deserialized.keys[2]);

        // Check child_ids
        assert_eq!(PageID(100), deserialized.child_ids[0]);
        assert_eq!(PageID(200), deserialized.child_ids[1]);
        assert_eq!(PageID(300), deserialized.child_ids[2]);
        assert_eq!(PageID(400), deserialized.child_ids[3]);
    }

    #[test]
    fn test_freelist_tsn_leaf_serialize() {
        // Create a FreeListTsnLeafNode with known values
        let node = FreeListTsnLeafNode {
            page_ids: vec![PageID(11), PageID(22), PageID(33), PageID(44)],
        };

        // Serialize
        let mut serialized = vec![0u8; node.calc_serialized_size()];
        node.serialize_into(&mut serialized).unwrap();

        // Validate structure: 2 bytes length, then 4 page IDs
        assert_eq!(&[4, 0], &serialized[0..2]);
        assert_eq!(&11u64.to_le_bytes(), &serialized[2..10]);
        assert_eq!(&22u64.to_le_bytes(), &serialized[10..18]);
        assert_eq!(&33u64.to_le_bytes(), &serialized[18..26]);
        assert_eq!(&44u64.to_le_bytes(), &serialized[26..34]);

        // Deserialize and round-trip compare
        let deserialized = FreeListTsnLeafNode::from_slice(&serialized)
            .expect("Failed to deserialize FreeListTsnLeafNode");
        assert_eq!(node, deserialized);
        assert_eq!(4, deserialized.page_ids.len());
        assert_eq!(PageID(11), deserialized.page_ids[0]);
        assert_eq!(PageID(22), deserialized.page_ids[1]);
        assert_eq!(PageID(33), deserialized.page_ids[2]);
        assert_eq!(PageID(44), deserialized.page_ids[3]);
    }

    #[test]
    fn test_freelist_tsn_internal_serialize() {
        // Create a FreeListTsnInternalNode with known values
        let node = FreeListTsnInternalNode {
            keys: vec![PageID(5), PageID(15), PageID(25)],
            child_ids: vec![PageID(1000), PageID(2000), PageID(3000), PageID(4000)],
        };

        // Serialize
        let mut serialized = vec![0u8; node.calc_serialized_size()];
        node.serialize_into(&mut serialized).unwrap();

        // Validate structure
        // keys len = 3
        assert_eq!(&[3, 0], &serialized[0..2]);
        assert_eq!(&5u64.to_le_bytes(), &serialized[2..10]);
        assert_eq!(&15u64.to_le_bytes(), &serialized[10..18]);
        assert_eq!(&25u64.to_le_bytes(), &serialized[18..26]);
        // children len = 4
        assert_eq!(&[4, 0], &serialized[26..28]);
        assert_eq!(&1000u64.to_le_bytes(), &serialized[28..36]);
        assert_eq!(&2000u64.to_le_bytes(), &serialized[36..44]);
        assert_eq!(&3000u64.to_le_bytes(), &serialized[44..52]);
        assert_eq!(&4000u64.to_le_bytes(), &serialized[52..60]);

        // Deserialize and round-trip compare
        let deserialized = FreeListTsnInternalNode::from_slice(&serialized)
            .expect("Failed to deserialize FreeListTsnInternalNode");
        assert_eq!(node, deserialized);
        assert_eq!(3, deserialized.keys.len());
        assert_eq!(PageID(5), deserialized.keys[0]);
        assert_eq!(PageID(15), deserialized.keys[1]);
        assert_eq!(PageID(25), deserialized.keys[2]);
        assert_eq!(4, deserialized.child_ids.len());
        assert_eq!(PageID(1000), deserialized.child_ids[0]);
        assert_eq!(PageID(2000), deserialized.child_ids[1]);
        assert_eq!(PageID(3000), deserialized.child_ids[2]);
        assert_eq!(PageID(4000), deserialized.child_ids[3]);
    }
}
