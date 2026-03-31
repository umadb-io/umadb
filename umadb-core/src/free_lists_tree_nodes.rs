use crate::common::{PageID, Tsn};
use byteorder::{ByteOrder, LittleEndian};
use umadb_dcb::{DcbError, DcbResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreeListLeafNode {
    pub keys: Vec<Tsn>,
    pub values: Vec<FreeListLeafValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

    pub fn serialize_into(&self, buf: &mut [u8]) -> usize {
        let mut i = 0usize;
        let klen = self.keys.len() as u16;
        buf[i..i + 2].copy_from_slice(&klen.to_le_bytes());
        i += 2;
        for key in &self.keys {
            buf[i..i + 8].copy_from_slice(&key.0.to_le_bytes());
            i += 8;
        }
        for value in &self.values {
            let plen = value.page_ids.len() as u16;
            buf[i..i + 2].copy_from_slice(&plen.to_le_bytes());
            i += 2;
            for page_id in &value.page_ids {
                buf[i..i + 8].copy_from_slice(&page_id.0.to_le_bytes());
                i += 8;
            }
            buf[i..i + 8].copy_from_slice(&value.root_id.0.to_le_bytes());
            i += 8;
        }
        i
    }

    /// Creates a FreeListLeafNode from a byte slice
    ///
    /// # Arguments
    /// * `slice` - The byte slice to deserialize from
    ///
    /// # Returns
    /// * `Result<Self>` - The deserialized FreeListLeafNode or an error
    pub fn from_slice(slice: &[u8]) -> DcbResult<Self> {
        // Check if the slice has at least 2 bytes for keys_len
        if slice.len() < 2 {
            return Err(DcbError::DeserializationError(format!(
                "Expected at least 2 bytes, got {}",
                slice.len()
            )));
        }

        // Extract the length of the keys (first 2 bytes)
        let keys_len = LittleEndian::read_u16(&slice[0..2]) as usize;

        // Calculate the minimum expected size for the keys
        let min_expected_size = 2 + (keys_len * 8);
        if slice.len() < min_expected_size {
            return Err(DcbError::DeserializationError(format!(
                "Expected at least {} bytes for keys, got {}",
                min_expected_size,
                slice.len()
            )));
        }

        // Extract the keys (8 bytes each)
        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let start = 2 + (i * 8);
            let tsn = LittleEndian::read_u64(&slice[start..start + 8]);
            keys.push(Tsn(tsn));
        }

        // Extract the values
        let mut values = Vec::with_capacity(keys_len);
        let mut offset = 2 + (keys_len * 8);

        for _ in 0..keys_len {
            if offset + 2 > slice.len() {
                return Err(DcbError::DeserializationError(
                    "Unexpected end of data while reading page_ids length".to_string(),
                ));
            }

            // Extract the length of page_ids (2 bytes)
            let page_ids_len = LittleEndian::read_u16(&slice[offset..offset + 2]) as usize;
            offset += 2;

            if offset + (page_ids_len * 8) > slice.len() {
                return Err(DcbError::DeserializationError(
                    "Unexpected end of data while reading page_ids".to_string(),
                ));
            }

            // Extract the page_ids (8 bytes each)
            let mut page_ids = Vec::with_capacity(page_ids_len);
            for j in 0..page_ids_len {
                let start = offset + (j * 8);
                let page_id = LittleEndian::read_u64(&slice[start..start + 8]);
                page_ids.push(PageID(page_id));
            }
            offset += page_ids_len * 8;

            if offset + 8 > slice.len() {
                return Err(DcbError::DeserializationError(
                    "Unexpected end of data while reading root_id".to_string(),
                ));
            }

            // Extract the root_id (always 8 bytes); PageID(0) indicates no subtree
            let page_id = LittleEndian::read_u64(&slice[offset..offset + 8]);
            offset += 8;
            let root_id = PageID(page_id);

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

    pub fn serialize_into(&self, buf: &mut [u8]) -> usize {
        let mut i = 0usize;
        let klen = self.keys.len() as u16;
        buf[i..i + 2].copy_from_slice(&klen.to_le_bytes());
        i += 2;
        for key in &self.keys {
            buf[i..i + 8].copy_from_slice(&key.0.to_le_bytes());
            i += 8;
        }
        let clen = self.child_ids.len() as u16;
        buf[i..i + 2].copy_from_slice(&clen.to_le_bytes());
        i += 2;
        for child_id in &self.child_ids {
            buf[i..i + 8].copy_from_slice(&child_id.0.to_le_bytes());
            i += 8;
        }
        i
    }

    /// Creates a FreeListInternalNode from a byte slice
    ///
    /// # Arguments
    /// * `slice` - The byte slice to deserialize from
    ///
    /// # Returns
    /// * `Result<Self>` - The deserialized FreeListInternalNode or an error
    pub fn from_slice(slice: &[u8]) -> DcbResult<Self> {
        // Check if the slice has at least 2 bytes for keys_len
        if slice.len() < 2 {
            return Err(DcbError::DeserializationError(format!(
                "Expected at least 2 bytes, got {}",
                slice.len()
            )));
        }

        // Extract the length of the keys (first 2 bytes)
        let keys_len = LittleEndian::read_u16(&slice[0..2]) as usize;

        // Calculate the minimum expected size for the keys
        let min_expected_size = 2 + (keys_len * 8);
        if slice.len() < min_expected_size {
            return Err(DcbError::DeserializationError(format!(
                "Expected at least {} bytes for keys, got {}",
                min_expected_size,
                slice.len()
            )));
        }

        // Extract the keys (8 bytes each)
        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let start = 2 + (i * 8);
            let tsn = LittleEndian::read_u64(&slice[start..start + 8]);
            keys.push(Tsn(tsn));
        }

        // Extract the length of child_ids (2 bytes)
        let offset = 2 + (keys_len * 8);
        if offset + 2 > slice.len() {
            return Err(DcbError::DeserializationError(
                "Unexpected end of data while reading child_ids length".to_string(),
            ));
        }

        let child_ids_len = LittleEndian::read_u16(&slice[offset..offset + 2]) as usize;

        // Calculate the minimum expected size for the child_ids
        let min_expected_size = offset + 2 + (child_ids_len * 8);
        if slice.len() < min_expected_size {
            return Err(DcbError::DeserializationError(format!(
                "Expected at least {} bytes for child_ids, got {}",
                min_expected_size,
                slice.len()
            )));
        }

        // Extract the child_ids (8 bytes each)
        let mut child_ids = Vec::with_capacity(child_ids_len);
        for i in 0..child_ids_len {
            let start = offset + 2 + (i * 8);
            let page_id = LittleEndian::read_u64(&slice[start..start + 8]);
            child_ids.push(PageID(page_id));
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

    pub fn serialize_into(&self, dst: &mut [u8]) -> usize {
        let mut i = 0usize;
        let plen = self.page_ids.len() as u16;
        dst[i..i + 2].copy_from_slice(&plen.to_le_bytes());
        i += 2;
        for page_id in &self.page_ids {
            dst[i..i + 8].copy_from_slice(&page_id.0.to_le_bytes());
            i += 8;
        }
        i
    }

    pub fn from_slice(slice: &[u8]) -> DcbResult<Self> {
        if slice.len() < 2 {
            return Err(DcbError::DeserializationError(format!(
                "Expected at least 2 bytes, got {}",
                slice.len()
            )));
        }
        let plen = LittleEndian::read_u16(&slice[0..2]) as usize;
        let need = 2 + plen * 8;
        if slice.len() < need {
            return Err(DcbError::DeserializationError(format!(
                "Expected at least {} bytes, got {}",
                need,
                slice.len()
            )));
        }
        let mut page_ids = Vec::with_capacity(plen);
        let mut offset = 2;
        for _ in 0..plen {
            let v = LittleEndian::read_u64(&slice[offset..offset + 8]);
            page_ids.push(PageID(v));
            offset += 8;
        }
        Ok(FreeListTsnLeafNode { page_ids })
    }

    /// Returns true if calculated size with new page_id doesn't exceed the given size
    pub fn would_fit_new_page_id(&self, max_node_size: usize) -> bool {
        // Only grows by 8 bytes for the extra PageID
        self.calc_serialized_size() + 8 <= max_node_size
    }
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

    pub fn serialize_into(&self, dst: &mut [u8]) -> usize {
        let mut i = 0usize;
        let klen = self.keys.len() as u16;
        dst[i..i + 2].copy_from_slice(&klen.to_le_bytes());
        i += 2;
        for key in &self.keys {
            dst[i..i + 8].copy_from_slice(&key.0.to_le_bytes());
            i += 8;
        }
        let clen = self.child_ids.len() as u16;
        dst[i..i + 2].copy_from_slice(&clen.to_le_bytes());
        i += 2;
        for child_id in &self.child_ids {
            dst[i..i + 8].copy_from_slice(&child_id.0.to_le_bytes());
            i += 8;
        }
        i
    }

    pub fn from_slice(slice: &[u8]) -> DcbResult<Self> {
        if slice.len() < 2 {
            return Err(DcbError::DeserializationError(
                "Expected at least 2 bytes".to_string(),
            ));
        }
        let klen = LittleEndian::read_u16(&slice[0..2]) as usize;
        if slice.len() < 2 + klen * 8 + 2 {
            return Err(DcbError::DeserializationError("Data too short".to_string()));
        }
        let mut keys = Vec::with_capacity(klen);
        let mut offset = 2;
        for _ in 0..klen {
            let v = LittleEndian::read_u64(&slice[offset..offset + 8]);
            keys.push(PageID(v));
            offset += 8;
        }
        let clen = LittleEndian::read_u16(&slice[offset..offset + 2]) as usize;
        offset += 2;
        if slice.len() < offset + clen * 8 {
            return Err(DcbError::DeserializationError("Data too short".to_string()));
        }
        let mut child_ids = Vec::with_capacity(clen);
        for _ in 0..clen {
            let v = LittleEndian::read_u64(&slice[offset..offset + 8]);
            child_ids.push(PageID(v));
            offset += 8;
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
        leaf_node.serialize_into(&mut serialized);

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
        internal_node.serialize_into(&mut serialized);

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
        node.serialize_into(&mut serialized);

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
        node.serialize_into(&mut serialized);

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
