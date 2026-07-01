use crate::common::Position;
use crate::common::{PageID, Tsn};
use crate::slice_reader::SliceReader;
use bitflags::bitflags;
use std::io::{Cursor, Write};
use umadb_dcb::{DcbError, DcbResult};

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct HeaderFlags: u16 {
        const HAS_TRACKING_ROOT_ID = 0b0000_0001;
    }
}

// Node type definitions
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderNode {
    pub tsn: Tsn,
    pub free_lists_tree_root_id: PageID,
    pub events_tree_root_id: PageID,
    pub tags_tree_root_id: PageID,
    pub next_page_id: PageID,
    pub next_position: Position,
    /// On-disk schema version for the header node
    pub schema_version: u32,
    pub tracking_root_page_id: PageID,
}

impl Default for HeaderNode {
    fn default() -> Self {
        Self {
            tsn: Tsn(0),
            free_lists_tree_root_id: PageID(0),
            events_tree_root_id: PageID(0),
            tags_tree_root_id: PageID(0),
            next_page_id: PageID(0),
            next_position: Position(0),
            schema_version: crate::db::DB_SCHEMA_VERSION,
            tracking_root_page_id: PageID(0),
        }
    }
}

impl HeaderNode {
    /// Writes the serialized HeaderNode into the provided buffer and returns the number of bytes written (52).
    /// The buffer must be at least 52 bytes long.
    pub fn calc_serialized_size(&self) -> usize {
        let mut required_buf = 52;
        let mut flags = HeaderFlags::empty();
        if self.tracking_root_page_id != PageID(0) {
            flags |= HeaderFlags::HAS_TRACKING_ROOT_ID;
            required_buf += 8;
        }
        if !flags.is_empty() {
            required_buf += 2;
        }
        required_buf
    }

    pub fn serialize_into(&self, buf: &mut [u8]) -> DcbResult<usize> {
        let mut cursor = Cursor::new(buf);

        // 1. Write the legacy layout sequentially (first 48 bytes)
        cursor.write_all(&self.tsn.0.to_le_bytes())?;
        cursor.write_all(&self.next_page_id.0.to_le_bytes())?;
        cursor.write_all(&self.free_lists_tree_root_id.0.to_le_bytes())?;
        cursor.write_all(&self.events_tree_root_id.0.to_le_bytes())?;
        cursor.write_all(&self.tags_tree_root_id.0.to_le_bytes())?;
        cursor.write_all(&self.next_position.0.to_le_bytes())?;

        // 2. Append schema version (keeps first 48 bytes compatible)
        cursor.write_all(&self.schema_version.to_le_bytes())?;

        // 3. Determine dynamic flags
        let mut flags = HeaderFlags::empty();
        if self.tracking_root_page_id != PageID(0) {
            flags |= HeaderFlags::HAS_TRACKING_ROOT_ID;
        }

        // 4. Append extensions if necessary
        if !flags.is_empty() {
            // Write the flags themselves (2 bytes)
            cursor.write_all(&flags.bits().to_le_bytes())?;

            // Write optional fields
            if flags.contains(HeaderFlags::HAS_TRACKING_ROOT_ID) {
                cursor.write_all(&self.tracking_root_page_id.0.to_le_bytes())?;
            }
        }

        // 5. Let the cursor tell us exactly how many bytes were required
        Ok(cursor.position() as usize)
    }

    /// Deserializes a `HeaderNode` from a byte slice.
    ///
    /// This function is strictly backward-compatible. It accepts legacy 48-byte layouts
    /// and dynamically defaults newer fields to `0` or `empty` if the provided slice
    /// is shorter than the current schema.
    ///
    /// # Byte Layout
    /// **Core Header (Required - 48 bytes):**
    /// - `00..08`: `tsn` (u64)
    /// - `08..16`: `next_page_id` (u64)
    /// - `16..24`: `free_lists_tree_root_id` (u64)
    /// - `24..32`: `events_tree_root_id` (u64)
    /// - `32..40`: `tags_tree_root_id` (u64)
    /// - `40..48`: `next_position` (u64)
    ///
    /// **Extensions (Optional):**
    /// - `48..52`: `schema_version` (u32, defaults to 0)
    /// - `52..54`: `flags` (u16, defaults to empty)
    /// - `54..62`: `tracking_root_page_id` (u64, parsed only if `HAS_TRACKING_ROOT_ID` flag is set, defaults to 0)
    ///
    /// # Arguments
    /// * `slice` - The byte slice containing the serialized header data.
    ///
    /// # Errors
    /// Returns a `DcbError::DeserializationError` if:
    /// * The slice is smaller than the core 48 bytes.
    /// * Unrecognized bits are set in the `flags` field.
    /// * The `HAS_TRACKING_ROOT_ID` flag is present, but the slice is too short to contain the 8-byte ID.
    pub fn from_slice(slice: &[u8]) -> DcbResult<Self> {
        let mut reader = SliceReader::new(slice);

        // Read the core 48-byte header
        let tsn = reader.read_u64()?;
        let next_page_id = reader.read_u64()?;
        let freetree_root_id = reader.read_u64()?;
        let position_root_id = reader.read_u64()?;
        let tags_root_id = reader.read_u64()?;
        let next_position = reader.read_u64()?;

        // Read schema_version if the slice is long enough (backwards compatibility)
        let schema_version = if reader.remaining() >= 4 {
            reader.read_u32()?
        } else {
            0
        };

        // Read flags if the slice is long enough
        let flags = if reader.remaining() >= 2 {
            let bits = reader.read_u16()?;
            HeaderFlags::from_bits(bits).ok_or_else(|| {
                DcbError::DeserializationError("unknown flag bits set".to_string())
            })?
        } else {
            HeaderFlags::empty()
        };

        // Read tracking_tree_root_id conditionally based on the flag we just parsed
        let tracking_tree_root_id = if flags.contains(HeaderFlags::HAS_TRACKING_ROOT_ID) {
            reader.read_u64()?
        } else {
            0
        };

        Ok(HeaderNode {
            tsn: Tsn(tsn),
            next_page_id: PageID(next_page_id),
            free_lists_tree_root_id: PageID(freetree_root_id),
            events_tree_root_id: PageID(position_root_id),
            tags_tree_root_id: PageID(tags_root_id),
            next_position: Position(next_position),
            schema_version,
            tracking_root_page_id: PageID(tracking_tree_root_id),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_header_serialize_without_tracking_root_page_id() {
        // Create a HeaderNode with known values
        let header_node = HeaderNode {
            tsn: Tsn(42),
            next_page_id: PageID(123),
            free_lists_tree_root_id: PageID(456),
            events_tree_root_id: PageID(789),
            tags_tree_root_id: PageID(321),
            next_position: Position(9876543210),
            schema_version: crate::db::DB_SCHEMA_VERSION,
            tracking_root_page_id: PageID(0),
        };

        assert_eq!(52, header_node.calc_serialized_size());

        // Serialize the HeaderNode
        let mut serialized = [0u8; 52];
        let serialized_size = header_node.serialize_into(&mut serialized).unwrap();

        // Verify the serialized output has the correct length
        assert_eq!(52, serialized_size);

        // Verify the serialized output has the correct byte values
        // TSN(42) = 42u64 = [42, 0, 0, 0, 0, 0, 0, 0] in little-endian
        assert_eq!(&42u64.to_le_bytes(), &serialized[0..8]);

        // PageID(123) as u64
        assert_eq!(&123u64.to_le_bytes(), &serialized[8..16]);

        // PageID(456) as u64
        assert_eq!(&456u64.to_le_bytes(), &serialized[16..24]);

        // PageID(789) as u64
        assert_eq!(&789u64.to_le_bytes(), &serialized[24..32]);

        // root_tags_tree_id PageID(321) as u64
        assert_eq!(&321u64.to_le_bytes(), &serialized[32..40]);

        // next_position 9876543210u64 => little-endian bytes
        assert_eq!(&9876543210u64.to_le_bytes(), &serialized[40..48]);

        // schema_version
        assert_eq!(
            &crate::db::DB_SCHEMA_VERSION.to_le_bytes(),
            &serialized[48..52]
        );

        // Deserialize back to a HeaderNode
        let deserialized =
            HeaderNode::from_slice(&serialized).expect("Failed to deserialize HeaderNode");

        // Verify that the deserialized node matches the original
        assert_eq!(header_node.tsn, deserialized.tsn);
        assert_eq!(header_node.next_page_id, deserialized.next_page_id);
        assert_eq!(
            header_node.free_lists_tree_root_id,
            deserialized.free_lists_tree_root_id
        );
        assert_eq!(
            header_node.events_tree_root_id,
            deserialized.events_tree_root_id
        );
        assert_eq!(header_node.next_position, deserialized.next_position);
        assert_eq!(header_node.schema_version, deserialized.schema_version);
        assert_eq!(
            header_node.tracking_root_page_id,
            deserialized.tracking_root_page_id
        );
    }

    #[test]
    fn test_header_serialize_with_tracking_root_page_id() {
        // Create a HeaderNode with known values
        let header_node = HeaderNode {
            tsn: Tsn(42),
            next_page_id: PageID(123),
            free_lists_tree_root_id: PageID(456),
            events_tree_root_id: PageID(789),
            tags_tree_root_id: PageID(321),
            next_position: Position(9876543210),
            schema_version: crate::db::DB_SCHEMA_VERSION,
            tracking_root_page_id: PageID(953),
        };

        assert_eq!(62, header_node.calc_serialized_size());

        // Serialize the HeaderNode
        let mut serialized = [0u8; 62];
        let serialized_size = header_node.serialize_into(&mut serialized).unwrap();

        // Verify the serialized output has the correct length
        assert_eq!(62, serialized_size);

        // Verify the serialized output has the correct byte values
        // TSN(42) = 42u64 = [42, 0, 0, 0, 0, 0, 0, 0] in little-endian
        assert_eq!(&42u64.to_le_bytes(), &serialized[0..8]);

        // PageID(123) as u64
        assert_eq!(&123u64.to_le_bytes(), &serialized[8..16]);

        // PageID(456) as u64
        assert_eq!(&456u64.to_le_bytes(), &serialized[16..24]);

        // PageID(789) as u64
        assert_eq!(&789u64.to_le_bytes(), &serialized[24..32]);

        // root_tags_tree_id PageID(321) as u64
        assert_eq!(&321u64.to_le_bytes(), &serialized[32..40]);

        // next_position 9876543210u64 => little-endian bytes
        assert_eq!(&9876543210u64.to_le_bytes(), &serialized[40..48]);

        // schema_version
        assert_eq!(
            &crate::db::DB_SCHEMA_VERSION.to_le_bytes(),
            &serialized[48..52]
        );

        // bit flags
        assert_eq!(&1u16.to_le_bytes(), &serialized[52..54]);

        // tracking tree root ID
        assert_eq!(&953u64.to_le_bytes(), &serialized[54..62]);

        // Deserialize back to a HeaderNode
        let deserialized =
            HeaderNode::from_slice(&serialized).expect("Failed to deserialize HeaderNode");

        // Verify that the deserialized node matches the original
        assert_eq!(header_node.tsn, deserialized.tsn);
        assert_eq!(header_node.next_page_id, deserialized.next_page_id);
        assert_eq!(
            header_node.free_lists_tree_root_id,
            deserialized.free_lists_tree_root_id
        );
        assert_eq!(
            header_node.events_tree_root_id,
            deserialized.events_tree_root_id
        );
        assert_eq!(header_node.next_position, deserialized.next_position);
        assert_eq!(header_node.schema_version, deserialized.schema_version);
        assert_eq!(
            header_node.tracking_root_page_id,
            deserialized.tracking_root_page_id
        );
    }
}

#[cfg(test)]
mod header_node_legacy_tests {
    use super::*;

    #[test]
    fn test_legacy_48_byte_deserialize_sets_schema_version_0() {
        // Build a header and serialize to current 52-byte format
        let header_node = HeaderNode {
            tsn: Tsn(1),
            next_page_id: PageID(2),
            free_lists_tree_root_id: PageID(3),
            events_tree_root_id: PageID(4),
            tags_tree_root_id: PageID(5),
            next_position: Position(6),
            schema_version: crate::db::DB_SCHEMA_VERSION,
            tracking_root_page_id: PageID(0),
        };
        let mut bytes52 = [0u8; 52];
        header_node.serialize_into(&mut bytes52).unwrap();

        // Take only the first 48 bytes to simulate legacy on-disk header
        let mut bytes48 = [0u8; 48];
        bytes48.copy_from_slice(&bytes52[..48]);

        // Deserialize and verify schema_version defaults to 0
        let deserialized =
            HeaderNode::from_slice(&bytes48).expect("legacy 48-byte header should deserialize");
        assert_eq!(deserialized.tsn, Tsn(1));
        assert_eq!(deserialized.next_page_id, PageID(2));
        assert_eq!(deserialized.free_lists_tree_root_id, PageID(3));
        assert_eq!(deserialized.events_tree_root_id, PageID(4));
        assert_eq!(deserialized.tags_tree_root_id, PageID(5));
        assert_eq!(deserialized.next_position, Position(6));
        assert_eq!(deserialized.schema_version, 0);
    }
}
