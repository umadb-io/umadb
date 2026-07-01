use uuid::Uuid;
use umadb_dcb::{DcbError, DcbResult};
use crate::common::{PageID, Position, Tsn};
use crate::tags_tree_nodes::{get_tag_key_width, TAG_HASH_LEN};

/// A zero-copy, advancing reader for byte slices.
pub struct SliceReader<'a> {
    slice: &'a [u8],
}

impl<'a> SliceReader<'a> {
    pub fn new(slice: &'a [u8]) -> Self {
        Self { slice }
    }

    /// Safely takes `len` bytes from the front and advances the internal slice.
    pub fn read_bytes(&mut self, len: usize) -> DcbResult<&'a [u8]> {
        if self.slice.len() < len {
            return Err(DcbError::DeserializationError(format!(
                "Unexpected end of data. Needed {len} bytes, {} remaining",
                self.slice.len()
            )));
        }
        let (head, tail) = self.slice.split_at(len);
        self.slice = tail;
        Ok(head)
    }

    pub fn read_u8(&mut self) -> DcbResult<u8> {
        let b = self.read_bytes(1)?;
        Ok(b[0])
    }

    pub fn read_u16(&mut self) -> DcbResult<u16> {
        let b = self.read_bytes(2)?;
        Ok(u16::from_le_bytes(b.try_into().unwrap()))
    }

    pub fn read_u32(&mut self) -> DcbResult<u32> {
        let b = self.read_bytes(4)?;
        Ok(u32::from_le_bytes(b.try_into().unwrap()))
    }

    pub fn read_u64(&mut self) -> DcbResult<u64> {
        let b = self.read_bytes(8)?;
        Ok(u64::from_le_bytes(b.try_into().unwrap()))
    }

    pub fn read_uuid(&mut self) -> DcbResult<Uuid> {
        let bytes = self.read_bytes(16)?;
        Uuid::from_slice(bytes).map_err(|e| {
            DcbError::DeserializationError(format!("Invalid UUID sequence: {e}"))
        })
    }

    pub fn read_page_id(&mut self) -> DcbResult<PageID> {
        Ok(PageID(self.read_u64()?))
    }

    pub fn read_position(&mut self) -> DcbResult<Position> {
        Ok(Position(self.read_u64()?))
    }

    pub fn read_tsn(&mut self) -> DcbResult<Tsn> {
        Ok(Tsn(self.read_u64()?))
    }

    /// Safely reads `len` bytes and validates them as a zero-copy UTF-8 string reference.
    pub fn read_str(&mut self, len: usize) -> DcbResult<&'a str> {
        let bytes = self.read_bytes(len)?;
        std::str::from_utf8(bytes).map_err(|_| {
            DcbError::DeserializationError("Invalid UTF-8 sequence".to_string())
        })
    }

    /// Safely reads `len` bytes and allocates them into an owned String.
    pub fn read_string(&mut self, len: usize) -> DcbResult<String> {
        // Calls our zero-copy method, then allocates if successful
        let s = self.read_str(len)?;
        Ok(s.to_string())
    }

    /// Reads a dynamically sized tag hash from disk and zero-pads it to the in-memory size.
    pub fn read_tag_hash(&mut self) -> DcbResult<[u8; TAG_HASH_LEN]> {
        let keyw = get_tag_key_width();
        let key_bytes = self.read_bytes(keyw)?;

        let mut key = [0u8; TAG_HASH_LEN];
        // Copy the on-disk width and leave the rest as zeros
        key[..keyw].copy_from_slice(key_bytes);

        Ok(key)
    }
    
    /// Returns the number of unread bytes remaining.
    pub fn remaining(&self) -> usize {
        self.slice.len()
    }
}