use umadb_dcb::{DcbError, DcbResult};

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

    /// Returns the number of unread bytes remaining.
    pub fn remaining(&self) -> usize {
        self.slice.len()
    }
}