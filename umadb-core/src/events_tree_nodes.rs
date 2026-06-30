use crate::common::PageID;
use crate::common::Position;
use crate::page::PAGE_HEADER_SIZE;
use bitflags::bitflags;
use byteorder::{ByteOrder, LittleEndian};
use std::collections::HashMap;
use std::io::{Cursor, Write};
use umadb_dcb::DcbError;
use umadb_dcb::DcbResult;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRecord {
    pub event_type: String,
    pub data: Vec<u8>,
    pub tags: Vec<String>,
    pub uuid: Option<Uuid>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventValue {
    Inline(EventRecord),
    // For large record (data + metadata) stored across overflow pages
    Overflow {
        event_type: String,
        data_len: u64,
        tags: Vec<String>,
        root_id: PageID,
        uuid: Option<Uuid>,
        metadata_len: u64,
    },
}

/// Maximum number of metadata entries in a single event. Limited by the `u16`
/// entry-count prefix used by the on-page encoding.
pub const MAX_METADATA_ENTRIES: usize = u16::MAX as usize;

/// Maximum length, in bytes, of a single metadata key or value. Limited by the
/// `u16` length prefix used by the on-page encoding.
pub const MAX_METADATA_ENTRY_LEN: usize = u16::MAX as usize;

/// Maximum length, in bytes, of an event type string. Limited by the `u16`
/// length prefix used by the on-page encoding.
pub const MAX_EVENT_TYPE_LEN: usize = u16::MAX as usize;

/// Maximum number of tags in a single event. Limited by the `u16` tag-count
/// prefix used by the on-page encoding.
pub const MAX_TAGS: usize = u16::MAX as usize;

/// Maximum length, in bytes, of a single tag string. Limited by the `u16`
/// length prefix used by the on-page encoding.
pub const MAX_TAG_LEN: usize = u16::MAX as usize;

pub fn validate_event_record_for_append(
    page_size: usize,
    record: EventRecord,
) -> DcbResult<((EventValue, usize), Option<(EventValue, usize)>)> {
    // validate_event_type(&record.event_type)?;
    validate_tags(&record.tags)?;
    validate_metadata(&record.metadata)?;

    let mut node = EventLeafNode {
        keys: vec![],
        values: vec![],
    };
    let empty_size = node.calc_serialized_size();
    node.keys.push(Position(0));
    node.values.push(EventValue::Inline(record));
    let inline_size = node.calc_serialized_size();
    let inline_value = node.values.pop().expect("node has one value");

    if inline_size + PAGE_HEADER_SIZE <= page_size {
        return Ok(((inline_value, inline_size - empty_size), None));
    }

    let overflow_value = match &inline_value {
        EventValue::Inline(record) => {
            EventValue::Overflow {
                event_type: record.event_type.clone(),
                data_len: record.data.len() as u64,
                tags: record.tags.clone(),
                root_id: PageID(0),
                uuid: record.uuid,
                metadata_len: if record.metadata.is_empty() {
                    0
                } else {
                    // Only non-zero matters for EventLeafNode::calc_serialized_size.
                    1
                },
            }
        }
        EventValue::Overflow { .. } => {
            return Err(DcbError::InternalError("Shouldn't get here".to_string()));
        }
    };

    node.values.push(overflow_value);
    let overflow_size = node.calc_serialized_size();
    let overflow_value = node.values.pop().expect("node has one value");

    if !(overflow_size + PAGE_HEADER_SIZE <= page_size) {
        return Err(DcbError::InvalidArgument(
            "event too large for page size".to_string(),
        ));
    }
    Ok((
        (inline_value, inline_size - empty_size),
        Some((overflow_value, overflow_size - empty_size)),
    ))
}

// Validate that the event type fits within the limits of the on-page
// encoding.
// pub fn validate_event_type(event_type: &str) -> DcbResult<()> {
//     if event_type.len() > MAX_EVENT_TYPE_LEN {
//         return Err(DcbError::InvalidArgument(format!(
//             "event type has length {} bytes, exceeding the maximum of {}",
//             event_type.len(),
//             MAX_EVENT_TYPE_LEN
//         )));
//     }
//     Ok(())
// }

/// Validate that the tags fit within the limits of the on-page encoding.
pub fn validate_tags(tags: &[String]) -> DcbResult<()> {
    if tags.len() > MAX_TAGS {
        return Err(DcbError::InvalidArgument(format!(
            "event has {} tags, exceeding the maximum of {}",
            tags.len(),
            MAX_TAGS
        )));
    }
    // for tag in tags {
    //     if tag.len() > MAX_TAG_LEN {
    //         return Err(DcbError::InvalidArgument(format!(
    //             "tag has length {} bytes, exceeding the maximum of {}",
    //             tag.len(),
    //             MAX_TAG_LEN
    //         )));
    //     }
    // }
    Ok(())
}

/// Validate that the metadata map fits within the limits of the on-page
/// encoding (which uses `u16` length prefixes). Returns
/// [`DcbError::InvalidArgument`] rather than letting an over-long key/value
/// silently truncate its length prefix and corrupt the encoding.
pub fn validate_metadata(metadata: &HashMap<String, String>) -> DcbResult<()> {
    if metadata.len() > MAX_METADATA_ENTRIES {
        return Err(DcbError::InvalidArgument(format!(
            "metadata has {} entries, exceeding the maximum of {}",
            metadata.len(),
            MAX_METADATA_ENTRIES
        )));
    }
    for (k, v) in metadata {
        if k.len() > MAX_METADATA_ENTRY_LEN {
            return Err(DcbError::InvalidArgument(format!(
                "metadata key has length {} bytes, exceeding the maximum of {}",
                k.len(),
                MAX_METADATA_ENTRY_LEN
            )));
        }
        if v.len() > MAX_METADATA_ENTRY_LEN {
            return Err(DcbError::InvalidArgument(format!(
                "metadata value for key '{}' has length {} bytes, exceeding the maximum of {}",
                k,
                v.len(),
                MAX_METADATA_ENTRY_LEN
            )));
        }
    }
    Ok(())
}

/// Number of bytes needed to serialize the given metadata map.
///
/// Layout: 2 bytes entry count, then for each entry 2 bytes key length + key
/// bytes + 2 bytes value length + value bytes.
pub fn metadata_serialized_size(metadata: &HashMap<String, String>) -> usize {
    let mut size = 2; // entry count (u16)
    for (k, v) in metadata {
        size += 2 + k.len() + 2 + v.len();
    }
    size
}

/// Serialize metadata into the start of `buf`, returning the number of bytes
/// written. Entries are written in sorted key order for deterministic output.
pub fn serialize_metadata_into(metadata: &HashMap<String, String>, buf: &mut [u8]) -> usize {
    // 1. Wrap the pre-allocated slice in a Cursor.
    // This tracks the index automatically without allocating heap memory.
    let mut cursor = Cursor::new(buf);

    // 2. Write the total count of elements (2 bytes)
    let n = metadata.len() as u16;
    let _ = cursor.write_all(&n.to_le_bytes());

    // 3. Loop directly through the HashMap iterator.
    // Zero allocations happen here because we no longer collect into a Vec or sort.
    for (k, v) in metadata {
        let kl = k.len() as u16;
        let _ = cursor.write_all(&kl.to_le_bytes());
        let _ = cursor.write_all(k.as_bytes());

        let vl = v.len() as u16;
        let _ = cursor.write_all(&vl.to_le_bytes());
        let _ = cursor.write_all(v.as_bytes());
    }

    // 4. Get the exact final index position from the cursor
    cursor.position() as usize
}

// TODO: Move this because it's only used in tests.
/// Serialize metadata into a freshly allocated buffer.
pub fn serialize_metadata(metadata: &HashMap<String, String>) -> Vec<u8> {
    let mut buf = vec![0u8; metadata_serialized_size(metadata)];
    serialize_metadata_into(metadata, &mut buf);
    buf
}

/// Read a metadata map from `slice` starting at `*offset`, advancing `*offset`
/// past the bytes consumed.
fn read_metadata(slice: &[u8], offset: &mut usize) -> DcbResult<HashMap<String, String>> {
    if *offset + 2 > slice.len() {
        return Err(DcbError::DeserializationError(
            "Unexpected end of data while reading metadata entry count".to_string(),
        ));
    }
    let n = LittleEndian::read_u16(&slice[*offset..*offset + 2]) as usize;
    *offset += 2;
    let mut map = HashMap::with_capacity(n);
    for _ in 0..n {
        if *offset + 2 > slice.len() {
            return Err(DcbError::DeserializationError(
                "Unexpected end of data while reading metadata key length".to_string(),
            ));
        }
        let kl = LittleEndian::read_u16(&slice[*offset..*offset + 2]) as usize;
        *offset += 2;
        if *offset + kl > slice.len() {
            return Err(DcbError::DeserializationError(
                "Unexpected end of data while reading metadata key".to_string(),
            ));
        }
        let key = match std::str::from_utf8(&slice[*offset..*offset + kl]) {
            Ok(s) => s.to_string(),
            Err(_) => {
                return Err(DcbError::DeserializationError(
                    "Invalid UTF-8 sequence in metadata key".to_string(),
                ));
            }
        };
        *offset += kl;
        if *offset + 2 > slice.len() {
            return Err(DcbError::DeserializationError(
                "Unexpected end of data while reading metadata value length".to_string(),
            ));
        }
        let vl = LittleEndian::read_u16(&slice[*offset..*offset + 2]) as usize;
        *offset += 2;
        if *offset + vl > slice.len() {
            return Err(DcbError::DeserializationError(
                "Unexpected end of data while reading metadata value".to_string(),
            ));
        }
        let value = match std::str::from_utf8(&slice[*offset..*offset + vl]) {
            Ok(s) => s.to_string(),
            Err(_) => {
                return Err(DcbError::DeserializationError(
                    "Invalid UTF-8 sequence in metadata value".to_string(),
                ));
            }
        };
        *offset += vl;
        map.insert(key, value);
    }
    Ok(map)
}

/// Deserialize a metadata map from a complete slice (used for the overflow
/// chain, where metadata bytes follow the event data).
pub fn deserialize_metadata(slice: &[u8]) -> DcbResult<HashMap<String, String>> {
    let mut offset = 0usize;
    read_metadata(slice, &mut offset)
}

impl PartialEq<EventValue> for EventRecord {
    fn eq(&self, other: &EventValue) -> bool {
        match other {
            EventValue::Inline(rec) => self == rec,
            EventValue::Overflow {
                event_type,
                data_len,
                tags,
                ..
            } => {
                &self.event_type == event_type
                    && &self.tags == tags
                    && (self.data.len() as u64) == *data_len
            }
        }
    }
}

impl PartialEq<EventRecord> for EventValue {
    fn eq(&self, other: &EventRecord) -> bool {
        other == self
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct EventValueFlags: u8 {
        const OVERFLOW      = 0b0000_0001; // event payload in overflow node
        const HAS_UUID      = 0b0000_0010; // event includes UUID field
        const HAS_METADATA  = 0b0000_0100; // event includes metadata field
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventLeafNode {
    pub keys: Vec<Position>,
    pub values: Vec<EventValue>,
}

impl EventLeafNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len
        let mut total_size = 2;

        // 8 bytes for each Position in keys
        total_size += self.keys.len() * 8;

        // For each value
        for value in &self.values {
            // 1 byte for discriminator
            total_size += 1;
            match value {
                EventValue::Inline(rec) => {
                    // 2 bytes for event_type length + bytes for the string
                    total_size += 2 + rec.event_type.len();
                    // 2 bytes for data length + bytes for the data
                    total_size += 2 + rec.data.len();
                    // 2 bytes for number of tags
                    total_size += 2;
                    // For each tag: 2 bytes for length + bytes for the string
                    for tag in &rec.tags {
                        total_size += 2 + tag.len();
                    }
                    if rec.uuid.is_some() {
                        total_size += 16;
                    }
                    if !rec.metadata.is_empty() {
                        total_size += metadata_serialized_size(&rec.metadata);
                    }
                }
                EventValue::Overflow {
                    event_type,
                    data_len: _,
                    tags,
                    root_id: _,
                    uuid,
                    metadata_len,
                } => {
                    // 2 bytes for event_type length + bytes for the string
                    total_size += 2 + event_type.len();
                    // 8 bytes for data_len (u64)
                    total_size += 8;
                    // 2 bytes for number of tags
                    total_size += 2;
                    // For each tag: 2 bytes for length + bytes for the string
                    for tag in tags {
                        total_size += 2 + tag.len();
                    }
                    // 8 bytes for root_id
                    total_size += 8;
                    if uuid.is_some() {
                        total_size += 16;
                    }
                    if *metadata_len > 0 {
                        // 8 bytes for metadata_len (u64)
                        total_size += 8;
                    }
                }
            }
        }

        total_size
    }

    /// No-allocation serialization into the provided buffer. Returns number of bytes written.
    pub fn serialize_into(&self, buf: &mut [u8]) -> usize {
        let mut i = 0usize;
        // keys_len
        let klen = self.keys.len() as u16;
        buf[i..i + 2].copy_from_slice(&klen.to_le_bytes());
        i += 2;
        // keys
        for key in &self.keys {
            let b = key.0.to_le_bytes();
            buf[i..i + 8].copy_from_slice(&b);
            i += 8;
        }
        // values
        for value in &self.values {
            let mut flags = EventValueFlags::empty();
            match value {
                EventValue::Inline(rec) => {
                    if rec.uuid.is_some() {
                        flags |= EventValueFlags::HAS_UUID;
                    }
                    if !rec.metadata.is_empty() {
                        flags |= EventValueFlags::HAS_METADATA;
                    }
                    buf[i] = flags.bits();
                    i += 1;
                    let et_len = rec.event_type.len() as u16;
                    buf[i..i + 2].copy_from_slice(&et_len.to_le_bytes());
                    i += 2;
                    let s = rec.event_type.as_bytes();
                    buf[i..i + s.len()].copy_from_slice(s);
                    i += s.len();
                    let dlen = rec.data.len() as u16;
                    buf[i..i + 2].copy_from_slice(&dlen.to_le_bytes());
                    i += 2;
                    buf[i..i + rec.data.len()].copy_from_slice(&rec.data);
                    i += rec.data.len();
                    let tlen = rec.tags.len() as u16;
                    buf[i..i + 2].copy_from_slice(&tlen.to_le_bytes());
                    i += 2;
                    for tag in &rec.tags {
                        let tl = tag.len() as u16;
                        buf[i..i + 2].copy_from_slice(&tl.to_le_bytes());
                        i += 2;
                        let tb = tag.as_bytes();
                        buf[i..i + tb.len()].copy_from_slice(tb);
                        i += tb.len();
                    }
                    if let Some(uuid) = rec.uuid {
                        buf[i..i + 16].copy_from_slice(uuid.as_bytes());
                        i += 16;
                    }
                    if !rec.metadata.is_empty() {
                        i += serialize_metadata_into(&rec.metadata, &mut buf[i..]);
                    }
                }
                EventValue::Overflow {
                    event_type,
                    data_len,
                    tags,
                    root_id,
                    uuid,
                    metadata_len,
                } => {
                    flags |= EventValueFlags::OVERFLOW;
                    if uuid.is_some() {
                        flags |= EventValueFlags::HAS_UUID;
                    }
                    if *metadata_len > 0 {
                        flags |= EventValueFlags::HAS_METADATA;
                    }
                    buf[i] = flags.bits();
                    i += 1;
                    let et_len = event_type.len() as u16;
                    buf[i..i + 2].copy_from_slice(&et_len.to_le_bytes());
                    i += 2;
                    let s = event_type.as_bytes();
                    buf[i..i + s.len()].copy_from_slice(s);
                    i += s.len();
                    buf[i..i + 8].copy_from_slice(&data_len.to_le_bytes());
                    i += 8;
                    let tlen = tags.len() as u16;
                    buf[i..i + 2].copy_from_slice(&tlen.to_le_bytes());
                    i += 2;
                    for tag in tags {
                        let tl = tag.len() as u16;
                        buf[i..i + 2].copy_from_slice(&tl.to_le_bytes());
                        i += 2;
                        let tb = tag.as_bytes();
                        buf[i..i + tb.len()].copy_from_slice(tb);
                        i += tb.len();
                    }
                    buf[i..i + 8].copy_from_slice(&root_id.0.to_le_bytes());
                    i += 8;
                    if uuid.is_some() {
                        buf[i..i + 16].copy_from_slice(uuid.unwrap().as_bytes());
                        i += 16;
                    }
                    if *metadata_len > 0 {
                        buf[i..i + 8].copy_from_slice(&metadata_len.to_le_bytes());
                        i += 8;
                    }
                }
            }
        }
        i
    }

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
            let position = LittleEndian::read_u64(&slice[start..start + 8]);
            keys.push(Position(position));
        }

        // Extract the values (EventValue)
        let mut values = Vec::with_capacity(keys_len);
        let mut offset = 2 + (keys_len * 8);

        for _ in 0..keys_len {
            // Read discriminator (1 byte)
            if offset + 1 > slice.len() {
                return Err(DcbError::DeserializationError(
                    "Unexpected end of data while reading value kind".to_string(),
                ));
            }

            let flags = EventValueFlags::from_bits(slice[offset]).ok_or(
                DcbError::DeserializationError("unknown flag bits set".to_string()),
            )?;
            offset += 1;

            // Extract event_type length (2 bytes)
            if offset + 2 > slice.len() {
                return Err(DcbError::DeserializationError(
                    "Unexpected end of data while reading event_type length".to_string(),
                ));
            }
            let event_type_len = LittleEndian::read_u16(&slice[offset..offset + 2]) as usize;
            offset += 2;
            if offset + event_type_len > slice.len() {
                return Err(DcbError::DeserializationError(
                    "Unexpected end of data while reading event_type".to_string(),
                ));
            }
            let event_type = match std::str::from_utf8(&slice[offset..offset + event_type_len]) {
                Ok(s) => s.to_string(),
                Err(_) => {
                    return Err(DcbError::DeserializationError(
                        "Invalid UTF-8 sequence in event_type".to_string(),
                    ));
                }
            };
            offset += event_type_len;

            let overflow = flags.contains(EventValueFlags::OVERFLOW);
            let has_uuid = flags.contains(EventValueFlags::HAS_UUID);
            let has_metadata = flags.contains(EventValueFlags::HAS_METADATA);

            if !overflow {
                // Inline: data_len u16 + data bytes
                if offset + 2 > slice.len() {
                    return Err(DcbError::DeserializationError(
                        "Unexpected end of data while reading data length".to_string(),
                    ));
                }
                let data_len = LittleEndian::read_u16(&slice[offset..offset + 2]) as usize;
                offset += 2;
                if offset + data_len > slice.len() {
                    return Err(DcbError::DeserializationError(
                        "Unexpected end of data while reading data".to_string(),
                    ));
                }
                let data = slice[offset..offset + data_len].to_vec();
                offset += data_len;

                // num tags
                if offset + 2 > slice.len() {
                    return Err(DcbError::DeserializationError(
                        "Unexpected end of data while reading number of tags".to_string(),
                    ));
                }
                let num_tags = LittleEndian::read_u16(&slice[offset..offset + 2]) as usize;
                offset += 2;
                let mut tags = Vec::with_capacity(num_tags);
                for _ in 0..num_tags {
                    if offset + 2 > slice.len() {
                        return Err(DcbError::DeserializationError(
                            "Unexpected end of data while reading tag length".to_string(),
                        ));
                    }
                    let tag_len = LittleEndian::read_u16(&slice[offset..offset + 2]) as usize;
                    offset += 2;
                    if offset + tag_len > slice.len() {
                        return Err(DcbError::DeserializationError(
                            "Unexpected end of data while reading tag".to_string(),
                        ));
                    }
                    let tag = match std::str::from_utf8(&slice[offset..offset + tag_len]) {
                        Ok(s) => s.to_string(),
                        Err(_) => {
                            return Err(DcbError::DeserializationError(
                                "Invalid UTF-8 sequence in tag".to_string(),
                            ));
                        }
                    };
                    offset += tag_len;
                    tags.push(tag);
                }

                let uuid = {
                    if has_uuid {
                        if offset + 16 > slice.len() {
                            return Err(DcbError::DeserializationError(
                                "Unexpected end of data while reading UUID".to_string(),
                            ));
                        }

                        match Uuid::from_slice(&slice[offset..offset + 16]) {
                            Ok(uuid) => {
                                offset += 16;
                                Some(uuid)
                            }
                            Err(err) => {
                                return Err(DcbError::DeserializationError(
                                    format!("Invalid UUID sequence: {err} ").to_string(),
                                ));
                            }
                        }
                    } else {
                        None
                    }
                };

                let metadata = if has_metadata {
                    read_metadata(slice, &mut offset)?
                } else {
                    HashMap::new()
                };

                values.push(EventValue::Inline(EventRecord {
                    event_type,
                    data,
                    tags,
                    uuid,
                    metadata,
                }));
            } else {
                // Overflow: data_len u64 + tags + root_id
                if offset + 8 > slice.len() {
                    return Err(DcbError::DeserializationError(
                        "Unexpected end of data while reading overflow data_len".to_string(),
                    ));
                }
                let data_len = LittleEndian::read_u64(&slice[offset..offset + 8]);
                offset += 8;

                if offset + 2 > slice.len() {
                    return Err(DcbError::DeserializationError(
                        "Unexpected end of data while reading number of tags".to_string(),
                    ));
                }
                let num_tags = LittleEndian::read_u16(&slice[offset..offset + 2]) as usize;
                offset += 2;
                let mut tags = Vec::with_capacity(num_tags);
                for _ in 0..num_tags {
                    if offset + 2 > slice.len() {
                        return Err(DcbError::DeserializationError(
                            "Unexpected end of data while reading tag length".to_string(),
                        ));
                    }
                    let tag_len = LittleEndian::read_u16(&slice[offset..offset + 2]) as usize;
                    offset += 2;
                    if offset + tag_len > slice.len() {
                        return Err(DcbError::DeserializationError(
                            "Unexpected end of data while reading tag".to_string(),
                        ));
                    }
                    let tag = match std::str::from_utf8(&slice[offset..offset + tag_len]) {
                        Ok(s) => s.to_string(),
                        Err(_) => {
                            return Err(DcbError::DeserializationError(
                                "Invalid UTF-8 sequence in tag".to_string(),
                            ));
                        }
                    };
                    offset += tag_len;
                    tags.push(tag);
                }
                if offset + 8 > slice.len() {
                    return Err(DcbError::DeserializationError(
                        "Unexpected end of data while reading overflow root_id".to_string(),
                    ));
                }
                let root_id = PageID(LittleEndian::read_u64(&slice[offset..offset + 8]));
                offset += 8;

                let uuid = {
                    if has_uuid {
                        if offset + 16 > slice.len() {
                            return Err(DcbError::DeserializationError(
                                "Unexpected end of data while reading UUID".to_string(),
                            ));
                        }

                        match Uuid::from_slice(&slice[offset..offset + 16]) {
                            Ok(uuid) => {
                                offset += 16;
                                Some(uuid)
                            }
                            Err(err) => {
                                return Err(DcbError::DeserializationError(
                                    format!("Invalid UUID sequence: {err} ").to_string(),
                                ));
                            }
                        }
                    } else {
                        None
                    }
                };
                let metadata_len = {
                    if has_metadata {
                        // metadata_len u64 (metadata bytes live at the tail of the overflow chain)
                        if offset + 8 > slice.len() {
                            return Err(DcbError::DeserializationError(
                                "Unexpected end of data while reading overflow metadata_len"
                                    .to_string(),
                            ));
                        }
                        let metadata_len = LittleEndian::read_u64(&slice[offset..offset + 8]);
                        offset += 8;
                        metadata_len
                    } else {
                        0
                    }
                };

                values.push(EventValue::Overflow {
                    event_type,
                    data_len,
                    tags,
                    root_id,
                    uuid,
                    metadata_len,
                });
            }
        }

        Ok(EventLeafNode { keys, values })
    }

    pub fn pop_last_key_and_value(&mut self) -> DcbResult<(Position, EventValue)> {
        let last_key = self
            .keys
            .pop()
            .expect("EventLeafNode should have some keys");
        let last_value = self
            .values
            .pop()
            .expect("EventLeafNode should have some values");
        Ok((last_key, last_value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventOverflowNode {
    pub next: PageID, // PageID(0) indicates end of chain
    pub data: Vec<u8>,
}

impl EventOverflowNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 8 bytes for next + data bytes
        8 + self.data.len()
    }

    pub fn serialize_into(&self, buf: &mut [u8]) -> usize {
        let size = self.calc_serialized_size();
        buf[0..8].copy_from_slice(&self.next.0.to_le_bytes());
        buf[8..size].copy_from_slice(&self.data);
        size
    }

    pub fn from_slice(slice: &[u8]) -> DcbResult<Self> {
        if slice.len() < 8 {
            return Err(DcbError::DeserializationError(
                "Overflow node too small".to_string(),
            ));
        }
        let next = PageID(LittleEndian::read_u64(&slice[0..8]));
        let data = slice[8..].to_vec();
        Ok(Self { next, data })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventInternalNode {
    pub keys: Vec<Position>,
    pub child_ids: Vec<PageID>,
}

impl EventInternalNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len
        let mut total_size = 2;

        // 8 bytes for each Position in keys
        total_size += self.keys.len() * 8;

        // 8 bytes for each PageID in child_ids
        total_size += self.child_ids.len() * 8;

        total_size
    }

    pub fn serialize_into(&self, buf: &mut [u8]) -> DcbResult<usize> {
        let mut i = 0usize;
        let klen = self.keys.len() as u16;
        buf[i..i + 2].copy_from_slice(&klen.to_le_bytes());
        i += 2;
        for key in &self.keys {
            buf[i..i + 8].copy_from_slice(&key.0.to_le_bytes());
            i += 8;
        }
        for child_id in &self.child_ids {
            buf[i..i + 8].copy_from_slice(&child_id.0.to_le_bytes());
            i += 8;
        }
        Ok(i)
    }

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
            let position = LittleEndian::read_u64(&slice[start..start + 8]);
            keys.push(Position(position));
        }

        // Calculate the offset after reading keys
        let offset = 2 + (keys_len * 8);

        // Derive child_ids_len from keys_len (always keys_len + 1)
        let child_ids_len = keys_len + 1;

        // Calculate the minimum expected size for the child_ids
        let min_expected_size = offset + (child_ids_len * 8);
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
            let start = offset + (i * 8);
            let page_id = LittleEndian::read_u64(&slice[start..start + 8]);
            child_ids.push(PageID(page_id));
        }

        Ok(EventInternalNode { keys, child_ids })
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
        promoted_key: Position,
        promoted_page_id: PageID,
    ) -> DcbResult<()> {
        self.keys.push(promoted_key);
        self.child_ids.push(promoted_page_id);
        Ok(())
    }
    pub fn split_off(&mut self) -> DcbResult<(Position, Vec<Position>, Vec<PageID>)> {
        let middle_idx = self.keys.len() - 2;
        let promoted_key = self.keys.remove(middle_idx);
        let new_keys = self.keys.split_off(middle_idx);
        let new_child_ids = self.child_ids.split_off(middle_idx + 1);
        Ok((promoted_key, new_keys, new_child_ids))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_internal_serialize() {
        // Create an EventInternalNode with known values
        let internal_node = EventInternalNode {
            keys: vec![Position(1000), Position(2000), Position(3000)],
            child_ids: vec![PageID(100), PageID(200), PageID(300), PageID(400)],
        };

        // Serialize the EventInternalNode
        let mut serialized = vec![0u8; internal_node.calc_serialized_size()];
        internal_node.serialize_into(&mut serialized).unwrap();

        // Verify the serialized output is not empty
        assert!(!serialized.is_empty());

        // Deserialize back to an EventInternalNode
        let deserialized = EventInternalNode::from_slice(&serialized)
            .expect("Failed to deserialize EventInternalNode");

        // Verify that the deserialized node matches the original
        assert_eq!(internal_node, deserialized);

        // Verify specific properties
        assert_eq!(3, deserialized.keys.len());
        assert_eq!(4, deserialized.child_ids.len());

        // Check keys
        assert_eq!(Position(1000), deserialized.keys[0]);
        assert_eq!(Position(2000), deserialized.keys[1]);
        assert_eq!(Position(3000), deserialized.keys[2]);

        // Check child_ids
        assert_eq!(PageID(100), deserialized.child_ids[0]);
        assert_eq!(PageID(200), deserialized.child_ids[1]);
        assert_eq!(PageID(300), deserialized.child_ids[2]);
        assert_eq!(PageID(400), deserialized.child_ids[3]);
    }

    #[test]
    fn test_event_leaf_serialize_without_uuid() {
        // Create an EventLeafNode with known values
        let leaf_node = EventLeafNode {
            keys: vec![Position(1000), Position(2000), Position(3000)],
            values: vec![
                EventValue::Inline(EventRecord {
                    event_type: "event_type_1".to_string(),
                    data: vec![1, 0, 0, 0], // 100 as little-endian bytes
                    tags: vec!["tag1".to_string(), "tag2".to_string(), "tag3".to_string()],
                    uuid: None,
                    metadata: HashMap::new(),
                }),
                EventValue::Inline(EventRecord {
                    event_type: "event_type_2".to_string(),
                    data: vec![2, 0, 0, 0], // 200 as little-endian bytes
                    tags: vec![
                        "tag4".to_string(),
                        "tag5".to_string(),
                        "tag6".to_string(),
                        "tag7".to_string(),
                    ],
                    uuid: None,
                    metadata: HashMap::new(),
                }),
                EventValue::Inline(EventRecord {
                    event_type: "event_type_3".to_string(),
                    data: vec![3, 0, 0, 0], // 300 as little-endian bytes
                    tags: vec!["tag8".to_string(), "tag9".to_string()],
                    uuid: None,
                    metadata: HashMap::new(),
                }),
            ],
        };

        // Serialize the EventLeafNode
        let mut serialized = vec![0u8; leaf_node.calc_serialized_size()];
        leaf_node.serialize_into(&mut serialized);

        // Verify the serialized output is not empty
        assert!(!serialized.is_empty());

        // Deserialize back to an EventLeafNode
        let deserialized =
            EventLeafNode::from_slice(&serialized).expect("Failed to deserialize EventLeafNode");

        // Verify that the deserialized node matches the original
        assert_eq!(leaf_node, deserialized);

        // Verify specific properties
        assert_eq!(3, deserialized.keys.len());
        assert_eq!(3, deserialized.values.len());

        // Check keys
        assert_eq!(Position(1000), deserialized.keys[0]);
        assert_eq!(Position(2000), deserialized.keys[1]);
        assert_eq!(Position(3000), deserialized.keys[2]);

        // Check first value
        match &deserialized.values[0] {
            EventValue::Inline(v) => {
                assert_eq!("event_type_1", v.event_type);
                assert_eq!(vec![1, 0, 0, 0], v.data);
                assert_eq!(
                    vec!["tag1".to_string(), "tag2".to_string(), "tag3".to_string()],
                    v.tags
                );
                assert_eq!(None, v.uuid);
            }
            _ => panic!("Expected Inline for first value"),
        }

        // Check second value
        match &deserialized.values[1] {
            EventValue::Inline(v) => {
                assert_eq!("event_type_2", v.event_type);
                assert_eq!(vec![2, 0, 0, 0], v.data);
                assert_eq!(
                    vec![
                        "tag4".to_string(),
                        "tag5".to_string(),
                        "tag6".to_string(),
                        "tag7".to_string()
                    ],
                    v.tags
                );
                assert_eq!(None, v.uuid);
            }
            _ => panic!("Expected Inline for second value"),
        }

        // Check third value
        match &deserialized.values[2] {
            EventValue::Inline(v) => {
                assert_eq!("event_type_3", v.event_type);
                assert_eq!(vec![3, 0, 0, 0], v.data);
                assert_eq!(vec!["tag8".to_string(), "tag9".to_string()], v.tags);
                assert_eq!(None, v.uuid);
            }
            _ => panic!("Expected Inline for third value"),
        }
    }

    #[test]
    fn test_event_leaf_serialize_with_uuid() {
        // Create an EventLeafNode with known values
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let uuid3 = Uuid::new_v4();
        let leaf_node = EventLeafNode {
            keys: vec![Position(1000), Position(2000), Position(3000)],
            values: vec![
                EventValue::Inline(EventRecord {
                    event_type: "event_type_1".to_string(),
                    data: vec![1, 0, 0, 0], // 100 as little-endian bytes
                    tags: vec!["tag1".to_string(), "tag2".to_string(), "tag3".to_string()],
                    uuid: Some(uuid1),
                    metadata: HashMap::new(),
                }),
                EventValue::Inline(EventRecord {
                    event_type: "event_type_2".to_string(),
                    data: vec![2, 0, 0, 0], // 200 as little-endian bytes
                    tags: vec![
                        "tag4".to_string(),
                        "tag5".to_string(),
                        "tag6".to_string(),
                        "tag7".to_string(),
                    ],
                    uuid: Some(uuid2),
                    metadata: HashMap::new(),
                }),
                EventValue::Inline(EventRecord {
                    event_type: "event_type_3".to_string(),
                    data: vec![3, 0, 0, 0], // 300 as little-endian bytes
                    tags: vec!["tag8".to_string(), "tag9".to_string()],
                    uuid: Some(uuid3),
                    metadata: HashMap::new(),
                }),
            ],
        };

        // Serialize the EventLeafNode
        let mut serialized = vec![0u8; leaf_node.calc_serialized_size()];
        leaf_node.serialize_into(&mut serialized);

        // Verify the serialized output is not empty
        assert!(!serialized.is_empty());

        // Deserialize back to an EventLeafNode
        let deserialized =
            EventLeafNode::from_slice(&serialized).expect("Failed to deserialize EventLeafNode");

        // Verify that the deserialized node matches the original
        assert_eq!(leaf_node, deserialized);

        // Verify specific properties
        assert_eq!(3, deserialized.keys.len());
        assert_eq!(3, deserialized.values.len());

        // Check keys
        assert_eq!(Position(1000), deserialized.keys[0]);
        assert_eq!(Position(2000), deserialized.keys[1]);
        assert_eq!(Position(3000), deserialized.keys[2]);

        // Check first value
        match &deserialized.values[0] {
            EventValue::Inline(v) => {
                assert_eq!("event_type_1", v.event_type);
                assert_eq!(vec![1, 0, 0, 0], v.data);
                assert_eq!(
                    vec!["tag1".to_string(), "tag2".to_string(), "tag3".to_string()],
                    v.tags
                );
                assert_eq!(Some(uuid1), v.uuid);
            }
            _ => panic!("Expected Inline for first value"),
        }

        // Check second value
        match &deserialized.values[1] {
            EventValue::Inline(v) => {
                assert_eq!("event_type_2", v.event_type);
                assert_eq!(vec![2, 0, 0, 0], v.data);
                assert_eq!(
                    vec![
                        "tag4".to_string(),
                        "tag5".to_string(),
                        "tag6".to_string(),
                        "tag7".to_string()
                    ],
                    v.tags
                );
                assert_eq!(Some(uuid2), v.uuid);
            }
            _ => panic!("Expected Inline for second value"),
        }

        // Check third value
        match &deserialized.values[2] {
            EventValue::Inline(v) => {
                assert_eq!("event_type_3", v.event_type);
                assert_eq!(vec![3, 0, 0, 0], v.data);
                assert_eq!(vec!["tag8".to_string(), "tag9".to_string()], v.tags);
                assert_eq!(Some(uuid3), v.uuid);
            }
            _ => panic!("Expected Inline for third value"),
        }
    }

    #[test]
    fn test_event_leaf_serialize_with_overflow_single_without_uuid() {
        let leaf_node = EventLeafNode {
            keys: vec![Position(111)],
            values: vec![EventValue::Overflow {
                event_type: "over_evt".to_string(),
                data_len: 1234567,
                tags: vec!["a".to_string(), "b".to_string()],
                root_id: PageID(123),
                uuid: None,
                metadata_len: 0,
            }],
        };
        // Serialize
        let mut serialized = vec![0u8; leaf_node.calc_serialized_size()];
        leaf_node.serialize_into(&mut serialized);
        assert!(!serialized.is_empty());
        // Deserialize
        let deserialized = EventLeafNode::from_slice(&serialized)
            .expect("Failed to deserialize EventLeafNode with overflow");
        assert_eq!(leaf_node, deserialized);

        // Check specific fields
        assert_eq!(1, deserialized.keys.len());
        assert_eq!(Position(111), deserialized.keys[0]);
        assert_eq!(1, deserialized.values.len());
        match &deserialized.values[0] {
            EventValue::Overflow {
                event_type,
                data_len,
                tags,
                root_id,
                uuid,
                metadata_len,
            } => {
                assert_eq!("over_evt", event_type);
                assert_eq!(1234567, *data_len);
                assert_eq!(vec!["a".to_string(), "b".to_string()], *tags);
                assert_eq!(PageID(123), *root_id);
                assert_eq!(None, *uuid);
                assert_eq!(0, *metadata_len);
            }
            _ => panic!("Expected Overflow variant"),
        }
    }

    #[test]
    fn test_event_leaf_serialize_with_overflow_single_with_uuid() {
        let uuid1 = Uuid::new_v4();
        let leaf_node = EventLeafNode {
            keys: vec![Position(111)],
            values: vec![EventValue::Overflow {
                event_type: "over_evt".to_string(),
                data_len: 1234567,
                tags: vec!["a".to_string(), "b".to_string()],
                root_id: PageID(123),
                uuid: Some(uuid1),
                metadata_len: 0,
            }],
        };
        // Serialize
        let mut serialized = vec![0u8; leaf_node.calc_serialized_size()];
        leaf_node.serialize_into(&mut serialized);
        assert!(!serialized.is_empty());
        // Deserialize
        let deserialized = EventLeafNode::from_slice(&serialized)
            .expect("Failed to deserialize EventLeafNode with overflow");
        assert_eq!(leaf_node, deserialized);

        // Check specific fields
        assert_eq!(1, deserialized.keys.len());
        assert_eq!(Position(111), deserialized.keys[0]);
        assert_eq!(1, deserialized.values.len());
        match &deserialized.values[0] {
            EventValue::Overflow {
                event_type,
                data_len,
                tags,
                root_id,
                uuid,
                metadata_len: 0,
            } => {
                assert_eq!("over_evt", event_type);
                assert_eq!(1234567, *data_len);
                assert_eq!(vec!["a".to_string(), "b".to_string()], *tags);
                assert_eq!(PageID(123), *root_id);
                assert_eq!(Some(uuid1), *uuid);
            }
            _ => panic!("Expected Overflow variant"),
        }
    }

    #[test]
    fn test_event_leaf_serialize_mixed_inline_and_overflow() {
        let inline = EventValue::Inline(EventRecord {
            event_type: "inline_evt".to_string(),
            data: vec![1, 2, 3],
            tags: vec!["x".to_string()],
            uuid: None,
            metadata: HashMap::new(),
        });
        let overflow = EventValue::Overflow {
            event_type: "overflow_evt".to_string(),
            data_len: 9999,
            tags: vec!["y".to_string(), "z".to_string()],
            root_id: PageID(999),
            uuid: None,
            metadata_len: 0,
        };
        let leaf_node = EventLeafNode {
            keys: vec![Position(10), Position(20)],
            values: vec![inline.clone(), overflow.clone()],
        };
        let mut serialized = vec![0u8; leaf_node.calc_serialized_size()];
        leaf_node.serialize_into(&mut serialized);
        let deserialized = EventLeafNode::from_slice(&serialized).unwrap();
        assert_eq!(leaf_node, deserialized);

        // Validate order and variants
        match &deserialized.values[0] {
            EventValue::Inline(rec) => {
                assert_eq!("inline_evt", rec.event_type);
                assert_eq!(vec![1, 2, 3], rec.data);
                assert_eq!(vec!["x".to_string()], rec.tags);
                assert_eq!(None, rec.uuid);
            }
            _ => panic!("Expected Inline at index 0"),
        }
        match &deserialized.values[1] {
            EventValue::Overflow {
                event_type,
                data_len,
                tags,
                root_id,
                uuid,
                metadata_len,
            } => {
                assert_eq!("overflow_evt", event_type);
                assert_eq!(9999, *data_len);
                assert_eq!(vec!["y".to_string(), "z".to_string()], *tags);
                assert_eq!(PageID(999), *root_id);
                assert_eq!(None, *uuid);
                assert_eq!(0, *metadata_len);
            }
            _ => panic!("Expected Overflow at index 1"),
        }
    }

    #[test]
    fn test_metadata_serialize_roundtrip() {
        let mut metadata = HashMap::new();
        metadata.insert("source".to_string(), "web".to_string());
        metadata.insert("correlation_id".to_string(), "abc-123".to_string());
        metadata.insert("empty_value".to_string(), "".to_string());

        let bytes = serialize_metadata(&metadata);
        assert_eq!(bytes.len(), metadata_serialized_size(&metadata));

        let deserialized = deserialize_metadata(&bytes).unwrap();
        assert_eq!(metadata, deserialized);
    }

    #[test]
    fn test_validate_metadata_within_limits_ok() {
        let mut metadata = HashMap::new();
        metadata.insert("k".to_string(), "v".to_string());
        // A key and value exactly at the maximum length are allowed.
        metadata.insert("a".repeat(MAX_METADATA_ENTRY_LEN), "b".to_string());
        metadata.insert("c".to_string(), "d".repeat(MAX_METADATA_ENTRY_LEN));
        assert!(validate_metadata(&metadata).is_ok());
    }

    // #[test]
    // fn test_validate_event_type_rejects_oversized() {
    //     let et = "a".repeat(MAX_EVENT_TYPE_LEN + 1);
    //     match validate_event_type(&et) {
    //         Err(DcbError::InvalidArgument(msg)) => {
    //             assert!(msg.contains("event type has length"));
    //             assert!(msg.contains("exceeding the maximum"));
    //         }
    //         other => panic!("Expected InvalidArgument, got {other:?}"),
    //     }
    // }

    // #[test]
    // fn test_validate_tags_rejects_oversized_tag() {
    //     let tags = vec!["tag".repeat(MAX_TAG_LEN + 1)];
    //     match validate_tags(&tags) {
    //         Err(DcbError::InvalidArgument(msg)) => {
    //             assert!(msg.contains("tag has length"));
    //             assert!(msg.contains("exceeding the maximum"));
    //         }
    //         other => panic!("Expected InvalidArgument, got {other:?}"),
    //     }
    // }

    #[test]
    fn test_validate_tags_rejects_too_many_tags() {
        let tags = vec!["tag".to_string(); MAX_TAGS + 1];
        match validate_tags(&tags) {
            Err(DcbError::InvalidArgument(msg)) => {
                assert!(msg.contains("event has"));
                assert!(msg.contains("tags, exceeding the maximum"));
            }
            other => panic!("Expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn test_validate_metadata_rejects_oversized_value() {
        let mut metadata = HashMap::new();
        metadata.insert("k".to_string(), "v".repeat(MAX_METADATA_ENTRY_LEN + 1));
        match validate_metadata(&metadata) {
            Err(DcbError::InvalidArgument(_)) => {}
            other => panic!("Expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn test_validate_metadata_rejects_oversized_key() {
        let mut metadata = HashMap::new();
        metadata.insert("k".repeat(MAX_METADATA_ENTRY_LEN + 1), "v".to_string());
        match validate_metadata(&metadata) {
            Err(DcbError::InvalidArgument(_)) => {}
            other => panic!("Expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn test_metadata_serialize_roundtrip_at_max_entry_len() {
        // A key/value at exactly the maximum length must survive a round-trip.
        let mut metadata = HashMap::new();
        metadata.insert("k".repeat(MAX_METADATA_ENTRY_LEN), "v".to_string());
        metadata.insert("k2".to_string(), "v".repeat(MAX_METADATA_ENTRY_LEN));
        let bytes = serialize_metadata(&metadata);
        assert_eq!(bytes.len(), metadata_serialized_size(&metadata));
        assert_eq!(metadata, deserialize_metadata(&bytes).unwrap());
    }

    #[test]
    fn test_empty_metadata_serialize_roundtrip() {
        let metadata: HashMap<String, String> = HashMap::new();
        let bytes = serialize_metadata(&metadata);
        // Just the 2-byte entry count of zero.
        assert_eq!(bytes.len(), 2);
        assert_eq!(metadata, deserialize_metadata(&bytes).unwrap());
    }

    #[test]
    fn test_event_leaf_serialize_inline_with_metadata() {
        let mut md1 = HashMap::new();
        md1.insert("source".to_string(), "ingest".to_string());
        md1.insert("schema".to_string(), "v2".to_string());

        let mut md3 = HashMap::new();
        md3.insert("trace".to_string(), "deadbeef".to_string());

        let uuid2 = Uuid::new_v4();
        let leaf_node = EventLeafNode {
            keys: vec![Position(10), Position(20), Position(30)],
            values: vec![
                // Inline with metadata, no uuid
                EventValue::Inline(EventRecord {
                    event_type: "with_md".to_string(),
                    data: vec![1, 2, 3, 4],
                    tags: vec!["tag1".to_string()],
                    uuid: None,
                    metadata: md1.clone(),
                }),
                // Inline with both uuid and metadata
                EventValue::Inline(EventRecord {
                    event_type: "uuid_and_md".to_string(),
                    data: vec![5, 6],
                    tags: vec!["tag2".to_string(), "tag3".to_string()],
                    uuid: Some(uuid2),
                    metadata: md3.clone(),
                }),
                // Inline with empty metadata to confirm the flag stays unset
                EventValue::Inline(EventRecord {
                    event_type: "no_md".to_string(),
                    data: vec![7],
                    tags: vec![],
                    uuid: None,
                    metadata: HashMap::new(),
                }),
            ],
        };

        let mut serialized = vec![0u8; leaf_node.calc_serialized_size()];
        let written = leaf_node.serialize_into(&mut serialized);
        assert_eq!(written, serialized.len());

        let deserialized =
            EventLeafNode::from_slice(&serialized).expect("Failed to deserialize EventLeafNode");
        assert_eq!(leaf_node, deserialized);

        match &deserialized.values[0] {
            EventValue::Inline(rec) => assert_eq!(md1, rec.metadata),
            _ => panic!("Expected Inline at index 0"),
        }
        match &deserialized.values[1] {
            EventValue::Inline(rec) => {
                assert_eq!(Some(uuid2), rec.uuid);
                assert_eq!(md3, rec.metadata);
            }
            _ => panic!("Expected Inline at index 1"),
        }
        match &deserialized.values[2] {
            EventValue::Inline(rec) => assert!(rec.metadata.is_empty()),
            _ => panic!("Expected Inline at index 2"),
        }
    }

    #[test]
    fn test_event_overflow_node_serialize_roundtrip() {
        let node = EventOverflowNode {
            next: PageID(77),
            data: vec![7, 8, 9, 10],
        };
        let mut ser = vec![0u8; node.calc_serialized_size()];
        node.serialize_into(&mut ser);
        assert_eq!(8 + 4, ser.len()); // 8 bytes next + 4 bytes data
        let de = EventOverflowNode::from_slice(&ser).unwrap();
        assert_eq!(node, de);
        assert_eq!(PageID(77), de.next);
        assert_eq!(vec![7, 8, 9, 10], de.data);
    }
}
