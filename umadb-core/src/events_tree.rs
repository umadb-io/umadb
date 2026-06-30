use crate::common::PageID;
use crate::common::Position;
use crate::events_tree_nodes::{
    EventInternalNode, EventLeafNode, EventOverflowNode, EventRecord, EventValue,
    deserialize_metadata, metadata_serialized_size, serialize_metadata_into,
    validate_event_record_for_append,
};
use crate::mvcc::{Mvcc, Writer};
use crate::node::Node;
use crate::page::{PAGE_HEADER_SIZE, Page};
use std::collections::HashMap;
use std::sync::Arc;
use umadb_dcb::{DcbError, DcbResult};

// Helpers for storing large event data across overflow pages
fn write_overflow_chain(mvcc: &Mvcc, writer: &mut Writer, data: &[u8]) -> DcbResult<PageID> {
    // Maximum payload per overflow page: page_size - header - next pointer (8 bytes)
    let payload_cap = mvcc.page_size.saturating_sub(PAGE_HEADER_SIZE + 8);
    if payload_cap == 0 {
        return Err(DcbError::DatabaseCorrupted(
            "Page size too small to store overflow data".to_string(),
        ));
    }
    // Split data into chunks from end to start so we can set next ids easily
    let chunk_count = data.len().div_ceil(payload_cap);
    let mut chunks: Vec<&[u8]> = Vec::with_capacity(chunk_count);
    let mut i = 0;
    while i < data.len() {
        let end = (i + payload_cap).min(data.len());
        chunks.push(&data[i..end]);
        i = end;
    }
    if chunks.is_empty() {
        // Store an empty chunk page to indicate zero-length data
        let page_id = writer.alloc_page_id();
        let node = EventOverflowNode {
            next: PageID(0),
            data: Vec::new(),
        };
        let page = Page::new(page_id, Node::EventOverflow(node));
        writer.insert_dirty(page)?;
        return Ok(page_id);
    }
    let mut next_id = PageID(0);
    for chunk in chunks.iter().rev() {
        let page_id = writer.alloc_page_id();
        let node = EventOverflowNode {
            next: next_id,
            data: (*chunk).to_vec(),
        };
        let page = Page::new(page_id, Node::EventOverflow(node));
        writer.insert_dirty(page)?;
        next_id = page_id;
    }
    Ok(next_id)
}

fn read_overflow_chain(
    mvcc: &Mvcc,
    dirty: &HashMap<PageID, Page>,
    mut page_id: PageID,
) -> DcbResult<Vec<u8>> {
    let mut out: Vec<u8> = Vec::new();
    while page_id.0 != 0 {
        page_id = with_page(mvcc, dirty, page_id, |page| match &page.node {
            Node::EventOverflow(node) => {
                out.extend_from_slice(&node.data);
                Ok(node.next)
            }
            _ => Err(DcbError::DatabaseCorrupted(
                "Expected EventOverflow node".to_string(),
            )),
        })?;
    }
    Ok(out)
}

fn with_page<T, F>(
    mvcc: &Mvcc,
    dirty: &HashMap<PageID, Page>,
    page_id: PageID,
    f: F,
) -> DcbResult<T>
where
    F: FnOnce(&Page) -> DcbResult<T>,
{
    if let Some(page) = dirty.get(&page_id) {
        f(page)
    } else {
        let page = mvcc.read_page(page_id)?;
        f(&page)
    }
}

/// Writes a fresh overflow chain with serialized event metadata followed by the event data.
///
/// Metadata is placed first so that a future metadata-only read can stop after
/// the leading page(s) without walking the potentially large event data.
fn data_and_metadata_to_overflow_chain(
    mvcc: &Mvcc,
    writer: &mut Writer,
    record_data: &Vec<u8>,
    metadata: &Vec<(String, String)>,
) -> DcbResult<(PageID, usize)> {
    let metadata_len = if metadata.is_empty() {
        0
    } else {
        metadata_serialized_size(metadata)
    };

    // 1. One allocation, perfectly sized for the final product
    let mut combined = vec![0u8; metadata_len + record_data.len()];

    if metadata_len > 0 {
        // 2. Overwrite the first part with metadata
        serialize_metadata_into(metadata, &mut combined[..metadata_len])?;
    }

    // 3. Overwrite the second part with record data (Fast memcpy)
    combined[metadata_len..].copy_from_slice(record_data);

    let root_id = write_overflow_chain(mvcc, writer, &combined)?;
    Ok((root_id, metadata_len))
}

fn materialize_event_value(
    mvcc: &Mvcc,
    dirty: &HashMap<PageID, Page>,
    value: &EventValue,
) -> DcbResult<EventRecord> {
    match value {
        EventValue::Inline(rec) => Ok(rec.clone()),
        EventValue::Overflow {
            event_type,
            data_len,
            tags,
            root_id,
            uuid,
            metadata_len,
        } => {
            let all_data = read_overflow_chain(mvcc, dirty, *root_id)?;
            let all_data_len = all_data.len();
            if (all_data_len as u64) != *data_len + *metadata_len {
                return Err(DcbError::DatabaseCorrupted(format!(
                    "Overflow data length mismatch, data: {data_len}, metadata: {metadata_len}"
                )));
            }
            // The chain holds leading metadata bytes followed by event data.
            let split = *metadata_len as usize;
            let metadata = if *metadata_len > 0 {
                deserialize_metadata(&all_data[..split])?
            } else {
                Vec::new()
            };
            let data = all_data[split..].to_vec();
            Ok(EventRecord {
                event_type: event_type.clone(),
                data,
                tags: tags.clone(),
                uuid: *uuid,
                metadata,
            })
        }
    }
}

// TODO: Move this because it's now only used in tests.
pub fn event_tree_append(
    mvcc: &Mvcc,
    writer: &mut Writer,
    event_record: EventRecord,
    position: Position,
) -> DcbResult<()> {
    let event_values_and_size_diffs =
        validate_event_record_for_append(mvcc.page_size, event_record)?;
    event_tree_append_event_value(mvcc, writer, event_values_and_size_diffs, position)
}

/// Append an event to the root event leaf page.
///
/// This function obtains a mutable reference to a dirty copy of the root event
/// leaf page (using copy-on-write if necessary) and appends the provided
/// Position to the keys and the EventRecord to the values.
pub fn event_tree_append_event_value(
    mvcc: &Mvcc,
    writer: &mut Writer,
    event_values_and_size_diffs: ((EventValue, usize), Option<(EventValue, usize)>),
    position: Position,
) -> DcbResult<()> {
    let verbose = mvcc.verbose;
    // if verbose {
    //     println!("Appending event: {position:?} {inline_value:?}");
    //     println!("Root is {:?}", writer.events_tree_root_id);
    // }
    // Get the current root page id for the event tree
    let mut current_page_id: PageID = writer.events_tree_root_id;

    // Traverse the tree to find a leaf node
    let mut stack: Vec<PageID> = Vec::new();
    loop {
        let current_page_ref = writer.get_page_ref(mvcc, current_page_id)?;
        if matches!(current_page_ref.node, Node::EventLeaf(_)) {
            break;
        }
        if let Node::EventInternal(internal_node) = &current_page_ref.node {
            if verbose {
                println!("{:?} is internal node", current_page_ref.page_id);
            }
            stack.push(current_page_id);
            current_page_id = *internal_node.child_ids.last().ok_or_else(|| {
                DcbError::DatabaseCorrupted("Internal node has no children".to_string())
            })?;
        } else {
            return Err(DcbError::DatabaseCorrupted(
                "Expected EventInternal node".to_string(),
            ));
        }
    }
    if verbose {
        println!("{current_page_id:?} is leaf node");
    }

    // Make the leaf page dirty
    let dirty_page_id = { writer.get_dirty_page_id(current_page_id)? };
    let replacement_info: Option<(PageID, PageID)> = {
        if dirty_page_id != current_page_id {
            Some((current_page_id, dirty_page_id))
        } else {
            None
        }
    };

    let serialized_size = writer
        .get_page_ref(mvcc, dirty_page_id)?
        .calc_serialized_size();

    // Decide what we need to do.
    // - if we already know inline value is too large for any page, indicated by
    //   the received overflow_value_and_size being Some(), we have to
    //   consider what to do with the given overflow value. Either it fits in the
    //   current page, or we must to split and append the overflow value to a new page.
    // - otherwise, if the received over_value_and_size is None, then we know the given
    //   inline_value can at least fit an empty page, so we have to decide whether it
    //   will fit this page. If it does fit this page then we can append it.
    // - otherwise, we need to decide between three options: (a) splitting the page
    //   and appending the inline value to the new page, (b) making an overflow node
    //   and seeing if it will fit this page, (c) splitting the page and appending an
    //   overflow value to the new page. The decision is about avoiding sparse pages
    //   as much as possible.
    //
    // Design:
    // - if we don't have an overflow value, then append if inline value fits or split and append
    // - otherwise, append if overflow value fits or split and append overflow value

    let (event_value, size_diff) = match event_values_and_size_diffs {
        ((EventValue::Inline(record), _), Some((mut overflow_value, overflow_size_diff))) => {
            let (overflow_root_id, overflow_metadata_len) =
                data_and_metadata_to_overflow_chain(mvcc, writer, &record.data, &record.metadata)?;
            if let EventValue::Overflow {
                root_id,
                metadata_len,
                ..
            } = &mut overflow_value
            {
                *root_id = overflow_root_id;
                *metadata_len = overflow_metadata_len as u64;
            } else {
                return Err(DcbError::InternalError(
                    "Shouldn't get here when setting root_id on event overflow value".to_string(),
                ));
            }

            (overflow_value, overflow_size_diff)
        }
        ((inline_value, inline_size_diff), None) => (inline_value, inline_size_diff),
        _ => {
            return Err(DcbError::InternalError(
                "Shouldn't get here when matching event_values_and_size_diffs".to_string(),
            ));
        }
    };

    // We may need to split the page.
    let mut popped: Option<EventValue> = None;

    if serialized_size + size_diff <= mvcc.page_size {
        // Get a mutable leaf node and append the event value and position.
        let dirty_leaf_page = writer.get_mut_dirty(dirty_page_id)?;
        match &mut dirty_leaf_page.node {
            Node::EventLeaf(node) => {
                if verbose {
                    println!(
                        "Pushing {:?} at {:?} onto {:?}",
                        event_value, position, dirty_page_id,
                    );
                }
                node.keys.push(position);
                node.values.push(event_value);
                // // Final check that the serialized size is okay.
                // let serialized_size = dirty_leaf_page.calc_serialized_size();
                // if serialized_size > mvcc.page_size {
                //     return Err(DcbError::DatabaseCorrupted(format!(
                //         "New event leaf page too large after appending value: {serialized_size}, max: {})",
                //         mvcc.page_size
                //     )));
                // }
            }
            _ => {
                return Err(DcbError::DatabaseCorrupted(
                    "Expected EventLeaf node".to_string(),
                ));
            }
        }
    } else {
        // Let's split and add this event value to a new page.
        popped = Some(event_value);
    }

    // Prepare for split propagation
    let mut split_info: Option<(Position, PageID)> = None;

    if let Some(event_value) = popped {
        // Build new leaf node page.
        let new_leaf_page_id = writer.alloc_page_id();
        let new_leaf_node = EventLeafNode {
            keys: vec![position],
            values: vec![event_value],
        };
        let new_leaf_page = Page::new(new_leaf_page_id, Node::EventLeaf(new_leaf_node.clone()));
        // // Final check that the serialized size is okay.
        // let serialized_size = new_leaf_page.calc_serialized_size();
        // if serialized_size > mvcc.page_size {
        //     return Err(DcbError::DatabaseCorrupted(format!(
        //         "New event leaf page too large after appending value: {serialized_size}, max: {})",
        //         mvcc.page_size
        //     )));
        // }
        if verbose {
            println!(
                "Created new leaf {:?}: {:?}",
                new_leaf_page_id, new_leaf_page.node
            );
        }
        writer.insert_dirty(new_leaf_page)?;
        if verbose {
            println!("Promoting {position:?} and {new_leaf_page_id:?}");
        }
        split_info = Some((position, new_leaf_page_id));
    }

    // Propagate splits and replacements up the stack
    let mut current_replacement_info = replacement_info;
    while let Some(parent_page_id) = stack.pop() {
        // Make the internal page dirty
        let dirty_page_id = writer.get_dirty_page_id(parent_page_id)?;
        let parent_replacement_info: Option<(PageID, PageID)> = {
            if dirty_page_id != parent_page_id {
                Some((parent_page_id, dirty_page_id))
            } else {
                None
            }
        };
        // Get a mutable internal node....
        let dirty_internal_page = writer.get_mut_dirty(dirty_page_id)?;

        if let Node::EventInternal(dirty_internal_node) = &mut dirty_internal_page.node {
            if let Some((old_id, new_id)) = current_replacement_info {
                dirty_internal_node.replace_last_child_id(old_id, new_id)?;
                if verbose {
                    println!(
                        "Replaced {old_id:?} with {new_id:?} in {dirty_page_id:?}: {dirty_internal_node:?}"
                    );
                }
            } else if verbose {
                println!("Nothing to replace in {dirty_page_id:?}")
            }
        } else {
            return Err(DcbError::DatabaseCorrupted(
                "Expected EventInternal node".to_string(),
            ));
        }

        if let Some((promoted_key, promoted_page_id)) = split_info {
            if let Node::EventInternal(dirty_internal_node) = &mut dirty_internal_page.node {
                // Add the promoted key and page ID
                dirty_internal_node
                    .append_promoted_key_and_page_id(promoted_key, promoted_page_id)?;

                if verbose {
                    println!(
                        "Appended promoted key {promoted_key:?} and child {promoted_page_id:?} in {dirty_page_id:?}: {dirty_internal_node:?}"
                    );
                }
            } else {
                return Err(DcbError::DatabaseCorrupted(
                    "Expected EventInternal node".to_string(),
                ));
            }
        }

        // Check if the internal page needs splitting

        if dirty_internal_page.calc_serialized_size() > mvcc.page_size {
            if let Node::EventInternal(dirty_internal_node) = &mut dirty_internal_page.node {
                if verbose {
                    println!("Splitting internal {dirty_page_id:?}...");
                }
                // Split the internal node
                // Ensure we have at least 3 keys and 4 child IDs before splitting
                if dirty_internal_node.keys.len() < 3 || dirty_internal_node.child_ids.len() < 4 {
                    return Err(DcbError::DatabaseCorrupted(
                        "Cannot split internal node with too few keys/children".to_string(),
                    ));
                }

                // Move the right-most key to a new node. Promote the next right-most key.
                let (promoted_key, new_keys, new_child_ids) = dirty_internal_node.split_off()?;

                // Ensure old node maintain the B-tree invariant: n keys should have n+1 child pointers
                assert_eq!(
                    dirty_internal_node.keys.len() + 1,
                    dirty_internal_node.child_ids.len()
                );

                let new_internal_node = EventInternalNode {
                    keys: new_keys,
                    child_ids: new_child_ids,
                };

                // Ensure the new node also maintains the invariant
                assert_eq!(
                    new_internal_node.keys.len() + 1,
                    new_internal_node.child_ids.len()
                );

                // Create a new internal page.
                let new_internal_page_id = writer.alloc_page_id();
                let new_internal_page =
                    Page::new(new_internal_page_id, Node::EventInternal(new_internal_node));
                if verbose {
                    println!(
                        "Created internal {:?}: {:?}",
                        new_internal_page_id, new_internal_page.node
                    );
                }
                writer.insert_dirty(new_internal_page)?;

                split_info = Some((promoted_key, new_internal_page_id));
            } else {
                return Err(DcbError::DatabaseCorrupted(
                    "Expected EventInternal node".to_string(),
                ));
            }
        } else {
            split_info = None;
        }
        current_replacement_info = parent_replacement_info;
    }

    if let Some((old_id, new_id)) = current_replacement_info {
        if writer.events_tree_root_id == old_id {
            writer.events_tree_root_id = new_id;
            if verbose {
                println!("Replaced root {old_id:?} with {new_id:?}");
            }
        } else {
            return Err(DcbError::RootIDMismatch(old_id.0, new_id.0));
        }
    }

    if let Some((promoted_key, promoted_page_id)) = split_info {
        // Create a new root
        let new_internal_node = EventInternalNode {
            keys: vec![promoted_key],
            child_ids: vec![writer.events_tree_root_id, promoted_page_id],
        };

        let new_root_page_id = writer.alloc_page_id();
        let new_root_page = Page::new(new_root_page_id, Node::EventInternal(new_internal_node));
        if verbose {
            println!(
                "Created new internal root {:?}: {:?}",
                new_root_page_id, new_root_page.node
            );
        }
        writer.insert_dirty(new_root_page)?;

        writer.events_tree_root_id = new_root_page_id;
    }

    Ok(())
}

pub fn event_tree_lookup(
    mvcc: &Mvcc,
    dirty: &HashMap<PageID, Page>,
    events_tree_root_id: PageID,
    position: Position,
) -> DcbResult<EventRecord> {
    enum LookupStep {
        Next(PageID),
        Found(EventRecord),
    }

    let mut current_page_id: PageID = events_tree_root_id;
    loop {
        let step = with_page(mvcc, dirty, current_page_id, |page| match &page.node {
            Node::EventInternal(internal) => {
                // Choose child based on upper bound of position in separator keys
                let idx = match internal.keys.binary_search(&position) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                };
                if idx >= internal.child_ids.len() {
                    return Err(DcbError::DatabaseCorrupted(
                        "Child index out of bounds in event tree".to_string(),
                    ));
                }
                Ok(LookupStep::Next(internal.child_ids[idx]))
            }
            Node::EventLeaf(leaf) => match leaf.keys.binary_search(&position) {
                Ok(i) => {
                    let rec = materialize_event_value(mvcc, dirty, &leaf.values[i])?;
                    Ok(LookupStep::Found(rec))
                }
                Err(_) => Err(DcbError::DatabaseCorrupted(format!(
                    "Event at position {position:?} not found",
                ))),
            },
            _ => Err(DcbError::DatabaseCorrupted(format!(
                "Expected EventInternal or EventLeaf node in event tree, got {}",
                page.node.type_name()
            ))),
        })?;

        match step {
            LookupStep::Next(next_page_id) => current_page_id = next_page_id,
            LookupStep::Found(rec) => return Ok(rec),
        }
    }
}

pub struct EventIterator<'a> {
    pub mvcc: &'a Mvcc,
    pub dirty: &'a HashMap<PageID, Page>,
    pub stack: Vec<(PageID, Option<usize>)>,
    pub page_cache: HashMap<PageID, Arc<Page>>,
    pub start: Option<Position>, // inclusive position, better for binary search
    pub backwards: bool,
}

impl<'a> EventIterator<'a> {
    pub fn new(
        mvcc: &'a Mvcc,
        dirty: &'a HashMap<PageID, Page>,
        events_tree_root_id: PageID,
        start: Option<Position>,
        backwards: bool,
    ) -> Self {
        let next_position = (events_tree_root_id, None);
        Self {
            mvcc,
            dirty,
            stack: vec![next_position],
            page_cache: HashMap::new(),
            start,
            backwards,
        }
    }

    pub fn next_batch(
        &mut self,
        batch_size: u32,
        cancel: Option<&std::sync::Arc<std::sync::atomic::AtomicBool>>,
    ) -> DcbResult<Vec<(Position, EventRecord)>> {
        let mut result: Vec<(Position, EventRecord)> = Vec::with_capacity(batch_size as usize);
        if batch_size == 0 {
            return Ok(result);
        }
        if self.backwards && self.start == Some(Position(0)) {
            return Ok(result);
        }
        while result.len() < batch_size as usize {
            if let Some(c) = cancel {
                if c.load(std::sync::atomic::Ordering::Relaxed) {
                    return Err(DcbError::CancelledByUser());
                }
            }
            let Some((page_id, mut stacked_idx)) = self.stack.pop() else {
                break; // traversal finished
            };

            // Compute actions under a scoped immutable borrow, then mutate cache/stack afterwards.
            let mut remove_page = false;
            let mut push_revisit: Option<(PageID, Option<usize>)> = None;
            let mut push_child: Option<(PageID, Option<usize>)> = None; // (child_id, stacked_keys_idx)
            let mut emit_event: Option<(Position, EventRecord)> = None;

            {
                // Obtain the current page (from dirty, or page cache, or deserialize).
                let page_ref: &Page = if let Some(p) = self.dirty.get(&page_id) {
                    p
                } else if let Some(p) = self.page_cache.get(&page_id) {
                    p.as_ref()
                } else {
                    let page_arc = self.mvcc.read_page(page_id)?;
                    self.page_cache.insert(page_id, page_arc);
                    self.page_cache
                        .get(&page_id)
                        .expect("page should be in cache")
                        .as_ref()
                };

                match &page_ref.node {
                    Node::EventInternal(internal) => {
                        // println!("Visit internal {page_id:?}");
                        if stacked_idx.is_none() && !internal.keys.is_empty() {
                            // println!(" - first visit");
                            // println!(" - keys: {:?}", internal.keys.clone());
                            // println!(" - child_ids: {:?}", internal.child_ids.clone());
                            // println!(" - from: {:?}", self.from);

                            stacked_idx = match &self.start {
                                Some(from) => match internal.keys.binary_search(from) {
                                    Ok(i) => Some(i + 1),
                                    Err(i) => Some(i),
                                },
                                None => {
                                    if !self.backwards {
                                        Some(0)
                                    } else {
                                        Some(internal.child_ids.len() - 1)
                                    }
                                }
                            };
                        }

                        if let Some(child_ids_idx) = stacked_idx {
                            // println!(" - child ids index: {} / {}", child_ids_idx + 1, internal.child_ids.len());
                            // println!(" - will visit child: {:?}", internal.child_ids[child_ids_idx]);
                            // Push the chosen child.
                            push_child = Some((internal.child_ids[child_ids_idx], None));
                            // Do or don't revisit this internal node?
                            if !self.backwards {
                                if child_ids_idx + 1 < internal.child_ids.len() {
                                    // Will revisit this internal node.
                                    // println!(" - will revisit");
                                    push_revisit = Some((page_id, Some(child_ids_idx + 1)));
                                } else {
                                    // Don't revisit this internal node.
                                    remove_page = true;
                                    // println!(" - will remove");
                                }
                            } else if child_ids_idx > 0 {
                                // Will revisit this internal node.
                                // println!(" - will revisit");
                                push_revisit = Some((page_id, Some(child_ids_idx - 1)));
                            } else {
                                // Don't revisit this internal node.
                                remove_page = true;
                                // println!(" - will remove");
                            }
                        } else {
                            // Shouldn't get here unless an internal
                            // node has no keys, which it should never do.
                            remove_page = true
                        };
                    }
                    Node::EventLeaf(leaf) => {
                        // println!("Visit leaf {page_id:?}");
                        if stacked_idx.is_none() {
                            // println!(" - first visit");
                            // println!(" - keys: {:?}", leaf.keys.clone());
                            let values_len = leaf.values.len();

                            stacked_idx = if values_len > 0 {
                                match &self.start {
                                    Some(from) => match leaf.keys.binary_search(from) {
                                        Ok(i) => Some(i),
                                        Err(i) => {
                                            if !self.backwards {
                                                Some(i)
                                            } else {
                                                Some(i - 1)
                                            }
                                        }
                                    },
                                    None => {
                                        if !self.backwards {
                                            Some(0)
                                        } else {
                                            Some(values_len - 1)
                                        }
                                    }
                                }
                            } else {
                                None
                            }
                        }

                        if let Some(values_idx) = stacked_idx {
                            // println!(" - values index: {} / {}", values_idx + 1, leaf.values.len());
                            if values_idx < leaf.values.len() {
                                let event_position = leaf.keys[values_idx];
                                let event_record = materialize_event_value(
                                    self.mvcc,
                                    self.dirty,
                                    &leaf.values[values_idx],
                                )?;
                                // println!(" - emit event position: {:?}", event_position.clone());
                                emit_event = Some((event_position, event_record));

                                if !self.backwards {
                                    if values_idx + 1 < leaf.values.len() {
                                        // Revisit this leaf.
                                        push_revisit = Some((page_id, Some(values_idx + 1)));
                                        // println!(" - not last value, will revisit");
                                    } else {
                                        // The last value.
                                        remove_page = true;
                                        // println!(" - last value, will remove");
                                    }
                                } else if values_idx > 0 {
                                    // Revisit this leaf.
                                    push_revisit = Some((page_id, Some(values_idx - 1)));
                                    // println!(" - not last value, will revisit");
                                } else {
                                    // The last value.
                                    remove_page = true;
                                    // println!(" - last value, will remove");
                                }
                            } else {
                                // No key greater or equal to 'from' in this leaf
                                // println!(" - value index out of range, why wasn't this removed?");
                                remove_page = true;
                            }
                        } else {
                            // No leaf values.
                            remove_page = true;
                        }
                    }
                    _ => {
                        return Err(DcbError::DatabaseCorrupted(format!(
                            "Expected EventInternal or EventLeaf node in event tree, got {}",
                            page_ref.node.type_name()
                        )));
                    }
                }
            }

            // Mutations after the borrow has ended
            if let Some(revisit) = push_revisit {
                // Revisit must be pushed first so that the child is processed next (LIFO)
                self.stack.push(revisit);
            }
            if let Some((child_id, child_start_idx)) = push_child {
                self.stack.push((child_id, child_start_idx));
            }
            if let Some((event_position, event_record)) = emit_event {
                result.push((event_position, event_record));
            }
            if remove_page {
                self.page_cache.remove(&page_id);
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::{Mvcc, StorageOptions};
    use crate::node::Node;
    use rand::random;
    use serial_test::serial;
    use tempfile::tempdir;

    static VERBOSE: bool = false;

    // Helper function to create a test database with a specified page size
    fn construct_db(page_size: usize) -> (tempfile::TempDir, Mvcc) {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("mvcc-test.db");
        let db = Mvcc::new(
            VERBOSE,
            StorageOptions::default()
                .db_path(db_path)
                .page_size(page_size),
        )
        .unwrap();
        (temp_dir, db)
    }

    #[test]
    #[serial]
    fn test_append_event_to_empty_leaf_root() {
        // Setup a temporary database
        let (_temp_dir, db) = construct_db(64);

        // Start a writer
        let mut writer = db.writer().unwrap();

        // Issue a new position
        let position = writer.issue_position();

        // Create an event record
        let record = EventRecord {
            event_type: "UserCreated".to_string(),
            data: vec![1, 2, 3, 4],
            tags: vec!["users".to_string(), "creation".to_string()],
            uuid: None,
            metadata: Vec::new(),
        };

        // Call append_event
        event_tree_append(&db, &mut writer, record.clone(), position).unwrap();

        // Verify that the dirty root page contains the appended key/value
        let new_root_id = writer.events_tree_root_id;
        assert!(writer.dirty.contains_key(&new_root_id));
        let page = writer.dirty.get(&new_root_id).unwrap();
        match &page.node {
            Node::EventLeaf(node) => {
                assert_eq!(vec![position], node.keys);
                assert_eq!(
                    vec![crate::events_tree_nodes::EventValue::Inline(record.clone())],
                    node.values
                );
            }
            _ => panic!("Expected EventLeaf node"),
        }

        // Commit the writer and verify persistence
        db.commit(&mut writer).unwrap();

        // Read back the latest header and the persisted root event leaf page
        let header_page = db.get_latest_header_page().unwrap();
        let header = header_page.as_header_node().unwrap();
        let persisted_page = db.read_page(header.events_tree_root_id).unwrap();
        match &persisted_page.node {
            Node::EventLeaf(node) => {
                assert_eq!(vec![position], node.keys);
                assert_eq!(
                    vec![crate::events_tree_nodes::EventValue::Inline(record)],
                    node.values
                );
            }
            _ => panic!("Expected EventLeaf node after commit"),
        }
    }

    #[test]
    #[serial]
    fn test_insert_events_until_split_leaf_one_writer() {
        // Setup a temporary database
        let (_temp_dir, db) = construct_db(256);

        // Start a writer
        let mut writer = db.writer().unwrap();

        let mut has_split_leaf = false;
        let mut appended: Vec<(Position, EventRecord)> = Vec::new();

        // Insert events until we split a leaf
        while !has_split_leaf {
            // Issue a new position and create a record
            let position = writer.issue_position();
            let record = EventRecord {
                event_type: "UserCreated".to_string(),
                data: (0..8).map(|_| random::<u8>()).collect(),
                tags: vec!["users".to_string(), "creation".to_string()],
                uuid: None,
                metadata: Vec::new(),
            };
            appended.push((position, record.clone()));

            // Append the event
            event_tree_append(&db, &mut writer, record, position).unwrap();

            // Check if we've split the leaf
            let root_page = writer.dirty.get(&writer.events_tree_root_id).unwrap();
            match &root_page.node {
                Node::EventInternal(_) => {
                    has_split_leaf = true;
                }
                _ => {}
            }
        }

        // Check keys and values of all pages
        let mut copy_inserted = appended.clone();

        // Get the root node
        let root_page = writer.dirty.get(&writer.events_tree_root_id).unwrap();
        let root_node = match &root_page.node {
            Node::EventInternal(node) => node,
            _ => panic!("Expected EventInternal node"),
        };

        // Check each child of the root
        for (i, &child_id) in root_node.child_ids.iter().enumerate() {
            let child_page = writer.dirty.get(&child_id).unwrap();
            assert_eq!(child_id, child_page.page_id);

            let child_node = match &child_page.node {
                Node::EventLeaf(node) => node,
                _ => panic!("Expected EventLeaf node"),
            };

            // Check that the keys are properly ordered
            if i > 0 {
                assert_eq!(root_node.keys[i - 1], child_node.keys[0]);
            }

            // Check each key and value in the child
            for (k, &key) in child_node.keys.iter().enumerate() {
                let record = &child_node.values[k];
                let (appended_position, appended_record) = copy_inserted.remove(0);
                assert_eq!(appended_position, key);
                assert_eq!(appended_record, record.clone());
            }
        }
    }

    #[test]
    #[serial]
    fn test_insert_events_until_split_leaf_many_writers() {
        // Setup a temporary database
        let (_temp_dir, db) = construct_db(256);

        let mut has_split_leaf = false;
        let mut appended: Vec<(Position, EventRecord)> = Vec::new();

        // Insert events until we split a leaf
        while !has_split_leaf {
            // Start a writer
            let mut writer = db.writer().unwrap();

            // Issue a new position and create a record
            let position = writer.issue_position();
            let record = EventRecord {
                event_type: "UserCreated".to_string(),
                data: (0..8).map(|_| random::<u8>()).collect(),
                tags: vec!["users".to_string(), "creation".to_string()],
                uuid: None,
                metadata: Vec::new(),
            };
            appended.push((position, record.clone()));

            // Append the event
            event_tree_append(&db, &mut writer, record, position).unwrap();

            // Check if we've split the leaf
            let root_page = writer.dirty.get(&writer.events_tree_root_id).unwrap();
            match &root_page.node {
                Node::EventInternal(_) => {
                    has_split_leaf = true;
                }
                _ => {}
            }

            db.commit(&mut writer).unwrap();
        }

        // Check keys and values of all pages
        let mut copy_inserted = appended.clone();

        // Start a writer
        let writer = db.writer().unwrap();

        // Get the root node
        let root_page = db.read_page(writer.events_tree_root_id).unwrap();
        let root_node = match &root_page.node {
            Node::EventInternal(node) => node,
            _ => panic!("Expected EventInternal node"),
        };

        // Check each child of the root
        for (i, &child_id) in root_node.child_ids.iter().enumerate() {
            let child_page = db.read_page(child_id).unwrap();
            assert_eq!(child_id, child_page.page_id);

            let child_node = match &child_page.node {
                Node::EventLeaf(node) => node,
                _ => panic!("Expected EventLeaf node"),
            };

            // Check that the keys are properly ordered
            if i > 0 {
                assert_eq!(root_node.keys[i - 1], child_node.keys[0]);
            }

            // Check each key and value in the child
            for (k, &key) in child_node.keys.iter().enumerate() {
                let record = &child_node.values[k];
                let (appended_position, appended_record) = copy_inserted.remove(0);
                assert_eq!(appended_position, key);
                assert_eq!(appended_record, record.clone());
            }
        }
    }

    #[test]
    #[serial]
    fn test_insert_events_until_split_internal_one_writer() {
        // Setup a temporary database
        let (_temp_dir, db) = construct_db(256);

        // Start a writer
        let mut writer = db.writer().unwrap();

        let mut has_split_internal = false;
        let mut appended: Vec<(Position, EventRecord)> = Vec::new();

        // Insert events until we split a leaf
        while !has_split_internal {
            // Issue a new position and create a record
            let position = writer.issue_position();
            let record = EventRecord {
                event_type: "UserCreated".to_string(),
                data: (0..8).map(|_| random::<u8>()).collect(),
                tags: vec!["users".to_string(), "creation".to_string()],
                uuid: None,
                metadata: Vec::new(),
            };
            appended.push((position, record.clone()));

            // Append the event
            event_tree_append(&db, &mut writer, record, position).unwrap();

            // Check if we've split an internal node
            let root_page = writer.dirty.get(&writer.events_tree_root_id).unwrap();
            match &root_page.node {
                Node::EventInternal(root_node) => {
                    // Check if the first child is an internal node
                    if !root_node.child_ids.is_empty() {
                        let child_id = root_node.child_ids[0];
                        if let Some(child_page) = writer.dirty.get(&child_id) {
                            match &child_page.node {
                                Node::EventInternal(_) => {
                                    has_split_internal = true;
                                }
                                _ => {}
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // Check keys and values of all pages
        let mut copy_inserted = appended.clone();

        // Get the root node
        let root_page = writer.dirty.get(&writer.events_tree_root_id).unwrap();
        let root_node = match &root_page.node {
            Node::EventInternal(node) => node,
            _ => panic!("Expected EventInternal node"),
        };

        // Check each child of the root
        for &child_id in root_node.child_ids.iter() {
            let child_page = writer.dirty.get(&child_id).unwrap();
            assert_eq!(child_id, child_page.page_id);

            let child_node = match &child_page.node {
                Node::EventInternal(node) => node,
                _ => panic!("Expected EventInternal node"),
            };

            for &grand_child_id in child_node.child_ids.iter() {
                let grand_child_page = writer.dirty.get(&grand_child_id).unwrap();
                assert_eq!(grand_child_id, grand_child_page.page_id);

                let grand_child_node = match &grand_child_page.node {
                    Node::EventLeaf(node) => node,
                    _ => panic!("Expected EventLeaf node"),
                };

                // Check each key and value in the child
                for (k, &key) in grand_child_node.keys.iter().enumerate() {
                    let record = &grand_child_node.values[k];
                    let (appended_position, appended_record) = copy_inserted.remove(0);
                    assert_eq!(appended_position, key);
                    assert_eq!(appended_record, record.clone());
                }
            }
        }
    }

    #[test]
    #[serial]
    fn test_insert_events_until_split_internal_many_writers() {
        // Setup a temporary database
        let (_temp_dir, db) = construct_db(512);

        let mut has_split_internal = false;
        let mut appended: Vec<(Position, EventRecord)> = Vec::new();

        // Insert events until we split a root internal node
        while !has_split_internal {
            // Start a writer
            let mut writer = db.writer().unwrap();

            // Issue a new position and create a record
            let position = writer.issue_position();
            let record = EventRecord {
                event_type: "UserCreated".to_string(),
                data: (0..8).map(|_| random::<u8>()).collect(),
                tags: vec!["users".to_string(), "creation".to_string()],
                uuid: None,
                metadata: Vec::new(),
            };
            appended.push((position, record.clone()));

            // Append the event
            event_tree_append(&db, &mut writer, record, position).unwrap();

            // Check if the root is an internal node
            let root_page = writer.dirty.get(&writer.events_tree_root_id).unwrap();
            match &root_page.node {
                Node::EventInternal(root_node) => {
                    // Check if the first child is an internal node
                    if !root_node.child_ids.is_empty() {
                        let child_id = root_node.child_ids[0];
                        if let Some(child_page) = writer.dirty.get(&child_id) {
                            match &child_page.node {
                                Node::EventInternal(_) => {
                                    has_split_internal = true;
                                }
                                _ => {}
                            }
                        }
                    }
                }
                _ => {}
            }
            db.commit(&mut writer).unwrap();
        }

        // Check keys and values of all pages
        let mut copy_inserted = appended.clone();

        // Start a writer
        let writer = db.writer().unwrap();

        // Get the root node
        let root_page = db.read_page(writer.events_tree_root_id).unwrap();
        let root_node = match &root_page.node {
            Node::EventInternal(node) => node,
            _ => panic!("Expected EventInternal node"),
        };

        // Check each child of the root
        for &child_id in root_node.child_ids.iter() {
            let child_page = db.read_page(child_id).unwrap();
            assert_eq!(child_id, child_page.page_id);

            let child_node = match &child_page.node {
                Node::EventInternal(node) => node,
                _ => panic!("Expected EventInternal node"),
            };

            for &grand_child_id in child_node.child_ids.iter() {
                let grand_child_page = db.read_page(grand_child_id).unwrap();
                assert_eq!(grand_child_id, grand_child_page.page_id);

                let grand_child_node = match &grand_child_page.node {
                    Node::EventLeaf(node) => node,
                    _ => panic!("Expected EventLeaf node"),
                };

                // Check each key and value in the child
                for (k, &key) in grand_child_node.keys.iter().enumerate() {
                    let record = &grand_child_node.values[k];
                    let (appended_position, appended_record) = copy_inserted.remove(0);
                    // println!("Checking appended event: {appended_position:?} {appended_record:?}");
                    assert_eq!(appended_position, key);
                    assert_eq!(appended_record, record.clone());
                }
            }
        }
        assert_eq!(0, copy_inserted.len());
    }

    #[test]
    #[serial]
    fn test_read_events_all() {
        // Setup a temporary database
        let (_temp_dir, db) = construct_db(512);

        let mut has_split_internal = false;
        let mut appended: Vec<(Position, EventRecord)> = Vec::new();

        // Insert events until we split a root internal node
        while !has_split_internal {
            // Start a writer
            let mut writer = db.writer().unwrap();

            // Issue a new position and create a record
            let position = writer.issue_position();
            let record = EventRecord {
                event_type: "UserCreated".to_string(),
                data: (0..8).map(|_| random::<u8>()).collect(),
                tags: vec!["users".to_string(), "creation".to_string()],
                uuid: None,
                metadata: Vec::new(),
            };
            appended.push((position, record.clone()));

            // Append the event
            event_tree_append(&db, &mut writer, record, position).unwrap();

            // Check if the root is an internal node
            let root_page = writer.dirty.get(&writer.events_tree_root_id).unwrap();
            match &root_page.node {
                Node::EventInternal(root_node) => {
                    // Check if the first child is an internal node
                    if !root_node.child_ids.is_empty() {
                        let child_id = root_node.child_ids[0];
                        if let Some(child_page) = writer.dirty.get(&child_id) {
                            match &child_page.node {
                                Node::EventInternal(_) => {
                                    has_split_internal = true;
                                }
                                _ => {}
                            }
                        }
                    }
                }
                _ => {}
            }
            db.commit(&mut writer).unwrap();
        }

        // Check keys and values of all pages
        let copy_inserted = appended.clone();

        // Start a reader
        let reader = db.reader().unwrap();
        let events_tree_root_id = reader.events_tree_root_id;
        let reader_tsn = reader.tsn;

        let dirty = HashMap::new();
        let mut events_iterator = EventIterator::new(&db, &dirty, events_tree_root_id, None, false);

        // Ensure the reader's tsn is registered while the iterator is alive
        {
            // DashMap: direct access without intermediate variable
            assert!(
                db.reader_tsns.iter().any(|r| *r.value() == reader_tsn),
                "TSN should remain registered until reader is dropped"
            );
        }

        // Progressively iterate over events using batches
        let mut scanned: Vec<(Position, EventRecord)> = Vec::new();
        loop {
            let batch = events_iterator.next_batch(3, None).unwrap();
            if batch.is_empty() {
                break;
            }
            scanned.extend(batch);

            // The reader should remain registered throughout iteration
            // DashMap: direct access without intermediate variable
            assert!(
                db.reader_tsns.iter().any(|r| *r.value() == reader_tsn),
                "TSN should remain registered until reader is dropped"
            );
        }

        assert_eq!(copy_inserted.len(), scanned.len());
        for (i, expected) in copy_inserted.iter().enumerate() {
            assert_eq!(expected.0, scanned[i].0);
            assert_eq!(expected.1, scanned[i].1);
        }

        // Additionally, validate lookup_event for each appended position using the existing reader in the iterator
        let dirty = HashMap::new();
        for (pos, expected_rec) in copy_inserted.iter() {
            let found = event_tree_lookup(&db, &dirty, events_tree_root_id, *pos).unwrap();
            assert_eq!(expected_rec, &found);
        }

        // Ensure we did not accumulate pages in the iterator cache
        assert!(
            events_iterator.page_cache.is_empty(),
            "EventIterator page_cache should be empty after full scan"
        );

        // While iterator is still alive, the reader should still be registered
        {
            // DashMap: direct access without intermediate variable
            assert!(
                db.reader_tsns.iter().any(|r| *r.value() == reader_tsn),
                "TSN should remain registered until reader is dropped"
            );
        }

        // Drop the reader and ensure the reader tsn is removed
        drop(reader);
        {
            // DashMap: direct access without intermediate variable
            assert!(
                db.reader_tsns.iter().all(|r| *r.value() != reader_tsn),
                "TSN should be removed after reader is dropped"
            );
            assert_eq!(0, db.reader_tsns.len());
        }
    }

    #[test]
    #[serial]
    fn test_read_events_from_forwards() {
        // Setup a temporary database
        let (_temp_dir, db) = construct_db(128);

        let mut has_split_internal = false;
        let mut appended: Vec<(Position, EventRecord)> = Vec::new();

        // Insert events until we split a root internal node
        while !has_split_internal {
            // Start a writer
            let mut writer = db.writer().unwrap();

            // Issue a new position and create a record
            let position = writer.issue_position();
            let record = EventRecord {
                event_type: "UserCreated".to_string(),
                data: (0..8).map(|_| random::<u8>()).collect(),
                tags: vec!["users".to_string(), "creation".to_string()],
                uuid: None,
                metadata: Vec::new(),
            };
            appended.push((position, record.clone()));

            // Append the event
            event_tree_append(&db, &mut writer, record, position).unwrap();

            // Check if the root is an internal node
            let root_page = writer.dirty.get(&writer.events_tree_root_id).unwrap();
            match &root_page.node {
                Node::EventInternal(root_node) => {
                    // Check if the first child is an internal node
                    if !root_node.child_ids.is_empty() {
                        let child_id = root_node.child_ids[0];
                        if let Some(child_page) = writer.dirty.get(&child_id) {
                            match &child_page.node {
                                Node::EventInternal(_) => {
                                    has_split_internal = true;
                                }
                                _ => {}
                            }
                        }
                    }
                }
                _ => {}
            }
            db.commit(&mut writer).unwrap();
        }

        let appended_len = appended.len();
        // println!("Appended number: {}", appended_len);

        // Iterate through various 'from' positions.
        for i in 0..appended_len + 2 {
            let from = Position(i as u64);

            // Start a reader
            let reader = db.reader().unwrap();
            let events_tree_root_id = reader.events_tree_root_id;
            let reader_tsn = reader.tsn;
            let dirty = HashMap::new();
            let mut events_iterator =
                EventIterator::new(&db, &dirty, events_tree_root_id, Some(from), false);

            // Ensure the reader's tsn is registered while the iterator is alive
            {
                // DashMap: direct access without intermediate variable
                assert!(
                    db.reader_tsns.iter().any(|r| *r.value() == reader_tsn),
                    "TSN should remain registered until reader is dropped"
                );
            }

            // Progressively iterate over events using batches
            let mut scanned: Vec<(Position, EventRecord)> = Vec::new();
            loop {
                let batch = events_iterator.next_batch(3, None).unwrap();
                if batch.is_empty() {
                    break;
                }
                scanned.extend(batch);

                // The reader should remain registered throughout iteration
                // DashMap: direct access without intermediate variable
                assert!(
                    db.reader_tsns.iter().any(|r| *r.value() == reader_tsn),
                    "TSN should remain registered until reader is dropped"
                );
            }

            // Expected are strictly from the chosen 'from' position
            let num_to_skip = i.max(1) - 1;
            let expected: Vec<(Position, EventRecord)> =
                appended.clone().into_iter().skip(num_to_skip).collect();
            assert_eq!(expected.len(), scanned.len());
            // println!("Get expected number: {}", expected.len());
            for (i, exp) in expected.iter().enumerate() {
                assert_eq!(exp.0, scanned[i].0);
                assert_eq!(exp.1, scanned[i].1);
            }

            // Ensure we did not accumulate pages in the iterator cache
            assert!(
                events_iterator.page_cache.is_empty(),
                "EventIterator page_cache should be empty after filtered scan"
            );

            // Drop the reader and ensure the reader tsn is removed
            drop(reader);
            {
                // DashMap: direct access without intermediate variable
                assert!(
                    db.reader_tsns.iter().all(|r| *r.value() != reader_tsn),
                    "TSN should be removed after reader is dropped"
                );
            }
        }
    }

    #[test]
    #[serial]
    fn test_read_events_from_backwards() {
        // Setup a temporary database
        let (_temp_dir, db) = construct_db(128);

        let mut has_split_internal = false;
        let mut appended: Vec<(Position, EventRecord)> = Vec::new();

        // Insert events until we split a root internal node
        while !has_split_internal {
            // Start a writer
            let mut writer = db.writer().unwrap();

            // Issue a new position and create a record
            let position = writer.issue_position();
            let record = EventRecord {
                event_type: "UserCreated".to_string(),
                data: (0..8).map(|_| random::<u8>()).collect(),
                tags: vec!["users".to_string(), "creation".to_string()],
                uuid: None,
                metadata: Vec::new(),
            };
            appended.push((position, record.clone()));

            // Append the event
            event_tree_append(&db, &mut writer, record, position).unwrap();

            // Check if the root is an internal node
            let root_page = writer.dirty.get(&writer.events_tree_root_id).unwrap();
            match &root_page.node {
                Node::EventInternal(root_node) => {
                    // Check if the first child is an internal node
                    if !root_node.child_ids.is_empty() {
                        let child_id = root_node.child_ids[0];
                        if let Some(child_page) = writer.dirty.get(&child_id) {
                            match &child_page.node {
                                Node::EventInternal(_) => {
                                    has_split_internal = true;
                                }
                                _ => {}
                            }
                        }
                    }
                }
                _ => {}
            }
            db.commit(&mut writer).unwrap();
        }

        let appended_len = appended.len();
        // println!("Appended number: {}", appended_len);

        // Iterate through various 'from' positions.
        for i in 0..appended_len + 2 {
            let from = Position(i as u64);

            // Start a reader
            let reader = db.reader().unwrap();
            let events_tree_root_id = reader.events_tree_root_id;
            let reader_tsn = reader.tsn;
            let dirty = HashMap::new();
            let mut events_iterator =
                EventIterator::new(&db, &dirty, events_tree_root_id, Some(from), true);

            // Ensure the reader's tsn is registered while the iterator is alive
            {
                // DashMap: direct access without intermediate variable
                assert!(
                    db.reader_tsns.iter().any(|r| *r.value() == reader_tsn),
                    "TSN should remain registered until reader is dropped"
                );
            }

            // Progressively iterate over events using batches
            let mut scanned: Vec<(Position, EventRecord)> = Vec::new();
            loop {
                let batch = events_iterator.next_batch(3, None).unwrap();
                if batch.is_empty() {
                    break;
                }
                scanned.extend(batch);

                // The reader should remain registered throughout iteration
                // DashMap: direct access without intermediate variable
                assert!(
                    db.reader_tsns.iter().any(|r| *r.value() == reader_tsn),
                    "TSN should remain registered until reader is dropped"
                );
            }

            // Expected are strictly from the chosen 'from' position
            let mut expected: Vec<(Position, EventRecord)> = appended.clone();
            expected.truncate(i);
            expected.reverse();
            assert_eq!(expected.len(), scanned.len());
            // println!("Get expected number: {}", expected.len());
            for (i, exp) in expected.iter().enumerate() {
                assert_eq!(exp.0, scanned[i].0);
                assert_eq!(exp.1, scanned[i].1);
            }

            // Ensure we did not accumulate pages in the iterator cache
            assert!(
                events_iterator.page_cache.is_empty(),
                "EventIterator page_cache should be empty after filtered scan"
            );

            // Drop the reader and ensure the reader tsn is removed
            drop(reader);
            {
                // DashMap: direct access without intermediate variable
                assert!(
                    db.reader_tsns.iter().all(|r| *r.value() != reader_tsn),
                    "TSN should be removed after reader is dropped"
                );
            }
        }
    }

    #[test]
    #[serial]
    fn test_large_event_data_exact_page_size() {
        let (_tmp, db) = construct_db(512);
        // Append one large event with data exactly equal to page size
        let mut writer = db.writer().unwrap();
        let pos = writer.issue_position();
        let data = vec![0xAB; 512];
        let event = EventRecord {
            event_type: "Big".into(),
            data: data.clone(),
            tags: vec![],
            uuid: None,
            metadata: Vec::new(),
        };
        event_tree_append(&db, &mut writer, event.clone(), pos).unwrap();
        db.commit(&mut writer).unwrap();

        // Lookup should return identical payload
        let reader = db.reader().unwrap();
        let dirty = HashMap::new();
        let got = event_tree_lookup(&db, &dirty, reader.events_tree_root_id, pos).unwrap();
        assert_eq!(event, got);

        // Ensure an overflow page is used for storage
        let header_page = db.get_latest_header_page().unwrap();
        let header = header_page.as_header_node().unwrap();
        let root = db.read_page(header.events_tree_root_id).unwrap();
        match &root.node {
            Node::EventInternal(internal) => {
                // Our key should be in the last child
                let leaf_id = *internal.child_ids.last().unwrap();
                let leaf_page = db.read_page(leaf_id).unwrap();
                match &leaf_page.node {
                    Node::EventLeaf(leaf) => match &leaf.values[0] {
                        EventValue::Overflow { data_len, .. } => {
                            assert_eq!(*data_len as usize, data.len())
                        }
                        _ => panic!("Expected Overflow for large event"),
                    },
                    _ => panic!("Expected EventLeaf child"),
                }
            }
            Node::EventLeaf(leaf) => match &leaf.values[0] {
                EventValue::Overflow { data_len, .. } => {
                    assert_eq!(*data_len as usize, data.len())
                }
                _ => panic!("Expected Overflow for large event"),
            },
            _ => panic!("Unexpected root node type"),
        }
    }

    #[test]
    #[serial]
    fn test_large_event_data_four_times_page_size() {
        let (_tmp, db) = construct_db(512);
        let mut writer = db.writer().unwrap();
        let pos = writer.issue_position();
        let data = vec![0xCD; 512 * 4];
        let event = EventRecord {
            event_type: "Bigger".into(),
            data: data.clone(),
            tags: vec![],
            uuid: None,
            metadata: Vec::new(),
        };
        event_tree_append(&db, &mut writer, event.clone(), pos).unwrap();
        db.commit(&mut writer).unwrap();

        // Lookup
        let reader = db.reader().unwrap();
        let dirty = HashMap::new();
        let got = event_tree_lookup(&db, &dirty, reader.events_tree_root_id, pos).unwrap();
        assert_eq!(event, got);

        // Ensure overflow in leaf
        let header_page = db.get_latest_header_page().unwrap();
        let header = header_page.as_header_node().unwrap();
        let root = db.read_page(header.events_tree_root_id).unwrap();
        let check_leaf = |leaf: &EventLeafNode| match &leaf.values[0] {
            EventValue::Overflow { data_len, .. } => {
                assert_eq!(*data_len as usize, data.len())
            }
            _ => panic!("Expected Overflow for very large event"),
        };
        match &root.node {
            Node::EventInternal(internal) => {
                let leaf_id = *internal.child_ids.last().unwrap();
                let leaf_page = db.read_page(leaf_id).unwrap();
                match &leaf_page.node {
                    Node::EventLeaf(leaf) => check_leaf(&leaf),
                    _ => panic!("Expected leaf"),
                }
            }
            Node::EventLeaf(leaf) => check_leaf(&leaf),
            _ => panic!("Unexpected root node type"),
        }
    }

    #[test]
    #[serial]
    fn test_inline_event_metadata_roundtrip() {
        let (_tmp, db) = construct_db(4096);
        let mut writer = db.writer().unwrap();
        let pos = writer.issue_position();

        let mut metadata = Vec::new();
        metadata.push(("source".to_string(), "web".to_string()));
        metadata.push(("correlation_id".to_string(), "abc-123".to_string()));

        let event = EventRecord {
            event_type: "SmallWithMetadata".into(),
            data: vec![1, 2, 3, 4],
            tags: vec!["t".into()],
            uuid: None,
            metadata: metadata.clone(),
        };
        event_tree_append(&db, &mut writer, event.clone(), pos).unwrap();
        db.commit(&mut writer).unwrap();

        // Read back through the materialization path and confirm metadata survives.
        let reader = db.reader().unwrap();
        let dirty = HashMap::new();
        let got = event_tree_lookup(&db, &dirty, reader.events_tree_root_id, pos).unwrap();
        assert_eq!(event, got);
        assert_eq!(metadata, got.metadata);

        // Confirm the value was stored inline (not in an overflow chain).
        let header_page = db.get_latest_header_page().unwrap();
        let header = header_page.as_header_node().unwrap();
        let root = db.read_page(header.events_tree_root_id).unwrap();
        match &root.node {
            Node::EventLeaf(leaf) => match &leaf.values[0] {
                EventValue::Inline(rec) => assert_eq!(metadata, rec.metadata),
                _ => panic!("Expected Inline value"),
            },
            _ => panic!("Expected EventLeaf root for a single small event"),
        }
    }

    #[test]
    #[serial]
    fn test_overflow_event_metadata_roundtrip() {
        let (_tmp, db) = construct_db(512);
        let mut writer = db.writer().unwrap();
        let pos = writer.issue_position();

        let mut metadata = Vec::new();
        metadata.push(("source".to_string(), "bulk-import".to_string()));
        metadata.push(("schema".to_string(), "v3".to_string()));

        // Data large enough to spill into overflow pages.
        let data = vec![0xAB; 512 * 4];
        let event = EventRecord {
            event_type: "BigWithMetadata".into(),
            data: data.clone(),
            tags: vec!["t".into()],
            uuid: Some(uuid::Uuid::new_v4()),
            metadata: metadata.clone(),
        };
        event_tree_append(&db, &mut writer, event.clone(), pos).unwrap();
        db.commit(&mut writer).unwrap();

        // Read back through the materialization path and confirm both data and
        // metadata survive the split/overflow chain.
        let reader = db.reader().unwrap();
        let dirty = HashMap::new();
        let got = event_tree_lookup(&db, &dirty, reader.events_tree_root_id, pos).unwrap();
        assert_eq!(event, got);
        assert_eq!(data, got.data);
        assert_eq!(metadata, got.metadata);

        // Confirm the value really was stored as an overflow value with a
        // non-zero metadata length.
        let header_page = db.get_latest_header_page().unwrap();
        let header = header_page.as_header_node().unwrap();
        let root = db.read_page(header.events_tree_root_id).unwrap();
        let check_leaf = |leaf: &EventLeafNode| match &leaf.values[0] {
            EventValue::Overflow {
                data_len,
                metadata_len,
                ..
            } => {
                assert_eq!(*data_len as usize, data.len());
                assert!(*metadata_len > 0, "expected non-zero metadata_len");
            }
            _ => panic!("Expected Overflow value for large event"),
        };
        match &root.node {
            Node::EventInternal(internal) => {
                let leaf_id = *internal.child_ids.last().unwrap();
                let leaf_page = db.read_page(leaf_id).unwrap();
                match &leaf_page.node {
                    Node::EventLeaf(leaf) => check_leaf(leaf),
                    _ => panic!("Expected EventLeaf child"),
                }
            }
            Node::EventLeaf(leaf) => check_leaf(leaf),
            _ => panic!("Unexpected root node type"),
        }
    }

    #[test]
    #[serial]
    fn test_direct_overflow_event_metadata_roundtrip() {
        // Data larger than u16::MAX takes the direct inline->overflow path in
        // event_tree_append (rather than the leaf-split conversion path).
        let (_tmp, db) = construct_db(4096);
        let mut writer = db.writer().unwrap();
        let pos = writer.issue_position();

        let mut metadata = Vec::new();
        metadata.push(("origin".to_string(), "direct-overflow".to_string()));

        let data = vec![0xCD; (u16::MAX as usize) + 1024];
        let event = EventRecord {
            event_type: "HugeWithMetadata".into(),
            data: data.clone(),
            tags: vec!["t".into()],
            uuid: None,
            metadata: metadata.clone(),
        };
        event_tree_append(&db, &mut writer, event.clone(), pos).unwrap();
        db.commit(&mut writer).unwrap();

        let reader = db.reader().unwrap();
        let dirty = HashMap::new();
        let got = event_tree_lookup(&db, &dirty, reader.events_tree_root_id, pos).unwrap();
        assert_eq!(event, got);
        assert_eq!(data, got.data);
        assert_eq!(metadata, got.metadata);
    }

    #[test]
    #[serial]
    fn test_append_rejects_oversized_metadata_key() {
        use crate::events_tree_nodes::MAX_METADATA_ENTRY_LEN;

        let (_tmp, db) = construct_db(4096);
        let mut writer = db.writer().unwrap();
        let pos = writer.issue_position();

        let mut metadata = Vec::new();
        metadata.push(("x".repeat(MAX_METADATA_ENTRY_LEN + 1), "source".to_string()));
        let event = EventRecord {
            event_type: "TooBigMetadata".into(),
            data: vec![1, 2, 3],
            tags: vec!["t".into()],
            uuid: None,
            metadata,
        };

        // Append must fail cleanly with InvalidArgument rather than corrupting
        // the encoding.
        match event_tree_append(&db, &mut writer, event, pos) {
            Err(DcbError::InvalidArgument(_)) => {}
            other => panic!("Expected InvalidArgument, got {other:?}"),
        }

        // Nothing should have been persisted: after committing, the position
        // must not resolve to a stored event.
        db.commit(&mut writer).unwrap();
        let reader = db.reader().unwrap();
        let dirty = HashMap::new();
        assert!(
            event_tree_lookup(&db, &dirty, reader.events_tree_root_id, pos).is_err(),
            "rejected event must not be retrievable"
        );
    }

    #[test]
    #[serial]
    fn test_append_rejects_oversized_metadata_value() {
        use crate::events_tree_nodes::MAX_METADATA_ENTRY_LEN;

        let (_tmp, db) = construct_db(4096);
        let mut writer = db.writer().unwrap();
        let pos = writer.issue_position();

        let mut metadata = Vec::new();
        metadata.push(("source".to_string(), "x".repeat(MAX_METADATA_ENTRY_LEN + 1)));
        let event = EventRecord {
            event_type: "TooBigMetadata".into(),
            data: vec![1, 2, 3],
            tags: vec!["t".into()],
            uuid: None,
            metadata,
        };

        // Append must fail cleanly with InvalidArgument rather than corrupting
        // the encoding.
        match event_tree_append(&db, &mut writer, event, pos) {
            Err(DcbError::InvalidArgument(_)) => {}
            other => panic!("Expected InvalidArgument, got {other:?}"),
        }

        // Nothing should have been persisted: after committing, the position
        // must not resolve to a stored event.
        db.commit(&mut writer).unwrap();
        let reader = db.reader().unwrap();
        let dirty = HashMap::new();
        assert!(
            event_tree_lookup(&db, &dirty, reader.events_tree_root_id, pos).is_err(),
            "rejected event must not be retrievable"
        );
    }

    #[test]
    #[serial]
    fn test_append_rejects_oversized_event_type() {
        use crate::events_tree_nodes::MAX_EVENT_TYPE_LEN;

        let (_tmp, db) = construct_db(4096);
        let mut writer = db.writer().unwrap();
        let pos = writer.issue_position();

        let event = EventRecord {
            event_type: "x".repeat(MAX_EVENT_TYPE_LEN + 1),
            data: vec![1, 2, 3],
            tags: vec!["t".into()],
            uuid: None,
            metadata: Vec::new(),
        };

        match event_tree_append(&db, &mut writer, event, pos) {
            Err(DcbError::InvalidArgument(_)) => {}
            other => panic!("Expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    #[serial]
    fn test_append_rejects_oversized_tag() {
        use crate::events_tree_nodes::MAX_TAG_LEN;

        let (_tmp, db) = construct_db(4096);
        let mut writer = db.writer().unwrap();
        let pos = writer.issue_position();

        let event = EventRecord {
            event_type: "Type".into(),
            data: vec![1, 2, 3],
            tags: vec!["x".repeat(MAX_TAG_LEN + 1)],
            uuid: None,
            metadata: Vec::new(),
        };

        match event_tree_append(&db, &mut writer, event, pos) {
            Err(DcbError::InvalidArgument(_)) => {}
            other => panic!("Expected InvalidArgument, got {other:?}"),
        }
    }
    // fn benchmark_append_and_lookup_varied_sizes() {
    //     // Benchmark-like test; prints durations for different sizes. Run with:
    //     // cargo test --lib mvcc_event_tree::tests::benchmark_append_and_lookup_varied_sizes -- --nocapture
    //     let sizes: [usize; 7] = [1, 10, 100, 1_000, 5_000, 10_000, 50_000];
    //     for &size in &sizes {
    //         let (_tmp, db) = construct_db(4096);
    //
    //         // Append phase
    //         let mut writer = db.writer().unwrap();
    //         let mut positions: Vec<Position> = Vec::with_capacity(size);
    //         let start_append = std::time::Instant::now();
    //         for n in 0..(size as u64) {
    //             let pos = writer.issue_position();
    //             let event = EventRecord {
    //                 event_type: "E".to_string(),
    //                 data: Vec::new(),
    //                 tags: Vec::new(),
    //             };
    //             std::hint::black_box(n);
    //             std::hint::black_box(&event);
    //             std::hint::black_box(pos);
    //             event_tree_append(&db, &mut writer, event, pos).unwrap();
    //             positions.push(pos);
    //         }
    //         let append_elapsed = start_append.elapsed();
    //         let start_commit = std::time::Instant::now();
    //         db.commit(&mut writer).unwrap();
    //         let commit_elapsed = start_commit.elapsed();
    //
    //         // Lookup phase
    //         let reader = db.reader().unwrap();
    //         let start_lookup = std::time::Instant::now();
    //         let dirty = HashMap::new();
    //         for &pos in &positions {
    //             let rec = event_tree_lookup(&db, &dirty, reader.events_tree_root_id, pos).unwrap();
    //             std::hint::black_box(&rec);
    //         }
    //         let lookup_elapsed = start_lookup.elapsed();
    //
    //         let append_avg_us = (append_elapsed.as_secs_f64() * 1_000_000.0) / (size as f64);
    //         let commit_avg_us = commit_elapsed.as_secs_f64() * 1_000_000.0;
    //         let lookup_avg_us = (lookup_elapsed.as_secs_f64() * 1_000_000.0) / (size as f64);
    //
    //         println!(
    //             "mvcc_event_tree benchmark: size={}, append_us_per_call={:.3}, commit_us={:.3}, lookup_us_per_call={:.3}",
    //             size, append_avg_us, commit_avg_us, lookup_avg_us
    //         );
    //     }
    // }

    #[test]
    #[serial]
    fn test_read_events_all_backwards_with_internal_nodes() {
        // Setup a temporary database with a small page size to force internal nodes quickly
        let (_temp_dir, db) = construct_db(128);

        let mut has_split_internal = false;
        let mut appended: Vec<(Position, EventRecord)> = Vec::new();

        // 1. Insert events until the root is an internal node that points to other internal nodes
        while !has_split_internal {
            let mut writer = db.writer().unwrap();
            let position = writer.issue_position();
            let record = EventRecord {
                event_type: "TestEvent".to_string(),
                data: vec![1, 2, 3, 4],
                tags: vec!["test".to_string()],
                uuid: None,
                metadata: Vec::new(),
            };
            appended.push((position, record.clone()));
            event_tree_append(&db, &mut writer, record, position).unwrap();

            let root_page = writer.dirty.get(&writer.events_tree_root_id).unwrap();
            if let Node::EventInternal(root_node) = &root_page.node {
                if !root_node.child_ids.is_empty() {
                    let child_id = root_node.child_ids[0];
                    if let Some(child_page) = writer.dirty.get(&child_id) {
                        if let Node::EventInternal(_) = &child_page.node {
                            has_split_internal = true;
                        }
                    }
                }
            }
            db.commit(&mut writer).unwrap();
        }

        // 2. Iterate backwards from the end (start = None)
        let reader = db.reader().unwrap();
        let dirty = HashMap::new();
        let mut events_iterator = EventIterator::new(
            &db,
            &dirty,
            reader.events_tree_root_id,
            None, // This triggers the branch for line 561
            true, // Backwards
        );

        let mut scanned = Vec::new();
        loop {
            let batch = events_iterator.next_batch(10, None).unwrap();
            if batch.is_empty() {
                break;
            }
            scanned.extend(batch);
        }

        // 3. Verify all events are returned in reverse order
        let mut expected = appended.clone();
        expected.reverse();

        assert_eq!(scanned.len(), expected.len());
        for i in 0..expected.len() {
            assert_eq!(scanned[i].0, expected[i].0);
        }
    }

    #[test]
    #[serial]
    fn test_append_event_with_tag_larger_than_page_size() {
        // This test was created when I realized that even though
        // page data and metadata overflows, it may be that the
        // tags or the type is so large that it can't possibly
        // fit in one page... the tree should detect this.
        use crate::events_tree_nodes::MAX_TAG_LEN;

        // Small page size to make it easy to exceed
        let page_size = 512;
        let (_tmp, db) = construct_db(page_size);
        let mut writer = db.writer().unwrap();
        let pos = writer.issue_position();

        // Tag larger than page size but less than MAX_TAG_LEN
        // Let's make it 600 bytes.
        let large_tag = "t".repeat(600);
        assert!(large_tag.len() > page_size);
        assert!(large_tag.len() < MAX_TAG_LEN);

        let event = EventRecord {
            event_type: "Type".into(),
            data: vec![1, 2, 3],
            tags: vec![large_tag],
            uuid: None,
            metadata: Vec::new(),
        };

        match event_tree_append(&db, &mut writer, event, pos) {
            Err(DcbError::InvalidArgument(message)) => {
                assert!(
                    message.contains("event too large for page size"),
                    "unexpected InvalidArgument message: {message}"
                );
            }
            other => panic!("Expected DatabaseCorrupted, got {other:?}"),
        }
    }
}
