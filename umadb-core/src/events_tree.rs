use crate::common::PageID;
use crate::common::Position;
use crate::events_tree_nodes::{
    EventInternalNode, EventLeafNode, EventOverflowNode, EventRecord, EventValue,
};
use crate::mvcc::{Mvcc, Writer};
use crate::node::Node;
use crate::page::{PAGE_HEADER_SIZE, Page};
use std::borrow::Cow;
use std::collections::HashMap;
use umadb_dcb::{DCBError, DCBResult};

// Helpers for storing large event data across overflow pages
fn write_overflow_chain(mvcc: &Mvcc, writer: &mut Writer, data: &[u8]) -> DCBResult<PageID> {
    // Maximum payload per overflow page: page_size - header - next pointer (8 bytes)
    let payload_cap = mvcc.page_size.saturating_sub(PAGE_HEADER_SIZE + 8);
    if payload_cap == 0 {
        return Err(DCBError::DatabaseCorrupted(
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
) -> DCBResult<Vec<u8>> {
    let mut out: Vec<u8> = Vec::new();
    while page_id.0 != 0 {
        // Prefer the dirty (unflushed) page if present; otherwise read from disk
        let page = if let Some(p) = dirty.get(&page_id) {
            p.clone()
        } else {
            mvcc.read_page(page_id)?
        };
        match page.node {
            Node::EventOverflow(node) => {
                out.extend_from_slice(&node.data);
                page_id = node.next;
            }
            _ => {
                return Err(DCBError::DatabaseCorrupted(
                    "Expected EventOverflow node".to_string(),
                ));
            }
        }
    }
    Ok(out)
}

fn materialize_event_value(
    mvcc: &Mvcc,
    dirty: &HashMap<PageID, Page>,
    value: &EventValue,
) -> DCBResult<EventRecord> {
    match value {
        EventValue::Inline(rec) => Ok(rec.clone()),
        EventValue::Overflow {
            event_type,
            data_len,
            tags,
            root_id,
            uuid,
        } => {
            let data = read_overflow_chain(mvcc, dirty, *root_id)?;
            if (data.len() as u64) != *data_len {
                return Err(DCBError::DatabaseCorrupted(
                    "Overflow data length mismatch".to_string(),
                ));
            }
            Ok(EventRecord {
                event_type: event_type.clone(),
                data,
                tags: tags.clone(),
                uuid: *uuid,
            })
        }
    }
}

/// Append an event to the root event leaf page.
///
/// This function obtains a mutable reference to a dirty copy of the root event
/// leaf page (using copy-on-write if necessary) and appends the provided
/// Position to the keys and the EventRecord to the values.
pub fn event_tree_append(
    mvcc: &Mvcc,
    writer: &mut Writer,
    event: EventRecord,
    position: Position,
) -> DCBResult<()> {
    let verbose = mvcc.verbose;
    if verbose {
        println!("Appending event: {position:?} {event:?}");
        println!("Root is {:?}", writer.events_tree_root_id);
    }
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
            current_page_id = *internal_node
                .child_ids
                .last()
                .expect("Internal node should have some children");
        } else {
            return Err(DCBError::DatabaseCorrupted(
                "Expected EventInternal node".to_string(),
            ));
        }
    }
    if verbose {
        println!("{current_page_id:?} is leaf node");
    }

    // Decide inline vs overflow based on data length before mut-borrowing the page
    let pending_value = if event.data.len() > u16::MAX as usize {
        let root_id = write_overflow_chain(mvcc, writer, &event.data)?;
        EventValue::Overflow {
            event_type: event.event_type.clone(),
            data_len: event.data.len() as u64,
            tags: event.tags.clone(),
            root_id,
            uuid: event.uuid,
        }
    } else {
        EventValue::Inline(event)
    };

    // Make the leaf page dirty
    let dirty_page_id = { writer.get_dirty_page_id(current_page_id)? };
    let replacement_info: Option<(PageID, PageID)> = {
        if dirty_page_id != current_page_id {
            Some((current_page_id, dirty_page_id))
        } else {
            None
        }
    };

    // We may need to pop the last key/value for splitting; hold it after we drop the borrow
    let mut popped: Option<(Position, EventValue)> = None;

    // Get a mutable leaf node and append the data
    {
        let dirty_leaf_page = writer.get_mut_dirty(dirty_page_id)?;
        match &mut dirty_leaf_page.node {
            Node::EventLeaf(node) => {
                node.keys.push(position);
                node.values.push(pending_value);

                // Check if the leaf needs splitting by estimating the serialized size
                let serialized_size = dirty_leaf_page.calc_serialized_size();
                if serialized_size > mvcc.page_size {
                    if let Node::EventLeaf(dirty_leaf_node) = &mut dirty_leaf_page.node {
                        let (last_key, last_value) = dirty_leaf_node.pop_last_key_and_value()?;
                        if verbose {
                            println!(
                                "Split leaf {:?}: {:?}",
                                dirty_page_id,
                                dirty_leaf_node.clone()
                            );
                        }
                        popped = Some((last_key, last_value));
                    } else {
                        return Err(DCBError::DatabaseCorrupted(
                            "Expected EventLeaf node".to_string(),
                        ));
                    }
                }
            }
            _ => {
                return Err(DCBError::DatabaseCorrupted(
                    "Expected EventLeaf node at event tree root".to_string(),
                ));
            }
        }
    }

    // Prepare for split propagation
    let mut split_info: Option<(Position, PageID)> = None;

    if let Some((last_key, mut last_value)) = popped {
        // Build new leaf node; convert to overflow if needed to fit
        let new_leaf_page_id = writer.alloc_page_id();
        let mut new_leaf_node = EventLeafNode {
            keys: vec![last_key],
            values: vec![last_value.clone()],
        };
        let mut new_leaf_page = Page::new(new_leaf_page_id, Node::EventLeaf(new_leaf_node.clone()));
        let serialized_size = new_leaf_page.calc_serialized_size();
        if serialized_size > mvcc.page_size
            && let EventValue::Inline(rec) = last_value
        {
            let root_id = write_overflow_chain(mvcc, writer, &rec.data)?;
            last_value = EventValue::Overflow {
                event_type: rec.event_type,
                data_len: rec.data.len() as u64,
                tags: rec.tags,
                root_id,
                uuid: rec.uuid,
            };
            new_leaf_node = EventLeafNode {
                keys: vec![last_key],
                values: vec![last_value.clone()],
            };
            new_leaf_page = Page::new(new_leaf_page_id, Node::EventLeaf(new_leaf_node.clone()));
            // serialized_size = new_leaf_page.calc_serialized_size();
        }
        // if serialized_size > mvcc.page_size {
        //     return Err(DCBError::DatabaseCorrupted(format!(
        //         "Event too large even after overflow conversion (size: {serialized_size}, max: {})",
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
            println!("Promoting {last_key:?} and {new_leaf_page_id:?}");
        }
        split_info = Some((last_key, new_leaf_page_id));
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
            return Err(DCBError::DatabaseCorrupted(
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
                return Err(DCBError::DatabaseCorrupted(
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
                    return Err(DCBError::DatabaseCorrupted(
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
                return Err(DCBError::DatabaseCorrupted(
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
            return Err(DCBError::RootIDMismatch(old_id.0, new_id.0));
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
) -> DCBResult<EventRecord> {
    let mut current_page_id: PageID = events_tree_root_id;
    loop {
        // Prefer the dirty (unflushed) page if present; otherwise read from disk
        let page = if let Some(p) = dirty.get(&current_page_id) {
            Cow::Borrowed(p)
        } else {
            Cow::Owned(mvcc.read_page(current_page_id)?)
        };
        match &page.node {
            Node::EventInternal(internal) => {
                // Choose child based on upper bound of position in separator keys
                let idx = match internal.keys.binary_search(&position) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                };
                if idx >= internal.child_ids.len() {
                    return Err(DCBError::DatabaseCorrupted(
                        "Child index out of bounds in event tree".to_string(),
                    ));
                }
                current_page_id = internal.child_ids[idx];
            }
            Node::EventLeaf(leaf) => {
                return match leaf.keys.binary_search(&position) {
                    Ok(i) => {
                        let rec = materialize_event_value(mvcc, dirty, &leaf.values[i])?;
                        Ok(rec)
                    }
                    Err(_) => Err(DCBError::DatabaseCorrupted(format!(
                        "Event at position {position:?} not found",
                    ))),
                };
            }
            _ => {
                return Err(DCBError::DatabaseCorrupted(format!(
                    "Expected EventInternal or EventLeaf node in event tree, got {}",
                    page.node.type_name()
                )));
            }
        }
    }
}

pub struct EventIterator<'a> {
    pub mvcc: &'a Mvcc,
    pub dirty: &'a HashMap<PageID, Page>,
    pub stack: Vec<(PageID, Option<usize>)>,
    pub page_cache: HashMap<PageID, Page>,
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

    pub fn next_batch(&mut self, batch_size: u32) -> DCBResult<Vec<(Position, EventRecord)>> {
        let mut result: Vec<(Position, EventRecord)> = Vec::with_capacity(batch_size as usize);
        if batch_size == 0 {
            return Ok(result);
        }
        if self.backwards && self.start == Some(Position(0)) {
            return Ok(result);
        }
        while result.len() < batch_size as usize {
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
                    p
                } else {
                    let page = self.mvcc.read_page(page_id)?;
                    self.page_cache.insert(page_id, page);
                    self.page_cache
                        .get(&page_id)
                        .expect("page should be in cache")
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
                            // TODO: Clarify if this is always because internal node is empty?
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
                        return Err(DCBError::DatabaseCorrupted(format!(
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
    use crate::node::Node;
    use rand::random;
    use serial_test::serial;
    use tempfile::tempdir;

    static VERBOSE: bool = false;

    // Helper function to create a test database with a specified page size
    fn construct_db(page_size: usize) -> (tempfile::TempDir, Mvcc) {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("mvcc-test.db");
        let db = Mvcc::new(&db_path, page_size, VERBOSE).unwrap();
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
        let (_header_page_id, header) = db.get_latest_header().unwrap();
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
            let batch = events_iterator.next_batch(3).unwrap();
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
                let batch = events_iterator.next_batch(3).unwrap();
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
                let batch = events_iterator.next_batch(3).unwrap();
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
        };
        event_tree_append(&db, &mut writer, event.clone(), pos).unwrap();
        db.commit(&mut writer).unwrap();

        // Lookup should return identical payload
        let reader = db.reader().unwrap();
        let dirty = HashMap::new();
        let got = event_tree_lookup(&db, &dirty, reader.events_tree_root_id, pos).unwrap();
        assert_eq!(event, got);

        // Ensure an overflow page is used for storage
        let (_hdr_id, header) = db.get_latest_header().unwrap();
        let root = db.read_page(header.events_tree_root_id).unwrap();
        match root.node {
            Node::EventInternal(internal) => {
                // Our key should be in the last child
                let leaf_id = *internal.child_ids.last().unwrap();
                let leaf_page = db.read_page(leaf_id).unwrap();
                match leaf_page.node {
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
                EventValue::Overflow { data_len, .. } => assert_eq!(*data_len as usize, data.len()),
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
        };
        event_tree_append(&db, &mut writer, event.clone(), pos).unwrap();
        db.commit(&mut writer).unwrap();

        // Lookup
        let reader = db.reader().unwrap();
        let dirty = HashMap::new();
        let got = event_tree_lookup(&db, &dirty, reader.events_tree_root_id, pos).unwrap();
        assert_eq!(event, got);

        // Ensure overflow in leaf
        let (_hdr_id, header) = db.get_latest_header().unwrap();
        let root = db.read_page(header.events_tree_root_id).unwrap();
        let check_leaf = |leaf: &EventLeafNode| match &leaf.values[0] {
            EventValue::Overflow { data_len, .. } => assert_eq!(*data_len as usize, data.len()),
            _ => panic!("Expected Overflow for very large event"),
        };
        match root.node {
            Node::EventInternal(internal) => {
                let leaf_id = *internal.child_ids.last().unwrap();
                let leaf_page = db.read_page(leaf_id).unwrap();
                match leaf_page.node {
                    Node::EventLeaf(leaf) => check_leaf(&leaf),
                    _ => panic!("Expected leaf"),
                }
            }
            Node::EventLeaf(leaf) => check_leaf(&leaf),
            _ => panic!("Unexpected root node type"),
        }
    }

    // #[test]
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
}
