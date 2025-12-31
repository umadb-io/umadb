use crate::common::{PageID, Position};
use crate::mvcc::{Mvcc, Writer};
use crate::node::Node;
use crate::page::Page;
use crate::tags_tree_nodes::{
    TagHash, TagInternalNode, TagLeafNode, TagsInternalNode, TagsLeafNode, TagsLeafValue,
};
use std::collections::HashMap;
use umadb_dcb::{DCBError, DCBResult};

/// Insert a Position into the tags tree at the given TagHash key.
///
/// Behaves similarly to Writer::insert_freed_page_id, but operates on the
/// Tags tree (TagsLeafNode/TagsInternalNode) and maintains TagHash keys in
/// sorted order. If the TagHash already exists in the leaf and the value has no
/// subtree (root_id == PageID(0)), the Position is appended to the positions
/// vector. Otherwise a new key/value pair is inserted at the correct sorted
/// index.
pub fn tags_tree_insert(
    mvcc: &Mvcc,
    writer: &mut Writer,
    tag: TagHash,
    pos: Position,
) -> DCBResult<()> {
    let verbose = mvcc.verbose;
    if verbose {
        println!("Inserting position {pos:?} for tag {tag:?}");
        println!("Tags root is {:?}", writer.tags_tree_root_id);
    }

    // Start from the root of the tags tree
    let mut current_page_id: PageID = writer.tags_tree_root_id;

    // Traverse to the correct leaf, keeping track of parent ids and the child index taken at each step
    let mut stack: Vec<(PageID, usize)> = Vec::new();
    loop {
        let current_page_ref = writer.get_page_ref(mvcc, current_page_id)?;
        match &current_page_ref.node {
            Node::TagsLeaf(_) => break,
            Node::TagsInternal(internal_node) => {
                if verbose {
                    println!("{:?} is TagsInternal node", current_page_ref.page_id);
                }
                // Decide child index using B-tree separator convention:
                // keys[i] is the minimum key in child i+1
                let child_idx = match internal_node.keys.binary_search(&tag) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                };
                // Push (parent_page_id, child_idx taken)
                stack.push((current_page_id, child_idx));
                current_page_id = internal_node.child_ids[child_idx];
            }
            _ => {
                return Err(DCBError::DatabaseCorrupted(
                    "Invalid node type in tags tree (expected TagsInternal/TagsLeaf)".to_string(),
                ));
            }
        }
    }

    if verbose {
        println!("{current_page_id:?} is TagsLeaf node");
    }

    // Make the leaf page dirty (copy-on-write if needed)
    let dirty_leaf_page_id = writer.get_dirty_page_id(current_page_id)?;
    // If copy-on-write happened, we need to replace the child id in the parent at the recorded index
    let mut replacement_info: Option<(PageID, PageID)> = None;
    if dirty_leaf_page_id != current_page_id {
        replacement_info = Some((current_page_id, dirty_leaf_page_id));
    }

    // Insert or append the position at the sorted index
    // We must avoid holding a mutable borrow of the leaf while allocating/inserting pages.
    // So we compute what to do, mutate minimally, then perform follow-up actions after dropping the borrow.
    let mut per_tag_append_root: Option<PageID> = None;
    let mut inline_appended_index: Option<usize> = None;
    {
        let leaf_page = writer.get_mut_dirty(dirty_leaf_page_id)?;
        match &mut leaf_page.node {
            Node::TagsLeaf(leaf) => {
                match leaf.keys.binary_search(&tag) {
                    Ok(i) => {
                        let root = leaf.values[i].root_id;
                        if root == PageID(0) {
                            // Append inline for now; we may migrate after we drop the borrow
                            leaf.values[i].positions.push(pos);
                            inline_appended_index = Some(i);
                            if verbose {
                                println!("Appended position to existing tag at index {i}");
                            }
                        } else {
                            // Defer per-tag append to after we drop the borrow
                            per_tag_append_root = Some(root);
                        }
                    }
                    Err(i) => {
                        // Insert new key/value at the correct index
                        leaf.keys.insert(i, tag);
                        leaf.values.insert(
                            i,
                            TagsLeafValue {
                                root_id: PageID(0),
                                positions: vec![pos],
                            },
                        );
                        if verbose {
                            println!("Inserted new tag at index {i}");
                        }
                    }
                }
            }
            _ => {
                return Err(DCBError::DatabaseCorrupted(
                    "Expected TagsLeaf node".to_string(),
                ));
            }
        }
    }

    // If we need to append into an existing per-tag leaf page, do it now (avoid overlapping borrows)
    if let Some(mut tag_root_id) = per_tag_append_root.take() {
        // Ensure tag page is dirty (copy-on-write)
        let dirty_tag_id = {
            // No outstanding borrows of writer here
            // Make sure the page is present in this writer's cache before COW
            let _ = writer.get_page_ref(mvcc, tag_root_id)?;
            writer.get_dirty_page_id(tag_root_id)?
        };
        if dirty_tag_id != tag_root_id {
            // Update the root_id in the TagsLeaf value to the new dirty page id
            tag_root_id = dirty_tag_id;
            {
                let leaf_page = writer.get_mut_dirty(dirty_leaf_page_id)?;
                if let Node::TagsLeaf(leaf) = &mut leaf_page.node {
                    let idx = leaf.keys.binary_search(&tag).map_err(|_| {
                        DCBError::DatabaseCorrupted("Tag key not found after COW".to_string())
                    })?;
                    leaf.values[idx].root_id = tag_root_id;
                } else {
                    return Err(DCBError::DatabaseCorrupted(
                        "Expected TagsLeaf node".to_string(),
                    ));
                }
            }
        }
        // Now insert into the per-tag subtree (handles TagLeaf and TagInternal roots)
        {
            // Traverse down to a leaf
            let mut current_page_id = tag_root_id;
            let mut stack: Vec<(PageID, usize)> = Vec::new();
            loop {
                let page_ref = writer.get_page_ref(mvcc, current_page_id)?;
                match &page_ref.node {
                    Node::TagInternal(internal) => {
                        let child_idx = match internal.keys.binary_search(&pos) {
                            Ok(i) => i + 1,
                            Err(i) => i,
                        };
                        stack.push((current_page_id, child_idx));
                        current_page_id = internal.child_ids[child_idx];
                    }
                    Node::TagLeaf(_) => break,
                    _ => {
                        return Err(DCBError::DatabaseCorrupted(
                            "Expected per-tag TagInternal/TagLeaf".to_string(),
                        ));
                    }
                }
            }

            // Make the leaf dirty and note replacement if COW
            let dirty_leaf_id = writer.get_dirty_page_id(current_page_id)?;
            let mut replacement_info: Option<(PageID, PageID)> = None;
            if dirty_leaf_id != current_page_id {
                replacement_info = Some((current_page_id, dirty_leaf_id));
            }

            // Insert into leaf and check overflow
            let mut split_info: Option<(Position, PageID)> = None; // (promoted_key, new_right_page_id)
            {
                let leaf_page = writer.get_mut_dirty(dirty_leaf_id)?;
                match &mut leaf_page.node {
                    Node::TagLeaf(tleaf) => {
                        tleaf.positions.push(pos);
                        let page_bytes =
                            crate::page::PAGE_HEADER_SIZE + tleaf.calc_serialized_size();
                        if page_bytes > mvcc.page_size {
                            // Move last pos to a new right leaf
                            let last_pos = tleaf
                                .pop_last_position()
                                .map_err(|e| DCBError::DatabaseCorrupted(format!("{e}")))?;
                            let right_id = {
                                let id = writer.alloc_page_id();
                                let page = Page::new(
                                    id,
                                    Node::TagLeaf(TagLeafNode {
                                        positions: vec![last_pos],
                                    }),
                                );
                                writer.insert_dirty(page)?;
                                id
                            };
                            split_info = Some((last_pos, right_id));
                        }
                    }
                    _ => {
                        return Err(DCBError::DatabaseCorrupted(
                            "Expected TagLeaf at per-tag insert".to_string(),
                        ));
                    }
                }
            }

            // Propagate replacements and splits up the per-tag subtree
            while let Some((parent_id, child_idx)) = stack.pop() {
                // COW parent if needed
                let dirty_parent_id = writer.get_dirty_page_id(parent_id)?;
                let parent_replacement_info = if dirty_parent_id != parent_id {
                    Some((parent_id, dirty_parent_id))
                } else {
                    None
                };

                // Apply child replacement if any
                if let Some((old_id, new_id)) = replacement_info.take() {
                    let parent_page = writer.get_mut_dirty(dirty_parent_id)?;
                    if let Node::TagInternal(internal) = &mut parent_page.node {
                        if internal.child_ids[child_idx] == old_id {
                            internal.child_ids[child_idx] = new_id;
                        } else {
                            return Err(DCBError::DatabaseCorrupted(
                                "Per-tag parent did not contain expected child id".to_string(),
                            ));
                        }
                    } else {
                        return Err(DCBError::DatabaseCorrupted(
                            "Expected TagInternal".to_string(),
                        ));
                    }
                }

                // Apply promoted split from below if any
                if let Some((promoted_key, new_child_id)) = split_info.take() {
                    let parent_page = writer.get_mut_dirty(dirty_parent_id)?;
                    if let Node::TagInternal(internal) = &mut parent_page.node {
                        internal.keys.insert(child_idx, promoted_key);
                        internal.child_ids.insert(child_idx + 1, new_child_id);
                    } else {
                        return Err(DCBError::DatabaseCorrupted(
                            "Expected TagInternal".to_string(),
                        ));
                    }
                }

                // Now check for internal overflow and split if needed
                let parent_page = writer.get_mut_dirty(dirty_parent_id)?;
                let needs_split = parent_page.calc_serialized_size() > mvcc.page_size;
                if needs_split {
                    if let Node::TagInternal(internal) = &mut parent_page.node {
                        if internal.keys.len() < 3 || internal.child_ids.len() < 4 {
                            return Err(DCBError::DatabaseCorrupted(
                                "Cannot split per-tag internal with too few keys/children"
                                    .to_string(),
                            ));
                        }
                        let (promote_up, new_keys, new_child_ids) = internal.split_off()?;
                        let new_internal = TagInternalNode {
                            keys: new_keys,
                            child_ids: new_child_ids,
                        };
                        let new_internal_id = writer.alloc_page_id();
                        let new_internal_page =
                            Page::new(new_internal_id, Node::TagInternal(new_internal));
                        writer.insert_dirty(new_internal_page)?;
                        split_info = Some((promote_up, new_internal_id));
                    } else {
                        return Err(DCBError::DatabaseCorrupted(
                            "Expected TagInternal".to_string(),
                        ));
                    }
                }

                // Prepare replacement info for upper level if parent was COWed
                replacement_info = parent_replacement_info;
            }

            // Apply root replacement for the per-tag root if needed
            if let Some((old_id, new_id)) = replacement_info.take() {
                if tag_root_id == old_id {
                    tag_root_id = new_id;
                } else {
                    return Err(DCBError::RootIDMismatch(old_id.0, new_id.0));
                }
            }

            // If we still have a split to propagate, create a new per-tag internal root
            if let Some((promoted_key, promoted_page_id)) = split_info.take() {
                let new_root_id = writer.alloc_page_id();
                let new_root = TagInternalNode {
                    keys: vec![promoted_key],
                    child_ids: vec![tag_root_id, promoted_page_id],
                };
                let new_root_page = Page::new(new_root_id, Node::TagInternal(new_root));
                writer.insert_dirty(new_root_page)?;
                tag_root_id = new_root_id;
            }

            // Update the TagsLeafValue.root_id to the latest per-tag root id
            let leaf_page = writer.get_mut_dirty(dirty_leaf_page_id)?;
            if let Node::TagsLeaf(leaf) = &mut leaf_page.node {
                let idx = leaf.keys.binary_search(&tag).map_err(|_| {
                    DCBError::DatabaseCorrupted(
                        "Tag key not found after per-tag insert".to_string(),
                    )
                })?;
                leaf.values[idx].root_id = tag_root_id;
            } else {
                return Err(DCBError::DatabaseCorrupted(
                    "Expected TagsLeaf node".to_string(),
                ));
            }
        }
    }

    // If we appended inline, check if the page overflowed and migrate positions to a per-tag TagLeaf page
    if let Some(i) = inline_appended_index.take() {
        let sz = writer
            .get_page_ref(mvcc, dirty_leaf_page_id)?
            .calc_serialized_size();
        if sz > mvcc.page_size {
            if verbose {
                println!("Migrating inline positions to per-tag TagLeafNode for index {i}",);
            }
            // Take positions out into a local var
            let positions = {
                let leaf_page = writer.get_mut_dirty(dirty_leaf_page_id)?;
                if let Node::TagsLeaf(leaf) = &mut leaf_page.node {
                    std::mem::take(&mut leaf.values[i].positions)
                } else {
                    return Err(DCBError::DatabaseCorrupted(
                        "Expected TagsLeaf node".to_string(),
                    ));
                }
            };
            // Create per-tag page or split into an internal if needed
            let new_root_id = {
                let mut pos_vec = positions;
                let page_bytes = crate::page::PAGE_HEADER_SIZE
                    + TagLeafNode {
                        positions: pos_vec.clone(),
                    }
                    .calc_serialized_size();
                if page_bytes <= mvcc.page_size {
                    let tag_leaf_id = writer.alloc_page_id();
                    let tag_leaf_page = Page::new(
                        tag_leaf_id,
                        Node::TagLeaf(TagLeafNode { positions: pos_vec }),
                    );
                    writer.insert_dirty(tag_leaf_page)?;
                    tag_leaf_id
                } else {
                    // Split: move the last position to the right leaf and create an internal root
                    let last_pos = pos_vec.pop().ok_or_else(|| {
                        DCBError::DatabaseCorrupted("No positions to split".to_string())
                    })?;
                    let left_bytes = crate::page::PAGE_HEADER_SIZE
                        + TagLeafNode {
                            positions: pos_vec.clone(),
                        }
                        .calc_serialized_size();
                    if left_bytes > mvcc.page_size {
                        return Err(DCBError::DatabaseCorrupted(
                            "Recursive per-tag split not implemented".to_string(),
                        ));
                    }
                    let left_id = {
                        let id = writer.alloc_page_id();
                        let page = Page::new(id, Node::TagLeaf(TagLeafNode { positions: pos_vec }));
                        writer.insert_dirty(page)?;
                        id
                    };
                    let right_id = {
                        let id = writer.alloc_page_id();
                        let page = Page::new(
                            id,
                            Node::TagLeaf(TagLeafNode {
                                positions: vec![last_pos],
                            }),
                        );
                        writer.insert_dirty(page)?;
                        id
                    };
                    {
                        let internal = TagInternalNode {
                            keys: vec![last_pos],
                            child_ids: vec![left_id, right_id],
                        };
                        let id = writer.alloc_page_id();
                        let page = Page::new(id, Node::TagInternal(internal));
                        writer.insert_dirty(page)?;
                        id
                    }
                }
            };
            // Update root_id in the leaf
            let leaf_page = writer.get_mut_dirty(dirty_leaf_page_id)?;
            if let Node::TagsLeaf(leaf) = &mut leaf_page.node {
                leaf.values[i].root_id = new_root_id;
            } else {
                return Err(DCBError::DatabaseCorrupted(
                    "Expected TagsLeaf node".to_string(),
                ));
            }
        }
    }

    // Track split information as (promoted_key, new_page_id, parent_child_idx_to_insert_after)
    let mut split_info: Option<(TagHash, PageID)> = None;

    // Check if leaf overflows
    let needs_split = {
        let page = writer.get_mut_dirty(dirty_leaf_page_id)?;
        page.calc_serialized_size() > mvcc.page_size
    };
    if needs_split {
        let leaf_page = writer.get_mut_dirty(dirty_leaf_page_id)?;
        if let Node::TagsLeaf(leaf) = &mut leaf_page.node {
            // Move half of the keys and values to a new right sibling
            let mid = leaf.keys.len() / 2;
            let right_keys: Vec<TagHash> = leaf.keys.split_off(mid);
            let right_values: Vec<TagsLeafValue> = leaf.values.split_off(mid);
            let promoted_key = right_keys[0];
            if verbose {
                println!(
                    "Split TagsLeaf {:?}, moving {} keys to new right sibling; promoted key {:?}",
                    dirty_leaf_page_id,
                    right_keys.len(),
                    promoted_key
                );
            }
            let new_leaf_node = TagsLeafNode {
                keys: right_keys,
                values: right_values,
            };
            let new_leaf_page_id = writer.alloc_page_id();
            let new_leaf_page = Page::new(new_leaf_page_id, Node::TagsLeaf(new_leaf_node));
            writer.insert_dirty(new_leaf_page)?;

            split_info = Some((promoted_key, new_leaf_page_id));
        }
    }

    // Propagate replacements and splits up the tree
    let mut current_replacement_info = replacement_info; // (old_id, new_id, idx)
    while let Some((parent_page_id, parent_child_idx)) = stack.pop() {
        // Make parent page dirty
        let dirty_parent_page_id = writer.get_dirty_page_id(parent_page_id)?;
        let parent_replacement_info = if dirty_parent_page_id != parent_page_id {
            // Need to replace this parent in its own parent later
            Some((parent_page_id, dirty_parent_page_id))
        } else {
            None
        };

        let parent_page = writer.get_mut_dirty(dirty_parent_page_id)?;
        // First, apply child replacement if needed
        if let Node::TagsInternal(internal) = &mut parent_page.node {
            if let Some((old_id, new_id)) = current_replacement_info.take() {
                let target_idx = parent_child_idx;
                if internal.child_ids[target_idx] == old_id {
                    internal.child_ids[target_idx] = new_id;
                    if verbose {
                        println!(
                            "Replaced child at idx {target_idx}: {old_id:?} -> {new_id:?} in {dirty_parent_page_id:?}",
                        );
                    }
                } else {
                    return Err(DCBError::DatabaseCorrupted(
                        "Parent did not contain expected child id".to_string(),
                    ));
                }
            } else if verbose {
                println!("No child replacement needed in {dirty_parent_page_id:?}");
            }
        } else {
            return Err(DCBError::DatabaseCorrupted(
                "Expected TagsInternal node".to_string(),
            ));
        }

        // Then, apply promoted split from below, if any
        if let Some((promoted_key, new_child_id)) = split_info.take() {
            if let Node::TagsInternal(internal) = &mut parent_page.node {
                let insert_key_idx = parent_child_idx;
                let insert_child_idx = parent_child_idx + 1;
                internal.keys.insert(insert_key_idx, promoted_key);
                internal.child_ids.insert(insert_child_idx, new_child_id);
                if verbose {
                    println!(
                        "Inserted promoted key at {insert_key_idx} and child at {insert_child_idx} in {dirty_parent_page_id:?}",
                    );
                }
            } else {
                return Err(DCBError::DatabaseCorrupted(
                    "Expected TagsInternal node".to_string(),
                ));
            }
        }

        // Now check for internal overflow after any insertion
        let needs_split = parent_page.calc_serialized_size() > mvcc.page_size;
        if needs_split {
            if let Node::TagsInternal(internal) = &mut parent_page.node {
                if verbose {
                    println!("Splitting TagsInternal {dirty_parent_page_id:?}...");
                }
                if internal.keys.len() < 3 || internal.child_ids.len() < 4 {
                    return Err(DCBError::DatabaseCorrupted(
                        "Cannot split internal node with too few keys/children".to_string(),
                    ));
                }
                let (promote_up, new_keys, new_child_ids) = internal.split_off()?;
                let new_internal = TagsInternalNode {
                    keys: new_keys,
                    child_ids: new_child_ids,
                };
                assert_eq!(internal.keys.len() + 1, internal.child_ids.len());
                assert_eq!(new_internal.keys.len() + 1, new_internal.child_ids.len());
                let new_internal_id = writer.alloc_page_id();
                let new_internal_page =
                    Page::new(new_internal_id, Node::TagsInternal(new_internal));
                if verbose {
                    println!("Created new TagsInternal {new_internal_id:?}");
                }
                writer.insert_dirty(new_internal_page)?;
                split_info = Some((promote_up, new_internal_id));
            } else {
                return Err(DCBError::DatabaseCorrupted(
                    "Expected TagsInternal node".to_string(),
                ));
            }
        } else {
            split_info = None;
        }

        // Prepare replacement info for the next level up if this parent was copied-on-write
        current_replacement_info = parent_replacement_info;
    }

    // Apply root replacement if needed
    if let Some((old_id, new_id)) = current_replacement_info.take() {
        if writer.tags_tree_root_id == old_id {
            writer.tags_tree_root_id = new_id;
            if verbose {
                println!("Replaced Tags root {old_id:?} -> {new_id:?}");
            }
        } else {
            return Err(DCBError::RootIDMismatch(old_id.0, new_id.0));
        }
    }

    // If we still have a split to propagate, create a new internal root
    if let Some((promoted_key, promoted_page_id)) = split_info.take() {
        let new_root_id = writer.alloc_page_id();
        let new_root = TagsInternalNode {
            keys: vec![promoted_key],
            child_ids: vec![writer.tags_tree_root_id, promoted_page_id],
        };
        let new_root_page = Page::new(new_root_id, Node::TagsInternal(new_root));
        if verbose {
            println!("Created new TagsInternal root {new_root_id:?}");
        }
        writer.insert_dirty(new_root_page)?;
        writer.tags_tree_root_id = new_root_id;
    }

    Ok(())
}

// Iterator over positions for a given tag in the tags tree
pub struct TagsTreeIterator<'a> {
    db: &'a Mvcc,
    dirty: &'a HashMap<PageID, Page>,
    tags_root_id: PageID,
    tag: TagHash,
    start: Option<Position>,
    backwards: bool,
    // New traversal machinery similar to EventIterator
    stack: Vec<(PageID, Option<usize>)>,
    page_cache: HashMap<PageID, Page>,
    // Current batch of positions (from a single page)
    batch: Vec<Position>,
    batch_index: usize,
    state: IterState,
}

enum IterState {
    NotStarted,
    Ready,
    Done,
}

impl<'a> TagsTreeIterator<'a> {
    pub fn new(
        db: &'a Mvcc,
        dirty: &'a HashMap<PageID, Page>,
        tags_root_id: PageID,
        tag: TagHash,
        start: Option<Position>,
        backwards: bool,
    ) -> Self {
        Self {
            db,
            dirty,
            tags_root_id,
            tag,
            start,
            backwards,
            stack: Vec::new(),
            page_cache: HashMap::new(),
            batch: Vec::new(),
            batch_index: 0,
            state: IterState::NotStarted,
        }
    }
}

impl<'a> Iterator for TagsTreeIterator<'a> {
    type Item = Position;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.state {
                IterState::NotStarted | IterState::Ready => {
                    // If current batch is empty or exhausted, fetch next batch
                    if self.batch_index >= self.batch.len() {
                        if !self.next_batch() {
                            self.state = IterState::Done;
                            continue;
                        }
                        self.state = IterState::Ready;
                    }
                    if self.batch_index < self.batch.len() {
                        let p = self.batch[self.batch_index];
                        self.batch_index += 1;
                        return Some(p);
                    } else {
                        // No items in batch and no more batches
                        self.state = IterState::Done;
                        continue;
                    }
                }
                IterState::Done => return None,
            }
        }
    }
}

impl<'a> TagsTreeIterator<'a> {
    // Return next batch (positions from a single page). Returns false if no more batches.
    fn next_batch(&mut self) -> bool {
        let tag = self.tag;
        let from = self.start;
        match self.state {
            IterState::NotStarted => {
                // Descend the tags tree to find the entry for the tag
                let mut current_page_id: PageID = self.tags_root_id;
                loop {
                    // Use a scoped borrow to avoid holding &mut self across later mutations
                    let mut next_child: Option<PageID> = None;
                    let mut found_inline: Option<Vec<Position>> = None;
                    let mut per_tag_root: Option<PageID> = None;
                    {
                        let page = match self.get_page_cached(current_page_id) {
                            Ok(p) => p,
                            Err(_) => {
                                self.state = IterState::Done;
                                return false;
                            }
                        };
                        match &page.node {
                            Node::TagsInternal(internal) => {
                                let idx = match internal.keys.binary_search(&tag) {
                                    Ok(i) => i + 1,
                                    Err(i) => i,
                                };
                                if idx >= internal.child_ids.len() {
                                    self.state = IterState::Done;
                                    return false;
                                }
                                next_child = Some(internal.child_ids[idx]);
                            }
                            Node::TagsLeaf(leaf) => match leaf.keys.binary_search(&tag) {
                                Ok(i) => {
                                    let val = &leaf.values[i];
                                    if val.root_id == PageID(0) {
                                        found_inline = Some(val.positions.clone());
                                    } else {
                                        per_tag_root = Some(val.root_id);
                                    }
                                }
                                Err(_) => {
                                    self.state = IterState::Done;
                                    return false;
                                }
                            },
                            _ => {
                                self.state = IterState::Done;
                                return false;
                            }
                        }
                    }
                    if let Some(child) = next_child {
                        current_page_id = child;
                        continue;
                    }
                    if let Some(mut positions) = found_inline {
                        if !self.backwards {
                            if let Some(f) = from {
                                let start_idx = positions.partition_point(|p| *p < f);
                                self.batch = positions.split_off(start_idx);
                            } else {
                                // No lower bound: take all positions in forward order
                                self.batch = positions;
                            }
                        } else {
                            if let Some(f) = from {
                                // take positions <= f and iterate in reverse order
                                let end_idx = positions.partition_point(|p| *p <= f);
                                self.batch = positions.drain(..end_idx).collect();
                            } else {
                                // No upper bound: take all then reverse for backwards
                                self.batch = positions;
                            }
                            self.batch.reverse();
                        }
                        self.batch_index = 0;
                        return !self.batch.is_empty();
                    }
                    if let Some(root) = per_tag_root {
                        self.stack.clear();
                        self.page_cache.shrink_to_fit(); // optional, keep cache small early
                        self.stack.push((root, None));
                        break; // proceed to traverse subtree for first batch
                    }
                    // If neither inline nor per_tag_root found, done
                    self.state = IterState::Done;
                    return false;
                }
                // fallthrough to traverse subtree stack below
            }
            IterState::Ready => { /* continue to traverse subtree or finish */ }
            IterState::Done => return false,
        }

        // Traverse per-tag subtree like EventIterator: DFS, each TagLeaf yields a batch
        while let Some((page_id, mut stacked_idx)) = self.stack.pop() {
            // Plan pushes and batch under a scoped immutable borrow first
            let mut remove_page = false;
            let mut push_revisit: Option<(PageID, Option<usize>)> = None;
            let mut push_child: Option<(PageID, Option<usize>)> = None;
            let mut leaf_batch: Option<Vec<Position>> = None;
            let backwards = self.backwards;
            {
                let page_ref = match self.get_page_cached(page_id) {
                    Ok(p) => p,
                    Err(_) => {
                        self.state = IterState::Done;
                        return false;
                    }
                };
                match &page_ref.node {
                    Node::TagInternal(internal) => {
                        if stacked_idx.is_none() && !internal.keys.is_empty() {
                            stacked_idx = match from {
                                Some(f) => match internal.keys.binary_search(&f) {
                                    Ok(i) => Some(i + 1),
                                    Err(i) => Some(i),
                                },
                                None => {
                                    if !backwards {
                                        Some(0)
                                    } else {
                                        Some(internal.child_ids.len().saturating_sub(1))
                                    }
                                }
                            };
                        }
                        let child_idx = stacked_idx.unwrap_or(if !backwards {
                            0
                        } else {
                            internal.child_ids.len().saturating_sub(1)
                        });
                        if child_idx < internal.child_ids.len() {
                            // Determine revisit direction based on backwards flag
                            if !backwards {
                                if child_idx + 1 < internal.child_ids.len() {
                                    push_revisit = Some((page_id, Some(child_idx + 1)));
                                } else {
                                    remove_page = true;
                                }
                            } else if child_idx > 0 {
                                push_revisit = Some((page_id, Some(child_idx - 1)));
                            } else {
                                remove_page = true;
                            }
                            push_child = Some((internal.child_ids[child_idx], None));
                        } else {
                            // No valid child at computed index; drop this page from cache
                            remove_page = true;
                        }
                    }
                    Node::TagLeaf(tleaf) => {
                        if !tleaf.positions.is_empty() {
                            if !backwards {
                                if let Some(f) = from {
                                    let start_idx = tleaf.positions.partition_point(|p| *p < f);
                                    if start_idx < tleaf.positions.len() {
                                        let batch = tleaf.positions[start_idx..].to_vec();
                                        leaf_batch = Some(batch);
                                    }
                                } else {
                                    // No lower bound: entire leaf in forward order
                                    leaf_batch = Some(tleaf.positions.clone());
                                }
                            } else if let Some(f) = from {
                                let end_idx = tleaf.positions.partition_point(|p| *p <= f);
                                if end_idx > 0 {
                                    let mut batch: Vec<Position> =
                                        tleaf.positions[..end_idx].to_vec();
                                    batch.reverse();
                                    leaf_batch = Some(batch);
                                }
                            } else {
                                // No upper bound: entire leaf reversed
                                let mut batch = tleaf.positions.clone();
                                batch.reverse();
                                leaf_batch = Some(batch);
                            }
                        }
                        // We won't revisit a TagLeaf: remove from cache after processing
                        remove_page = true;
                    }
                    _ => {
                        // Unknown node kind in per-tag subtree; drop it
                        remove_page = true;
                    }
                }
            }
            if let Some(rev) = push_revisit {
                self.stack.push(rev);
            }
            if let Some(ch) = push_child {
                self.stack.push(ch);
            }
            if remove_page {
                self.page_cache.remove(&page_id);
            }
            if let Some(batch) = leaf_batch {
                self.batch = batch;
                self.batch_index = 0;
                return true;
            }
        }
        // No more leaves/batches
        false
    }

    fn get_page_cached(&mut self, page_id: PageID) -> DCBResult<&Page> {
        if let Some(p) = self.dirty.get(&page_id) {
            return Ok(p);
        }
        if !self.page_cache.contains_key(&page_id) {
            let page = self.db.read_page(page_id)?;
            self.page_cache.insert(page_id, page);
        }
        Ok(self.page_cache.get(&page_id).expect("cached page missing"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::Mvcc;
    use tempfile::{TempDir, tempdir};

    static VERBOSE: bool = false;

    // Test helper: iterate a range of `from` positions and assert lookup returns
    // the positions that were inserted for the given tag and are >= `from`.
    fn verify_tag_lookup_range(db: &Mvcc, tag: TagHash, inserted: &Vec<Position>, backwards: bool) {
        // Try a range that goes slightly below the first position and past the last.
        let len = inserted.len();
        for i in 0..(len + 3) {
            let start = Position(i as u64);
            let reader = db.reader().unwrap();
            let got =
                tags_tree_lookup(db, reader.tags_tree_root_id, tag, start, backwards).unwrap();
            let expected: Vec<Position> = if !backwards {
                inserted
                    .iter()
                    .cloned()
                    .filter(|p| p.0 >= start.0)
                    .collect()
            } else {
                let mut v: Vec<Position> = inserted
                    .iter()
                    .cloned()
                    .filter(|p| p.0 <= start.0)
                    .collect();
                v.reverse();
                v
            };
            assert_eq!(expected, got, "from={:?}, backwards={}", start, backwards);
        }
    }

    fn construct_db(page_size: usize) -> (TempDir, Mvcc) {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("mvcc-test.db");
        let db = Mvcc::new(&db_path, page_size, VERBOSE).unwrap();
        (temp_dir, db)
    }

    fn th(n: u64) -> TagHash {
        // helper to generate a 16-byte TagHash from a number (for tests)
        let mut out: TagHash = [0u8; crate::tags_tree_nodes::TAG_HASH_LEN];
        out[0..8].copy_from_slice(&n.to_le_bytes());
        out
    }

    fn tags_tree_lookup(
        mvcc: &Mvcc,
        tags_root_id: PageID,
        tag: TagHash,
        from: Position,
        backwards: bool,
    ) -> DCBResult<Vec<Position>> {
        // Reuse the iterator to traverse and collect all positions for the tag
        let dirty = HashMap::new();
        let iter = TagsTreeIterator::new(mvcc, &dirty, tags_root_id, tag, Some(from), backwards);
        Ok(iter.collect())
    }

    #[test]
    fn test_insert_position_into_empty_leaf_root() {
        let (_tmp, db) = construct_db(1024);
        let mut writer = db.writer().unwrap();
        let tag = th(10);
        let pos = writer.issue_position();
        tags_tree_insert(&db, &mut writer, tag, pos).unwrap();
        db.commit(&mut writer).unwrap();

        let writer = db.writer().unwrap();
        // Verify by reading root page
        let page = db.read_page(writer.tags_tree_root_id).unwrap();
        match &page.node {
            Node::TagsLeaf(leaf) => {
                assert_eq!(leaf.keys.len(), 1);
                assert_eq!(leaf.keys[0], tag);
                assert_eq!(leaf.values[0].positions, vec![pos]);
            }
            _ => panic!("Expected TagsLeaf at root"),
        }
    }

    #[test]
    fn test_insert_positions_into_empty_leaf_root() {
        let (_tmp, db) = construct_db(1024);
        let mut writer = db.writer().unwrap();
        // Insert out of order
        let tags = [30u64, 10, 20, 20, 15];
        for &n in &tags {
            let pos = writer.issue_position();
            tags_tree_insert(&db, &mut writer, th(n), pos).unwrap();
        }
        // Verify sorted order and duplicate handling in leaf
        let page = writer.get_page_ref(&db, writer.tags_tree_root_id).unwrap();
        match &page.node {
            Node::TagsLeaf(leaf) => {
                // Extract unique sorted keys for expectation: 10,15,20,30
                assert_eq!(leaf.keys, vec![th(10), th(15), th(20), th(30)]);
                // 20 should have two positions
                let idx20 = leaf.keys.iter().position(|k| *k == th(20)).unwrap();
                assert!(leaf.values[idx20].positions.len() >= 2);
            }
            _ => panic!("Expected TagsLeaf at root"),
        }

        // Commit and verify lookup_tag on some keys
        db.commit(&mut writer).unwrap();
        let reader = db.reader().unwrap();
        let res_10 =
            tags_tree_lookup(&db, reader.tags_tree_root_id, th(10), Position(1), false).unwrap();
        assert!(!res_10.is_empty());
        let res_missing =
            tags_tree_lookup(&db, reader.tags_tree_root_id, th(999), Position(1), false).unwrap();
        assert!(res_missing.is_empty());
    }

    #[test]
    fn test_lookup_tag_returns_positions() {
        let (_tmp, db) = construct_db(1024);
        let mut writer = db.writer().unwrap();
        let t1 = th(42);
        let p1 = writer.issue_position();
        tags_tree_insert(&db, &mut writer, t1, p1).unwrap();
        let p2 = writer.issue_position();
        tags_tree_insert(&db, &mut writer, t1, p2).unwrap();
        let t2 = th(7);
        let p3 = writer.issue_position();
        tags_tree_insert(&db, &mut writer, t2, p3).unwrap();
        db.commit(&mut writer).unwrap();

        let reader = db.reader().unwrap();
        let vals = tags_tree_lookup(&db, reader.tags_tree_root_id, t1, Position(1), false).unwrap();
        assert_eq!(vals, vec![p1, p2]);
        // non-existent
        let vals_none =
            tags_tree_lookup(&db, reader.tags_tree_root_id, th(1000), Position(1), false).unwrap();
        assert!(vals_none.is_empty());
    }

    #[test]
    fn split_tags_tree_leaf_one_writer() {
        // Setup a temporary database
        let (_temp_dir, db) = construct_db(256);

        // Start a writer
        let mut writer = db.writer().unwrap();

        let mut has_split_leaf = false;
        let mut appended: Vec<(TagHash, Position)> = Vec::new();

        // Insert tag-position pairs until we split a leaf
        let mut n: u64 = 0;
        while !has_split_leaf {
            // Issue a new position and create a deterministic increasing tag
            let position = writer.issue_position();
            let tag = th(n);
            n += 1;
            appended.push((tag, position));

            // Insert the pair into the tags tree
            tags_tree_insert(&db, &mut writer, tag, position).unwrap();

            // Check if we've split the leaf
            let root_page = writer.dirty.get(&writer.tags_tree_root_id).unwrap();
            match &root_page.node {
                Node::TagsInternal(_) => {
                    has_split_leaf = true;
                }
                _ => {}
            }
        }

        // Check keys and values of all pages
        let mut copy_inserted = appended.clone();

        // Get the root node
        let root_page = writer.dirty.get(&writer.tags_tree_root_id).unwrap();
        let root_node = match &root_page.node {
            Node::TagsInternal(node) => node,
            _ => panic!("Expected TagsInternal node"),
        };

        // Check each child of the root
        for (i, &child_id) in root_node.child_ids.iter().enumerate() {
            let child_page = writer.dirty.get(&child_id).unwrap();
            assert_eq!(child_id, child_page.page_id);

            let child_node = match &child_page.node {
                Node::TagsLeaf(node) => node,
                _ => panic!("Expected TagsLeaf node"),
            };

            // Check that the keys are properly ordered
            if i > 0 {
                assert_eq!(root_node.keys[i - 1], child_node.keys[0]);
            }

            // Check each key and value in the child
            for (k, &key) in child_node.keys.iter().enumerate() {
                let val = &child_node.values[k];
                let (appended_tag, appended_pos) = copy_inserted.remove(0);
                assert_eq!(appended_tag, key);
                assert_eq!(val.positions.len(), 1);
                assert_eq!(val.positions[0], appended_pos);
            }
        }

        // Also verify lookup across a range of 'from' positions for a representative tag.
        db.commit(&mut writer).unwrap();
        let (tag_last, pos_last) = appended.last().cloned().unwrap();
        verify_tag_lookup_range(&db, tag_last, &vec![pos_last], false);
        verify_tag_lookup_range(&db, tag_last, &vec![pos_last], true);
    }

    #[test]
    fn split_tags_tree_internal_one_writer() {
        // Setup a temporary database
        let (_temp_dir, db) = construct_db(256);

        // Start a writer
        let mut writer = db.writer().unwrap();

        let mut has_split_internal = false;
        let mut appended: Vec<(TagHash, Position)> = Vec::new();

        // Insert tag-position pairs until we split an internal node
        let mut n: u64 = 0;
        while !has_split_internal {
            // Issue a new position and create a deterministic increasing tag
            let position = writer.issue_position();
            let tag = th(n);
            n += 1;
            appended.push((tag, position));

            // Insert the pair into the tags tree
            tags_tree_insert(&db, &mut writer, tag, position).unwrap();

            // Check if we've split an internal node: root is internal and its first child is also internal
            let root_page = writer.dirty.get(&writer.tags_tree_root_id).unwrap();
            match &root_page.node {
                Node::TagsInternal(root_node) => {
                    if !root_node.child_ids.is_empty() {
                        let child_id = root_node.child_ids[0];
                        if let Some(child_page) = writer.dirty.get(&child_id) {
                            match &child_page.node {
                                Node::TagsInternal(_) => {
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
        let root_page = writer.dirty.get(&writer.tags_tree_root_id).unwrap();
        let root_node = match &root_page.node {
            Node::TagsInternal(node) => node,
            _ => panic!("Expected TagsInternal node"),
        };

        // Check each child of the root
        for &child_id in root_node.child_ids.iter() {
            let child_page = writer.dirty.get(&child_id).unwrap();
            assert_eq!(child_id, child_page.page_id);

            let child_node = match &child_page.node {
                Node::TagsInternal(node) => node,
                _ => panic!("Expected TagsInternal node"),
            };

            for &grand_child_id in child_node.child_ids.iter() {
                let grand_child_page = writer.dirty.get(&grand_child_id).unwrap();
                assert_eq!(grand_child_id, grand_child_page.page_id);

                let grand_child_node = match &grand_child_page.node {
                    Node::TagsLeaf(node) => node,
                    _ => panic!("Expected TagsLeaf node"),
                };

                // Check each key and value in the leaf
                for (k, &key) in grand_child_node.keys.iter().enumerate() {
                    let val = &grand_child_node.values[k];
                    let (appended_tag, appended_pos) = copy_inserted.remove(0);
                    assert_eq!(appended_tag, key);
                    assert_eq!(val.positions.len(), 1);
                    assert_eq!(val.positions[0], appended_pos);
                }
            }
        }
        assert_eq!(0, copy_inserted.len());

        // Persist and validate lookup_tag for each inserted tag
        db.commit(&mut writer).unwrap();
        let reader = db.reader().unwrap();
        for (tag, pos) in &appended {
            let positions =
                tags_tree_lookup(&db, reader.tags_tree_root_id, *tag, Position(1), false).unwrap();
            assert_eq!(positions, vec![*pos]);
        }
        // Also verify 'from' filtering using a representative tag
        let (tag_last, pos_last) = appended.last().cloned().unwrap();
        verify_tag_lookup_range(&db, tag_last, &vec![pos_last], false);
        verify_tag_lookup_range(&db, tag_last, &vec![pos_last], true);
    }

    #[test]
    fn split_tags_tree_internal_many_writers() {
        // Setup a temporary database
        let (_temp_dir, db) = construct_db(512);

        let mut has_split_internal = false;
        let mut appended: Vec<(TagHash, Position)> = Vec::new();

        // Insert tag-position pairs until we split a root internal node
        let mut n: u64 = 0;
        while !has_split_internal {
            // Start a writer
            let mut writer = db.writer().unwrap();

            // Issue a new position and create a deterministic increasing tag
            let position = writer.issue_position();
            let tag = th(n);
            n += 1;
            appended.push((tag, position));

            // Insert the pair into the tags tree
            tags_tree_insert(&db, &mut writer, tag, position).unwrap();

            // Check if the root is an internal node and its first child is also internal
            let root_page = writer.dirty.get(&writer.tags_tree_root_id).unwrap();
            match &root_page.node {
                Node::TagsInternal(root_node) => {
                    if !root_node.child_ids.is_empty() {
                        let child_id = root_node.child_ids[0];
                        if let Some(child_page) = writer.dirty.get(&child_id) {
                            match &child_page.node {
                                Node::TagsInternal(_) => {
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
        // Sort expected values by TagHash lexicographic order (array cmp)
        copy_inserted.sort_by(|a, b| a.0.cmp(&b.0));

        // Start a writer to access the latest root id
        let writer = db.writer().unwrap();

        // Get the root node (persisted)
        let root_page = db.read_page(writer.tags_tree_root_id).unwrap();
        let root_node = match &root_page.node {
            Node::TagsInternal(node) => node,
            _ => panic!("Expected TagsInternal node"),
        };

        // Check each child of the root
        for &child_id in root_node.child_ids.iter() {
            let child_page = db.read_page(child_id).unwrap();
            assert_eq!(child_id, child_page.page_id);

            let child_node = match &child_page.node {
                Node::TagsInternal(node) => node,
                _ => panic!("Expected TagsInternal node"),
            };

            for &grand_child_id in child_node.child_ids.iter() {
                let grand_child_page = db.read_page(grand_child_id).unwrap();
                assert_eq!(grand_child_id, grand_child_page.page_id);

                let grand_child_node = match &grand_child_page.node {
                    Node::TagsLeaf(node) => node,
                    _ => panic!("Expected TagsLeaf node"),
                };

                // Check each key and value in the leaf
                for (k, &key) in grand_child_node.keys.iter().enumerate() {
                    let val = &grand_child_node.values[k];
                    let (appended_tag, appended_pos) = copy_inserted.remove(0);
                    assert_eq!(appended_tag, key);
                    assert_eq!(val.positions.len(), 1);
                    assert_eq!(val.positions[0], appended_pos);
                }
            }
        }
        assert_eq!(0, copy_inserted.len());

        // Validate lookup_tag for each inserted tag
        let reader = db.reader().unwrap();
        for (tag, pos) in &appended {
            let positions =
                tags_tree_lookup(&db, reader.tags_tree_root_id, *tag, Position(1), false).unwrap();
            assert_eq!(positions, vec![*pos]);
        }
        // Also verify 'from' filtering using a representative tag
        let (tag_last, pos_last) = appended.last().cloned().unwrap();
        verify_tag_lookup_range(&db, tag_last, &vec![pos_last], false);
        verify_tag_lookup_range(&db, tag_last, &vec![pos_last], true);
    }

    #[test]
    fn overflow_to_per_tag_leaf_one_writer() {
        // Use small page size to force overflow with inline positions
        let (_tmp, db) = construct_db(256);
        let mut writer = db.writer().unwrap();
        let tag = th(777);
        let mut inserted: Vec<Position> = Vec::new();
        // 30 positions will exceed: header(9) + node(20 + 8P) > 256 when P >= 29
        for _ in 0..30 {
            let p = writer.issue_position();
            tags_tree_insert(&db, &mut writer, tag, p).unwrap();
            inserted.push(p);
        }
        // Root should still be a TagsLeaf containing the tag key
        let root_page = writer.dirty.get(&writer.tags_tree_root_id).unwrap();
        match &root_page.node {
            Node::TagsLeaf(leaf) => {
                let idx = leaf.keys.binary_search(&tag).unwrap();
                let val = &leaf.values[idx];
                assert_ne!(
                    val.root_id,
                    PageID(0),
                    "Expected per-tag root_id to be non-zero after overflow migration"
                );
                assert!(
                    val.positions.is_empty(),
                    "Inline positions should have been migrated to per-tag page"
                );
                // The per-tag page should exist and be a TagLeaf containing all inserted positions
                let tag_page = writer
                    .dirty
                    .get(&val.root_id)
                    .expect("Per-tag page should be dirty");
                match &tag_page.node {
                    Node::TagLeaf(tleaf) => {
                        assert_eq!(tleaf.positions, inserted);
                    }
                    other => panic!("Expected TagLeaf node, got {:?}", other),
                }
            }
            other => panic!("Expected TagsLeaf root, got {:?}", other),
        }

        // Persist and validate lookup_tag for each inserted tag
        db.commit(&mut writer).unwrap();
        let reader = db.reader().unwrap();
        let positions =
            tags_tree_lookup(&db, reader.tags_tree_root_id, tag, Position(1), false).unwrap();
        assert_eq!(inserted, positions);
        // Verify range of 'from' values produce the expected suffix of positions
        verify_tag_lookup_range(&db, tag, &inserted, false);
        verify_tag_lookup_range(&db, tag, &inserted, true);
    }

    #[test]
    fn overflow_to_per_tag_leaf_many_writers() {
        // Use small page size to force overflow with inline positions
        let (_tmp, db) = construct_db(256);
        let tag = th(777);
        let mut inserted: Vec<Position> = Vec::new();
        // 30 positions will exceed: header(9) + node(20 + 8P) > 256 when P >= 29
        for _ in 0..30 {
            let mut writer = db.writer().unwrap();
            let p = writer.issue_position();
            tags_tree_insert(&db, &mut writer, tag, p).unwrap();
            db.commit(&mut writer).unwrap();
            inserted.push(p);
        }
        // Root should still be a TagsLeaf containing the tag key
        let writer = db.writer().unwrap();
        let root_page = db.read_page(writer.tags_tree_root_id).unwrap();
        match &root_page.node {
            Node::TagsLeaf(leaf) => {
                let idx = leaf.keys.binary_search(&tag).unwrap();
                let val = &leaf.values[idx];
                assert_ne!(
                    val.root_id,
                    PageID(0),
                    "Expected per-tag root_id to be non-zero after overflow migration"
                );
                assert!(
                    val.positions.is_empty(),
                    "Inline positions should have been migrated to per-tag page"
                );
                // The per-tag page should exist and be a TagLeaf containing all inserted positions
                let tag_page = db
                    .read_page(val.root_id)
                    .expect("Per-tag page should exist");
                match &tag_page.node {
                    Node::TagLeaf(tleaf) => {
                        assert_eq!(tleaf.positions, inserted);
                    }
                    other => panic!("Expected TagLeaf node, got {:?}", other),
                }
            }
            other => panic!("Expected TagsLeaf root, got {:?}", other),
        }

        // Persist and validate lookup_tag for each inserted tag
        let reader = db.reader().unwrap();
        let positions =
            tags_tree_lookup(&db, reader.tags_tree_root_id, tag, Position(1), false).unwrap();
        assert_eq!(inserted, positions);
        // Verify range of 'from' values produce the expected suffix of positions
        verify_tag_lookup_range(&db, tag, &inserted, false);
        verify_tag_lookup_range(&db, tag, &inserted, true);
    }

    #[test]
    fn split_per_tag_leaf_one_writer() {
        // Use small page size to force per-tag TagLeaf split after migration
        let (_tmp, db) = construct_db(256);
        let mut writer = db.writer().unwrap();
        let tag = th(888);
        let mut inserted: Vec<Position> = Vec::new();
        // First, insert enough to migrate inline -> per-tag TagLeaf (30 triggers migration at page size 256)
        for _ in 0..30 {
            let p = writer.issue_position();
            tags_tree_insert(&db, &mut writer, tag, p).unwrap();
            inserted.push(p);
        }
        // Next insert should cause per-tag TagLeaf overflow and split to TagInternal
        let last = writer.issue_position();
        tags_tree_insert(&db, &mut writer, tag, last).unwrap();
        inserted.push(last);

        // Inspect structure in-memory
        let root_page = writer.dirty.get(&writer.tags_tree_root_id).unwrap();
        match &root_page.node {
            Node::TagsLeaf(leaf) => {
                let idx = leaf.keys.binary_search(&tag).unwrap();
                let val = &leaf.values[idx];
                assert_ne!(
                    val.root_id,
                    PageID(0),
                    "Expected per-tag root_id to be non-zero after split"
                );
                // Should now point to a TagInternal node
                let int_page = writer
                    .dirty
                    .get(&val.root_id)
                    .expect("Per-tag internal page should be dirty");
                match &int_page.node {
                    Node::TagInternal(internal) => {
                        assert_eq!(internal.keys.len(), 1);
                        assert_eq!(internal.child_ids.len(), 2);
                        assert_eq!(
                            internal.keys[0], last,
                            "Promoted key should be the last position moved to right leaf"
                        );
                        let left_page = writer.dirty.get(&internal.child_ids[0]).unwrap();
                        let right_page = writer.dirty.get(&internal.child_ids[1]).unwrap();
                        match (&left_page.node, &right_page.node) {
                            (Node::TagLeaf(left_leaf), Node::TagLeaf(right_leaf)) => {
                                assert_eq!(
                                    left_leaf.positions,
                                    inserted[..inserted.len() - 1].to_vec()
                                );
                                assert_eq!(right_leaf.positions, vec![last]);
                            }
                            other => panic!("Expected left/right TagLeaf nodes, got {:?}", other),
                        }
                    }
                    other => {
                        panic!("Expected TagInternal node, got {:?}", other)
                    }
                }
            }
            other => panic!("Expected TagsLeaf root, got {:?}", other),
        }

        // Persist and validate lookup_tag for each inserted tag
        db.commit(&mut writer).unwrap();
        let reader = db.reader().unwrap();
        let positions =
            tags_tree_lookup(&db, reader.tags_tree_root_id, tag, Position(1), false).unwrap();
        assert_eq!(inserted, positions);
        // Verify range of 'from' values produce the expected suffix of positions
        verify_tag_lookup_range(&db, tag, &inserted, false);
        verify_tag_lookup_range(&db, tag, &inserted, true);
    }

    #[test]
    fn split_per_tag_leaf_many_writers() {
        // Use small page size to force per-tag TagLeaf split after migration
        let (_tmp, db) = construct_db(256);
        let tag = th(888);
        let mut inserted: Vec<Position> = Vec::new();
        // First, insert enough to migrate inline -> per-tag TagLeaf (30 triggers migration at page size 256)
        for _ in 0..30 {
            let mut writer = db.writer().unwrap();
            let p = writer.issue_position();
            tags_tree_insert(&db, &mut writer, tag, p).unwrap();
            db.commit(&mut writer).unwrap();
            inserted.push(p);
        }
        // Next insert should cause per-tag TagLeaf overflow and split to TagInternal
        let mut writer = db.writer().unwrap();
        let last = writer.issue_position();
        tags_tree_insert(&db, &mut writer, tag, last).unwrap();
        inserted.push(last);
        db.commit(&mut writer).unwrap();

        // Inspect structure from file.
        let writer = db.writer().unwrap();
        let root_page = db.read_page(writer.tags_tree_root_id).unwrap();
        match &root_page.node {
            Node::TagsLeaf(leaf) => {
                let idx = leaf.keys.binary_search(&tag).unwrap();
                let val = &leaf.values[idx];
                assert_ne!(
                    val.root_id,
                    PageID(0),
                    "Expected per-tag root_id to be non-zero after split"
                );
                // Should now point to a TagInternal node
                let int_page = db
                    .read_page(val.root_id)
                    .expect("Per-tag internal page should be dirty");
                match &int_page.node {
                    Node::TagInternal(internal) => {
                        assert_eq!(internal.keys.len(), 1);
                        assert_eq!(internal.child_ids.len(), 2);
                        assert_eq!(
                            internal.keys[0], last,
                            "Promoted key should be the last position moved to right leaf"
                        );
                        let left_page = db.read_page(internal.child_ids[0]).unwrap();
                        let right_page = db.read_page(internal.child_ids[1]).unwrap();
                        match (&left_page.node, &right_page.node) {
                            (Node::TagLeaf(left_leaf), Node::TagLeaf(right_leaf)) => {
                                assert_eq!(
                                    left_leaf.positions,
                                    inserted[..inserted.len() - 1].to_vec()
                                );
                                assert_eq!(right_leaf.positions, vec![last]);
                            }
                            other => panic!("Expected left/right TagLeaf nodes, got {:?}", other),
                        }
                    }
                    other => {
                        panic!("Expected TagInternal node, got {:?}", other)
                    }
                }
            }
            other => panic!("Expected TagsLeaf root, got {:?}", other),
        }

        let reader = db.reader().unwrap();
        let positions =
            tags_tree_lookup(&db, reader.tags_tree_root_id, tag, Position(1), false).unwrap();
        assert_eq!(inserted, positions);
        // Verify range of 'from' values produce the expected suffix of positions
        verify_tag_lookup_range(&db, tag, &inserted, false);
        verify_tag_lookup_range(&db, tag, &inserted, true);
    }

    #[test]
    fn split_per_tag_internal_one_writer() {
        // Use small page size to force multiple splits within the per-tag subtree
        let (_tmp, db) = construct_db(256);
        let mut writer = db.writer().unwrap();
        let tag = th(999);

        let mut inserted: Vec<Position> = Vec::new();

        // First, drive migration (inline -> TagLeaf) and the first per-tag leaf split to create a TagInternal root
        for _ in 0..31 {
            // 30 to migrate, +1 to split TagLeaf
            let p = writer.issue_position();
            tags_tree_insert(&db, &mut writer, tag, p).unwrap();
            inserted.push(p);
        }

        // Now keep inserting until the per-tag TagInternal root itself splits (i.e., its child is TagInternal)
        let mut safety = 5000; // ample room
        loop {
            // Inspect per-tag root
            let root_page = writer.dirty.get(&writer.tags_tree_root_id).unwrap();
            let per_tag_root_id = match &root_page.node {
                Node::TagsLeaf(leaf) => {
                    let idx = leaf.keys.binary_search(&tag).unwrap();
                    leaf.values[idx].root_id
                }
                other => panic!("Expected TagsLeaf root, got {:?}", other),
            };
            assert_ne!(
                per_tag_root_id,
                PageID(0),
                "Expected per-tag root to be initialized"
            );
            let per_tag_root = writer.dirty.get(&per_tag_root_id).unwrap();
            match &per_tag_root.node {
                Node::TagInternal(root_internal) => {
                    if !root_internal.child_ids.is_empty() {
                        let first_child_id = root_internal.child_ids[0];
                        if let Some(child_page) = writer.dirty.get(&first_child_id) {
                            if matches!(child_page.node, Node::TagInternal(_)) {
                                // Success: per-tag internal has split creating a second level
                                break;
                            }
                        }
                    }
                }
                Node::TagLeaf(_) => {
                    // Not yet split to internal; continue inserting
                }
                other => panic!("Expected per-tag TagInternal/TagLeaf, got {:?}", other),
            }

            // Insert another position and continue
            let p = writer.issue_position();
            tags_tree_insert(&db, &mut writer, tag, p).unwrap();
            inserted.push(p);

            safety -= 1;
            if safety == 0 {
                panic!("Exceeded safety limit without causing per-tag internal split");
            }
        }

        db.commit(&mut writer).unwrap();
        let reader = db.reader().unwrap();
        let positions =
            tags_tree_lookup(&db, reader.tags_tree_root_id, tag, Position(1), false).unwrap();
        assert_eq!(inserted, positions);
        // Verify range of 'from' values produce the expected suffix of positions
        verify_tag_lookup_range(&db, tag, &inserted, false);
        verify_tag_lookup_range(&db, tag, &inserted, true);
    }

    #[test]
    fn split_per_tag_internal_many_writers() {
        // Use small page size to force multiple splits within the per-tag subtree
        let (_tmp, db) = construct_db(256);
        let tag = th(999);

        let mut inserted: Vec<Position> = Vec::new();

        // First, drive migration (inline -> TagLeaf) and the first per-tag leaf split to create a TagInternal root
        for _ in 0..31 {
            // 30 to migrate, +1 to split TagLeaf
            let mut writer = db.writer().unwrap();
            let p = writer.issue_position();
            tags_tree_insert(&db, &mut writer, tag, p).unwrap();
            db.commit(&mut writer).unwrap();
            inserted.push(p);
        }

        // Now keep inserting until the per-tag TagInternal root itself splits (i.e., its child is TagInternal)
        let mut safety = 5000; // ample room
        loop {
            // Inspect per-tag root
            let mut writer = db.writer().unwrap();
            let root_page = db.read_page(writer.tags_tree_root_id).unwrap();
            let per_tag_root_id = match &root_page.node {
                Node::TagsLeaf(leaf) => {
                    let idx = leaf.keys.binary_search(&tag).unwrap();
                    leaf.values[idx].root_id
                }
                other => panic!("Expected TagsLeaf root, got {:?}", other),
            };
            assert_ne!(
                per_tag_root_id,
                PageID(0),
                "Expected per-tag root to be initialized"
            );
            let per_tag_root = db.read_page(per_tag_root_id).unwrap();
            match &per_tag_root.node {
                Node::TagInternal(root_internal) => {
                    if !root_internal.child_ids.is_empty() {
                        let first_child_id = root_internal.child_ids[0];
                        let child_page = db.read_page(first_child_id).unwrap();
                        if matches!(child_page.node, Node::TagInternal(_)) {
                            // Success: per-tag internal has split creating a second level
                            break;
                        }
                    }
                }
                Node::TagLeaf(_) => {
                    // Not yet split to internal; continue inserting
                }
                other => panic!("Expected per-tag TagInternal/TagLeaf, got {:?}", other),
            }

            // Insert another position and continue
            let p = writer.issue_position();
            tags_tree_insert(&db, &mut writer, tag, p).unwrap();
            inserted.push(p);
            db.commit(&mut writer).unwrap();

            safety -= 1;
            if safety == 0 {
                panic!("Exceeded safety limit without causing per-tag internal split");
            }
        }

        let reader = db.reader().unwrap();
        let positions =
            tags_tree_lookup(&db, reader.tags_tree_root_id, tag, Position(1), false).unwrap();
        assert_eq!(inserted, positions);
        // Verify range of 'from' values produce the expected suffix of positions
        verify_tag_lookup_range(&db, tag, &inserted, false);
        verify_tag_lookup_range(&db, tag, &inserted, true);
    }

    #[test]
    fn tags_tree_iter_collects_inserted_positions() {
        let (_tmp, db) = construct_db(1024);
        let mut writer = db.writer().unwrap();
        let tag = th(55);
        let mut inserted: Vec<Position> = Vec::new();
        for _ in 0..5 {
            let p = writer.issue_position();
            tags_tree_insert(&db, &mut writer, tag, p).unwrap();
            inserted.push(p);
        }
        db.commit(&mut writer).unwrap();

        let reader = db.reader().unwrap();
        let dirty = HashMap::new();

        // from first -> all positions
        let after_first = inserted[0];
        let collected_all: Vec<Position> = TagsTreeIterator::new(
            &db,
            &dirty,
            reader.tags_tree_root_id,
            tag,
            Some(after_first),
            false,
        )
        .collect();
        assert_eq!(collected_all, inserted);

        // from second -> drop first
        let from_first = inserted[1];
        let collected_fron_first: Vec<Position> = TagsTreeIterator::new(
            &db,
            &dirty,
            reader.tags_tree_root_id,
            tag,
            Some(from_first),
            false,
        )
        .collect();
        assert_eq!(collected_fron_first, inserted[1..].to_vec());

        // from middle -> drop up to that element
        let from_mid = inserted[3];
        let collected_from_mid: Vec<Position> = TagsTreeIterator::new(
            &db,
            &dirty,
            reader.tags_tree_root_id,
            tag,
            Some(from_mid),
            false,
        )
        .collect();
        assert_eq!(collected_from_mid, inserted[3..].to_vec());

        // from last -> empty
        let from_last = *inserted.last().unwrap();
        let collected_from_last: Vec<Position> = TagsTreeIterator::new(
            &db,
            &dirty,
            reader.tags_tree_root_id,
            tag,
            Some(from_last),
            false,
        )
        .collect();
        assert_eq!(collected_from_last, inserted[inserted.len() - 1..].to_vec());

        // from last + 1 -> empty
        let after_last = Position(from_last.0 + 1);
        let collected_empty: Vec<Position> = TagsTreeIterator::new(
            &db,
            &dirty,
            reader.tags_tree_root_id,
            tag,
            Some(after_last),
            false,
        )
        .collect();
        assert!(collected_empty.is_empty());

        // non-existent tag yields empty iterator regardless of after
        let empty_iter = TagsTreeIterator::new(
            &db,
            &dirty,
            reader.tags_tree_root_id,
            th(9999),
            Some(Position(0)),
            false,
        );
        assert_eq!(empty_iter.collect::<Vec<Position>>(), Vec::new());
    }

    // #[test]
    // fn benchmark_insert_and_lookup_varied_sizes_one_tag_one_position() {
    //     // Benchmark-like test; prints durations for different sizes. Run with:
    //     // cargo test --lib mvcc_tags_tree::tests::benchmark_insert_and_lookup_varied_sizes -- --nocapture
    //     let sizes: [usize; 8] = [1, 1, 10, 100, 1_000, 5_000, 10_000, 50_000];
    //     for &size in &sizes {
    //         let (_tmp, db) = construct_db(4096);
    //
    //         // Insert phase
    //         let mut writer = db.writer().unwrap();
    //         let start_insert = Instant::now();
    //         for n in 0..(size as u64) {
    //             let pos = writer.issue_position();
    //             let tag = th(n);
    //             hint::black_box(tag);
    //             hint::black_box(pos);
    //             tags_tree_insert(&db, &mut writer, tag, pos).unwrap();
    //         }
    //         let insert_elapsed = start_insert.elapsed();
    //         let start_commit = Instant::now();
    //         db.commit(&mut writer).unwrap();
    //         let commit_elapsed = start_commit.elapsed();
    //
    //         // Lookup phase
    //         let reader = db.reader().unwrap();
    //         let start_lookup = Instant::now();
    //         for n in 0..(size as u64) {
    //             let res = tags_tree_lookup(&db, reader.tags_tree_root_id, th(n), Position(1), false).unwrap();
    //             hint::black_box(&res);
    //         }
    //         let lookup_elapsed = start_lookup.elapsed();
    //
    //         let insert_avg_us = (insert_elapsed.as_secs_f64() * 1_000_000.0) / (size as f64);
    //         let commit_avg_us = commit_elapsed.as_secs_f64() * 1_000_000.0;
    //         let lookup_avg_us = (lookup_elapsed.as_secs_f64() * 1_000_000.0) / (size as f64);
    //
    //         println!(
    //             "mvcc_tags_tree benchmark one tag one position: size={size}, insert_us_per_call={insert_avg_us:.3}, commit_us={commit_avg_us:.3}, lookup_us_per_call={lookup_avg_us:.3}"
    //         );
    //     }
    // }
    //
    // #[test]
    // fn benchmark_insert_and_lookup_varied_one_tag_many_positions() {
    //     // Benchmark-like test; prints durations for different sizes. Run with:
    //     // cargo test --lib mvcc_tags_tree::tests::benchmark_insert_and_lookup_varied_sizes -- --nocapture
    //     let sizes = [1, 1, 10, 100, 1_000, 5_000, 10_000, 50_000];
    //     for &size in &sizes {
    //         let (_tmp, db) = construct_db(4096);
    //
    //         // Insert phase
    //         let mut writer = db.writer().unwrap();
    //         let tag = th(size.try_into().unwrap());
    //         hint::black_box(tag);
    //         let start_insert = Instant::now();
    //         for _ in 0..(size as u64) {
    //             let pos = writer.issue_position();
    //             hint::black_box(pos);
    //             tags_tree_insert(&db, &mut writer, tag, pos).unwrap();
    //         }
    //         let insert_elapsed = start_insert.elapsed();
    //         let start_commit = Instant::now();
    //         db.commit(&mut writer).unwrap();
    //         let commit_elapsed = start_commit.elapsed();
    //
    //         // Lookup phase
    //         let reader = db.reader().unwrap();
    //         let start_lookup = Instant::now();
    //         let res = tags_tree_lookup(&db, reader.tags_tree_root_id, tag, Position(1), false).unwrap();
    //         hint::black_box(&res);
    //         let lookup_elapsed = start_lookup.elapsed();
    //
    //         let insert_avg_us = (insert_elapsed.as_secs_f64() * 1_000_000.0) / (size as f64);
    //         let commit_avg_us = commit_elapsed.as_secs_f64() * 1_000_000.0;
    //         let lookup_avg_us = (lookup_elapsed.as_secs_f64() * 1_000_000.0) / (size as f64);
    //
    //         println!(
    //             "mvcc_tags_tree one tag many positions benchmark: size={size}, insert_us_per_call={insert_avg_us:.3}, commit_us={commit_avg_us:.3}, lookup_us_per_call={lookup_avg_us:.3}"
    //         );
    //     }
    // }
}
