use std::path::Path;

use crate::common::{PageID, Position};
use crate::events_tree::{EventIterator, event_tree_append, event_tree_lookup};
use crate::events_tree_nodes::EventRecord;
use crate::mvcc::{Mvcc, Writer};
use crate::node::Node;
use crate::page::Page;
use crate::tags_tree::{TagsTreeIterator, tags_tree_insert};
use crate::tags_tree_nodes::{TagHash, get_tag_key_width};
use crate::tracking_tree_nodes::{TrackingInternalNode, TrackingLeafNode};
use itertools::Itertools;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use umadb_dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBEventStoreSync, DCBQuery, DCBReadResponseSync,
    DCBResult, DCBSequencedEvent, TrackingInfo,
};
use uuid::Uuid;

pub static DEFAULT_PAGE_SIZE: usize = 4096;
pub const DEFAULT_DB_FILENAME: &str = "uma.db";

/// Database on-disk schema version for HeaderNode and related structures.
/// Set to 1 for current releases; previous versions of the code used 0.
pub const DB_SCHEMA_VERSION: u32 = 1;

/// EventStore implementing the DCBEventStoreSync interface
pub struct UmaDB {
    pub mvcc: Arc<Mvcc>,
}

impl UmaDB {
    /// Create a new EventStore at the given directory or file path.
    /// If a directory path is provided, a file named "uma.db" will be used inside it.
    pub fn new<P: AsRef<Path>>(path: P) -> DCBResult<Self> {
        let p = path.as_ref();
        let file_path = if p.is_dir() {
            p.join(DEFAULT_DB_FILENAME)
        } else {
            p.to_path_buf()
        };
        let mvcc = Mvcc::new(&file_path, DEFAULT_PAGE_SIZE, false)?;
        Ok(Self {
            mvcc: Arc::new(mvcc),
        })
    }

    pub fn from_arc(mvcc: Arc<Mvcc>) -> Self {
        Self { mvcc }
    }

    /// Returns the greatest recorded upstream position for a source, if any.
    pub fn get_tracking_info(&self, source: &str) -> DCBResult<Option<u64>> {
        let reader = self.mvcc.reader()?;
        let mut pid = reader.tracking_tree_root_id;
        if pid == PageID(0) {
            return Ok(None);
        }
        loop {
            let page = self.mvcc.read_page(pid)?;
            match page.node {
                Node::TrackingLeaf(node) => return Ok(node.get(source).map(|p| p.0)),
                Node::TrackingInternal(internal) => {
                    let idx = match internal.keys.binary_search_by(|k| k.as_str().cmp(source)) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    if idx >= internal.child_ids.len() {
                        return Err(DCBError::DatabaseCorrupted(
                            "tracking internal child index out of bounds".to_string(),
                        ));
                    }
                    pid = internal.child_ids[idx];
                }
                other => {
                    return Err(DCBError::DatabaseCorrupted(format!(
                        "Invalid tracking node type: {}",
                        other.type_name()
                    )))
                }
            }
        }
    }

    /// Appends a batch of (events, condition) using a single writer/transaction.
    /// For each item, behaves like append():
    /// - If condition is Some and matches any events (considering uncommitted writes), returns Err(IntegrityError) for that item and continues.
    /// - If events is empty, returns Ok(0) for that item and continues.
    /// - Otherwise performs unconditional append and records Ok(last_position) for that item.
    ///
    /// At the end, commits the writer once. If commit fails, returns the commit error and discards per-item results.
    pub fn append_batch(
        &self,
        items: Vec<(
            Vec<DCBEvent>,
            Option<DCBAppendCondition>,
            Option<TrackingInfo>,
        )>,
    ) -> DCBResult<Vec<DCBResult<u64>>> {
        // println!("Processing batch of {} items", items.len());

        let mvcc = &self.mvcc;
        let mut writer = mvcc.writer()?;
        let mut results: Vec<DCBResult<u64>> = Vec::with_capacity(items.len());

        for (events, condition, tracking) in items.into_iter() {
            let result =
                Self::process_append_request(events, condition, tracking, mvcc, &mut writer);
            results.push(result);
        }

        // Single commit at the end of the batch
        mvcc.commit(&mut writer)?;
        Ok(results)
    }

    pub fn process_append_request(
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
        tracking_info: Option<TrackingInfo>,
        mvcc: &Arc<Mvcc>,
        writer: &mut Writer,
    ) -> DCBResult<u64> {
        // Check condition using read_conditional (limit 1), starting after the provided position
        if let Some(cond) = condition {
            let from = cond.after.map(|after| Position(after + 1));
            let read_result1 = read_conditional(
                mvcc,
                &writer.dirty,
                writer.events_tree_root_id,
                writer.tags_tree_root_id,
                cond.fail_if_events_match.clone(),
                from,
                false,
                Some(1),
                false,
            );
            match read_result1 {
                Ok(found_vec) => {
                    // Read didn't error...
                    if let Some(matched) = found_vec.first() {
                        // Found one event... consider if the request is idempotent...
                        return match is_request_idempotent(
                            mvcc,
                            &writer.dirty,
                            writer.events_tree_root_id,
                            writer.tags_tree_root_id,
                            &events,
                            cond.fail_if_events_match.clone(),
                            from,
                        ) {
                            Ok(Some(last_recorded_position)) => Ok(last_recorded_position),
                            Ok(None) => {
                                // Propagate an integrity error for this item but continue with others
                                let msg = format!(
                                    "condition: {:?} matched: {:?}, ",
                                    cond.clone(),
                                    matched,
                                );
                                Err(DCBError::IntegrityError(msg))
                            }
                            Err(err) => {
                                // Propagate the error for this item but continue with others
                                Err(err)
                            }
                        };
                    }
                }
                Err(e) => {
                    // Propagate the read error for this item but continue with others
                    return Err(e);
                }
            }
        }

        // If tracking is provided for this item, enforce monotonicity and update tracking leaf under same writer
        if let Some(tracking_info) = tracking_info {
            if let Err(e) = tracking_upsert(
                mvcc,
                writer,
                &tracking_info.source,
                Position(tracking_info.position),
            ) {
                return Err(e);
            }
        }

        // Append unconditionally
        if events.is_empty() {
            return Ok(0);
        }
        match unconditional_append(mvcc, writer, events) {
            Ok(last) => Ok(last),
            Err(e) => Err(e),
        }
    }
}

impl DCBEventStoreSync for UmaDB {
    fn read(
        &self,
        query: Option<DCBQuery>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
        _subscribe: bool,
    ) -> DCBResult<Box<dyn DCBReadResponseSync + Send + 'static>> {
        let mvcc = &self.mvcc;
        let reader = mvcc.reader()?;

        // Compute last committed position for unlimited head
        let last_committed_position = reader.next_position.0.saturating_sub(1);

        // Build query and after
        let q = query.unwrap_or(DCBQuery { items: vec![] });
        let from = start.map(Position);

        // Delegate to read_conditional
        let events = read_conditional(
            mvcc,
            &HashMap::new(),
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            q,
            from,
            backwards,
            limit,
            false,
        )?;

        // Compute head according to semantics
        let head = if limit.is_none() {
            if last_committed_position == 0 {
                None
            } else {
                Some(last_committed_position)
            }
        } else {
            events.last().map(|e| e.position)
        };

        Ok(Box::new(ReadResponse {
            events: VecDeque::from(events),
            head,
        }))
    }

    fn head(&self) -> DCBResult<Option<u64>> {
        let db = &self.mvcc;
        let (_, header) = db.get_latest_header()?;
        let last = header.next_position.0.saturating_sub(1);
        if last == 0 { Ok(None) } else { Ok(Some(last)) }
    }

    fn get_tracking_info(&self, source: &str) -> DCBResult<Option<u64>> {
        UmaDB::get_tracking_info(self, source)
    }

    /// Append events with optional tracking enforcement and update.
    /// If tracking is provided, ensures the supplied position is strictly greater than
    /// any recorded for the given source, and atomically updates the tracking leaf.
    fn append(
        &self,
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
        tracking_info: Option<TrackingInfo>,
    ) -> DCBResult<u64> {
        if events.is_empty() {
            return Ok(0);
        }
        let mvcc = &self.mvcc;
        let mut writer = mvcc.writer()?;
        let result =
            Self::process_append_request(events, condition, tracking_info, mvcc, &mut writer);
        mvcc.commit(&mut writer)?;
        result
    }
}

struct ReadResponse {
    events: VecDeque<DCBSequencedEvent>,
    head: Option<u64>,
}

/// Ensure tracking constraint and update/insert the position into leaf without splitting.
fn tracking_upsert(mvcc: &Mvcc, writer: &mut Writer, source: &str, pos: Position) -> DCBResult<()> {
    // Enforce maximum key length (1-byte length field in tracking nodes)
    let key_len = source.as_bytes().len();
    if key_len > u8::MAX as usize {
        return Err(DCBError::InvalidArgument(
            format!("tracking source too long ({} > 255)", key_len),
        ));
    }

    let root = writer.tracking_tree_root_id;

    // Empty tree: create a new leaf as root
    if root == PageID(0) {
        let mut node = TrackingLeafNode::new();
        // First insert; no need to pre-check capacity as we will allocate a page and verify
        node.keys.push(source.to_string());
        node.values.push(pos);
        let new_root_id = writer.alloc_page_id();
        let page = Page::new(new_root_id, Node::TrackingLeaf(node));
        writer.insert_dirty(page)?;
        writer.tracking_tree_root_id = new_root_id;
        return Ok(());
    }

    // Descend the tree to find the target leaf
    let mut stack: Vec<(PageID, usize)> = Vec::new();
    let mut current_id = root;
    loop {
        let page = writer.get_page_ref(mvcc, current_id)?;
        match &page.node {
            Node::TrackingLeaf(_) => break,
            Node::TrackingInternal(internal) => {
                let child_idx = internal.child_index_for_key(source);
                if child_idx >= internal.child_ids.len() {
                    return Err(DCBError::DatabaseCorrupted(
                        "tracking internal child index out of bounds".to_string(),
                    ));
                }
                let next = internal.child_ids[child_idx];
                stack.push((current_id, child_idx));
                current_id = next;
            }
            other => {
                return Err(DCBError::DatabaseCorrupted(format!(
                    "Invalid tracking node type: {}",
                    other.type_name()
                )));
            }
        }
    }

    // At leaf: check monotonicity first on an immutable snapshot
    {
        let page = writer.get_page_ref(mvcc, current_id)?;
        let Node::TrackingLeaf(leaf) = &page.node else {
            return Err(DCBError::DatabaseCorrupted(
                "Expected TrackingLeaf".to_string(),
            ));
        };
        if let Some(existing) = leaf.get(source) {
            if pos.0 <= existing.0 {
                return Err(DCBError::IntegrityError(format!(
                    "non-increasing tracking position for source '{source}': {} <= {}",
                    pos.0, existing.0
                )));
            }
        }
    }

    // COW the leaf
    let dirty_leaf_id = writer.get_dirty_page_id(current_id)?;
    let mut replacement_info: Option<(PageID, PageID)> =
        (dirty_leaf_id != current_id).then_some((current_id, dirty_leaf_id));

    // We may need to propagate a split upward
    let mut split_info: Option<(String, PageID)> = None;

    // Insert/update in the leaf
    {
        let leaf_page = writer.get_mut_dirty(dirty_leaf_id)?;
        // First, insert/update within a limited scope to end the mutable borrow before size checks
        {
            let Node::TrackingLeaf(ref mut node) = leaf_page.node else {
                return Err(DCBError::DatabaseCorrupted(
                    "Dirty tracking page not a leaf".to_string(),
                ));
            };
            match node.keys.binary_search_by(|k| k.as_str().cmp(source)) {
                Ok(i) => node.values[i] = pos,
                Err(ins) => {
                    node.keys.insert(ins, source.to_string());
                    node.values.insert(ins, pos);
                }
            }
        }
        // Now check overflow and perform split if needed in a new scope
        if leaf_page.calc_serialized_size() > mvcc.page_size {
            let promoted_key: String;
            let right_id: PageID;
            {
                let Node::TrackingLeaf(ref mut node) = leaf_page.node else {
                    return Err(DCBError::DatabaseCorrupted(
                        "Dirty tracking page not a leaf".to_string(),
                    ));
                };
                if node.keys.len() < 2 {
                    return Err(DCBError::DatabaseCorrupted(
                        "Cannot split tracking leaf with too few keys".to_string(),
                    ));
                }
                let mid = node.keys.len() / 2;
                let right_keys = node.keys.split_off(mid);
                let right_vals = node.values.split_off(mid);
                promoted_key = right_keys
                    .first()
                    .ok_or_else(|| DCBError::DatabaseCorrupted("empty right split".to_string()))?
                    .clone();
                let right_leaf = TrackingLeafNode {
                    keys: right_keys,
                    values: right_vals,
                };
                right_id = writer.alloc_page_id();
                let right_page = Page::new(right_id, Node::TrackingLeaf(right_leaf));
                writer.insert_dirty(right_page)?;
            }
            split_info = Some((promoted_key, right_id));
        }
    }

    // Walk up the stack to apply replacements and propagate splits
    while let Some((parent_id, child_idx)) = stack.pop() {
        // COW parent if needed
        let dirty_parent_id = writer.get_dirty_page_id(parent_id)?;
        let parent_replacement_info =
            (dirty_parent_id != parent_id).then_some((parent_id, dirty_parent_id));

        // Apply child replacement if needed
        if let Some((old_id, new_id)) = replacement_info.take() {
            let parent_page = writer.get_mut_dirty(dirty_parent_id)?;
            let Node::TrackingInternal(ref mut internal) = parent_page.node else {
                return Err(DCBError::DatabaseCorrupted(
                    "Expected TrackingInternal".to_string(),
                ));
            };
            internal.replace_child_id_at(child_idx, old_id, new_id)?;
        }

        // Apply split promotion if any
        if let Some((prom_key, new_child_id)) = split_info.take() {
            let need_split: bool;
            {
                let parent_page = writer.get_mut_dirty(dirty_parent_id)?;
                let Node::TrackingInternal(ref mut internal) = parent_page.node else {
                    return Err(DCBError::DatabaseCorrupted(
                        "Expected TrackingInternal".to_string(),
                    ));
                };
                internal.insert_promoted_at(child_idx, prom_key, new_child_id);
                need_split = parent_page.calc_serialized_size() > mvcc.page_size;
            }
            if need_split {
                // Reborrow mutably to perform the split
                let parent_page = writer.get_mut_dirty(dirty_parent_id)?;
                let Node::TrackingInternal(ref mut internal) = parent_page.node else {
                    return Err(DCBError::DatabaseCorrupted(
                        "Expected TrackingInternal".to_string(),
                    ));
                };
                let (promote_up, right_keys, right_child_ids) = internal.split_off()?;
                let right_internal = TrackingInternalNode {
                    keys: right_keys,
                    child_ids: right_child_ids,
                };
                let right_internal_id = writer.alloc_page_id();
                let right_internal_page =
                    Page::new(right_internal_id, Node::TrackingInternal(right_internal));
                writer.insert_dirty(right_internal_page)?;
                split_info = Some((promote_up, right_internal_id));
            }
        }

        // Propagate parent replacement upwards if any
        replacement_info = parent_replacement_info;
    }

    // Apply root replacement if necessary
    if let Some((old_id, new_id)) = replacement_info.take() {
        if writer.tracking_tree_root_id == old_id {
            writer.tracking_tree_root_id = new_id;
        } else {
            return Err(DCBError::RootIDMismatch(old_id.0, new_id.0));
        }
    }

    // If we still have a pending promotion, create a new internal root
    if let Some((prom_key, right_id)) = split_info.take() {
        let new_root_id = writer.alloc_page_id();
        let left_id = writer.tracking_tree_root_id;
        let new_root = TrackingInternalNode {
            keys: vec![prom_key],
            child_ids: vec![left_id, right_id],
        };
        let new_root_page = Page::new(new_root_id, Node::TrackingInternal(new_root));
        writer.insert_dirty(new_root_page)?;
        writer.tracking_tree_root_id = new_root_id;
    }

    Ok(())
}

impl Iterator for ReadResponse {
    type Item = DCBResult<DCBSequencedEvent>;
    fn next(&mut self) -> Option<Self::Item> {
        self.events.pop_front().map(Ok)
    }
}

impl DCBReadResponseSync for ReadResponse {
    fn head(&mut self) -> DCBResult<Option<u64>> {
        Ok(self.head)
    }
    fn collect_with_head(&mut self) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        let events = self.events.drain(..).collect();
        Ok((events, self.head))
    }
    fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>> {
        let batch = self.events.drain(..).collect();
        Ok(batch)
    }
}

/// Append events unconditionally to the database.
///
/// For each event, this will:
/// - issue a position from the writer
/// - append an EventRecord to the event tree
/// - insert the position for each tag into the tags tree
///
/// Caller is responsible for committing the writer.
pub fn unconditional_append(
    mvcc: &Mvcc,
    writer: &mut Writer,
    events: Vec<DCBEvent>,
) -> DCBResult<u64> {
    // Note: when used with tracking, the caller must perform tracking checks and updates
    // before this call within the same writer to ensure atomicity.
    let mut last_pos_u64: u64 = 0;

    for ev in events.into_iter() {
        let position = writer.issue_position();
        last_pos_u64 = position.0;
        // Index tags before moving an event record into event_tree_append
        for tag in ev.tags.iter() {
            let tag_hash: TagHash = tag_to_hash(tag);
            tags_tree_insert(mvcc, writer, tag_hash, position)?;
        }
        let record = EventRecord {
            event_type: ev.event_type,
            data: ev.data,
            tags: ev.tags,
            uuid: ev.uuid,
        };
        event_tree_append(mvcc, writer, record, position)?;
    }

    Ok(last_pos_u64)
}

/// Read events using the tags index by merging per-tag iterators, grouping by position,
/// filtering by tag and type matches, and then looking up the event record.
pub fn read_conditional(
    mvcc: &Mvcc,
    dirty: &HashMap<PageID, Page>,
    events_tree_root_id: PageID,
    tags_tree_root_id: PageID,
    query: DCBQuery,
    start: Option<Position>,
    backwards: bool,
    limit: Option<u32>,
    force_sequential_read: bool,
) -> DCBResult<Vec<DCBSequencedEvent>> {
    const SCAN_BATCH_SIZE: u32 = 256;
    // Special case: explicit zero limit
    if let Some(0) = limit {
        return Ok(Vec::new());
    }

    // If no items, return all events with after/limit respected via sequential scan
    if query.items.is_empty() {
        let mut iter = EventIterator::new(mvcc, dirty, events_tree_root_id, start, backwards);
        let mut out: Vec<DCBSequencedEvent> = Vec::new();
        'outer_all: loop {
            let batch = iter.next_batch(limit.unwrap_or(SCAN_BATCH_SIZE))?;
            if batch.is_empty() {
                break;
            }
            for (pos, rec) in batch.into_iter() {
                out.push(DCBSequencedEvent {
                    position: pos.0,
                    event: DCBEvent {
                        event_type: rec.event_type,
                        data: rec.data,
                        tags: rec.tags,
                        uuid: rec.uuid,
                    },
                });
                if let Some(lim) = limit
                    && out.len() >= lim as usize
                {
                    break 'outer_all;
                }
            }
        }
        return Ok(out);
    }

    // All query items must have at least one tag to use the tag index path.
    let all_items_have_tags = query.items.iter().all(|it| !it.tags.is_empty());
    if !all_items_have_tags || force_sequential_read {
        // Fallback: sequentially scan all events and apply the same matching logic
        let mut iter = EventIterator::new(mvcc, dirty, events_tree_root_id, start, backwards);
        let mut out: Vec<DCBSequencedEvent> = Vec::new();
        let matches_item = |rec: &EventRecord| -> bool {
            for item in &query.items {
                let type_ok =
                    item.types.is_empty() || item.types.iter().any(|t| t == &rec.event_type);
                if !type_ok {
                    continue;
                }
                let tags_ok = item.tags.iter().all(|t| rec.tags.iter().any(|et| et == t));
                if type_ok && tags_ok {
                    return true;
                }
            }
            false
        };
        'outer_fallback: loop {
            let batch = iter.next_batch(SCAN_BATCH_SIZE)?;
            if batch.is_empty() {
                break;
            }
            for (pos, rec) in batch.into_iter() {
                if matches_item(&rec) {
                    out.push(DCBSequencedEvent {
                        position: pos.0,
                        event: DCBEvent {
                            event_type: rec.event_type,
                            data: rec.data,
                            tags: rec.tags,
                            uuid: rec.uuid,
                        },
                    });
                    if let Some(lim) = limit
                        && out.len() >= lim as usize
                    {
                        break 'outer_fallback;
                    }
                }
            }
        }
        return Ok(out);
    }

    // Invert query: tag -> list of query item indices that require this tag
    let mut tag_qiis: HashMap<String, Vec<usize>> = HashMap::with_capacity(query.items.len() * 2);
    let mut qi_tags: Vec<HashSet<String>> = Vec::with_capacity(query.items.len());

    for (qiid, item) in query.items.iter().enumerate() {
        qi_tags.push(item.tags.iter().cloned().collect());
        for tag in &item.tags {
            tag_qiis.entry(tag.clone()).or_default().push(qiid);
        }
    }

    // Prepare per-tag iterators yielding (position, tag, qiids)
    struct PositionTagQiidIterator<I>
    where
        I: Iterator<Item = Position>,
    {
        inner: I,
        tag: String,
        qiids: Vec<usize>,
    }
    impl<I> PositionTagQiidIterator<I>
    where
        I: Iterator<Item = Position>,
    {
        fn new(inner: I, tag: String, qiids: Vec<usize>) -> Self {
            Self { inner, tag, qiids }
        }
    }
    impl<I> Iterator for PositionTagQiidIterator<I>
    where
        I: Iterator<Item = Position>,
    {
        type Item = (Position, String, Vec<usize>);
        fn next(&mut self) -> Option<Self::Item> {
            self.inner
                .next()
                .map(|p| (p, self.tag.clone(), self.qiids.clone()))
        }
    }

    let mut tag_iters: Vec<PositionTagQiidIterator<_>> = Vec::new();
    for (tag, qiids) in tag_qiis.iter() {
        let tag_hash: TagHash = tag_to_hash(tag);
        let positions_iter =
            TagsTreeIterator::new(mvcc, dirty, tags_tree_root_id, tag_hash, start, backwards); // yields positions for tag
        tag_iters.push(PositionTagQiidIterator::new(
            positions_iter,
            tag.clone(),
            qiids.clone(),
        ));
    }

    // Merge iterators ordered by position
    let merged = tag_iters
        .into_iter()
        .kmerge_by(|a, b| if !backwards { a.0 < b.0 } else { a.0 > b.0 });

    // Group by position, collecting tags and qiids
    struct GroupByPositionIterator<I>
    where
        I: Iterator<Item = (Position, String, Vec<usize>)>,
    {
        inner: I,
        current_pos: Option<Position>,
        tags: HashSet<String>,
        qiis: HashSet<usize>,
        finished: bool,
    }
    impl<I> GroupByPositionIterator<I>
    where
        I: Iterator<Item = (Position, String, Vec<usize>)>,
    {
        fn new(inner: I) -> Self {
            Self {
                inner,
                current_pos: None,
                tags: HashSet::new(),
                qiis: HashSet::new(),
                finished: false,
            }
        }
    }
    impl<I> Iterator for GroupByPositionIterator<I>
    where
        I: Iterator<Item = (Position, String, Vec<usize>)>,
    {
        type Item = (Position, HashSet<String>, HashSet<usize>);
        fn next(&mut self) -> Option<Self::Item> {
            if self.finished {
                return None;
            }
            for (pos, tag, qiids) in self.inner.by_ref() {
                if self.current_pos.is_none() {
                    self.current_pos = Some(pos);
                } else if self.current_pos.unwrap() != pos {
                    let out_pos = self.current_pos.unwrap();
                    let out_tags = std::mem::take(&mut self.tags);
                    let out_qiis = std::mem::take(&mut self.qiis);
                    self.current_pos = Some(pos);
                    self.tags.insert(tag);
                    for q in qiids {
                        self.qiis.insert(q);
                    }
                    return Some((out_pos, out_tags, out_qiis));
                }
                self.tags.insert(tag);
                for q in qiids {
                    self.qiis.insert(q);
                }
            }
            if let Some(p) = self.current_pos.take() {
                self.finished = true;
                let out_tags = std::mem::take(&mut self.tags);
                let out_qiis = std::mem::take(&mut self.qiis);
                return Some((p, out_tags, out_qiis));
            }
            None
        }
    }

    let mut out: Vec<DCBSequencedEvent> = Vec::new();
    for (pos, tags_present, qiis_present) in GroupByPositionIterator::new(merged) {
        // Find any query item whose required tag set is subset of tags_present
        let matching_qiis: Vec<usize> = qiis_present
            .iter()
            .copied()
            .filter(|&qii| qi_tags[qii].is_subset(&tags_present))
            .collect();
        if matching_qiis.is_empty() {
            continue;
        }

        // Lookup the event record at position
        let rec = event_tree_lookup(mvcc, dirty, events_tree_root_id, pos)?;

        // Check type and actual tag matching against any of the matching items to avoid hash-collision false positives
        let mut match_ok = false;
        'matchcheck: for qii in matching_qiis.iter().copied() {
            let item = &query.items[qii];
            // Type must match (or be unspecified)
            let type_ok = item.types.is_empty() || item.types.iter().any(|t| t == &rec.event_type);
            if !type_ok {
                continue;
            }
            // Verify actual event tags contain all item tags (guards against tag-hash collisions)
            let tags_ok = item.tags.iter().all(|t| rec.tags.iter().any(|et| et == t));
            if tags_ok {
                match_ok = true;
                break 'matchcheck;
            }
        }
        if !match_ok {
            continue;
        }

        out.push(DCBSequencedEvent {
            position: pos.0,
            event: DCBEvent {
                event_type: rec.event_type,
                data: rec.data,
                tags: rec.tags,
                uuid: rec.uuid,
            },
        });
        if let Some(lim) = limit
            && out.len() >= lim as usize
        {
            break;
        }
    }

    Ok(out)
}
/// Compute a TagHash ([u8; 16]) from a tag string using a stable UUID v5 hash.
#[inline(always)]
pub fn tag_to_hash_v5uuid(tag: &str) -> TagHash {
    // Use a fixed namespace (URL) so the same tag always maps to the same UUID.
    // UUID v5 is name-based and stable across runs.
    let u = Uuid::new_v5(&Uuid::NAMESPACE_URL, tag.as_bytes());
    u.into_bytes()
}

#[inline(always)]
pub fn tag_to_hash_crc64(tag: &str) -> TagHash {
    // This is the legacy "schema version 0" tag hasher, which
    // creates 64-bit hashes. This causes too many conflicts
    // when there are many millions of events in the database.
    // And so it was replaced with version 5 UUIDs of the tag.
    const SALT: [u8; 4] = [0x9E, 0x37, 0x79, 0xB9];
    // Build a 64-bit value by combining two crc32 hashes for stability and simplicity.
    let mut hasher1 = crc32fast::Hasher::new();
    hasher1.update(tag.as_bytes());
    let a = hasher1.finalize();

    let mut hasher2 = crc32fast::Hasher::new();
    // Note: Benchmark (benches/tag_hash_bench.rs) shows two update() calls
    // are consistently faster than concatenating bytes+salt into a buffer
    // and calling update() once, because concatenation requires allocation
    // and copying. Keeping the two calls avoids extra work and is at least
    // as fast across sizes from 0..8192 bytes.
    hasher2.update(tag.as_bytes());
    hasher2.update(&SALT);
    let b = hasher2.finalize();

    let value = ((a as u64) << 32) | (b as u64);
    let mut out: TagHash = [0u8; crate::tags_tree_nodes::TAG_HASH_LEN];
    out[..8].copy_from_slice(&value.to_le_bytes());
    // The remaining 8 bytes are zeros as required by the new 128-bit TagHash format.
    out
}

#[inline]
pub fn tag_to_hash(tag: &str) -> TagHash {
    if get_tag_key_width() == 16 {
        tag_to_hash_v5uuid(tag)
    } else {
        tag_to_hash_crc64(tag)
    }
}

pub fn is_request_idempotent(
    mvcc: &Arc<Mvcc>,
    dirty: &HashMap<PageID, Page>,
    events_tree_root_id: PageID,
    tags_tree_root_id: PageID,
    events: &Vec<DCBEvent>,
    fail_if_events_match: DCBQuery,
    start: Option<Position>,
) -> DCBResult<Option<u64>> {
    // Check events for event IDs. If all have events IDs then
    // call read_conditional again with limit=event.len() and then
    // see if all events have matching UUIDs.
    let submitted_events_len = events.len();
    let mut submitted_event_ids: Vec<Option<Uuid>> = vec![];
    for submitted_event in events {
        if submitted_event.uuid.is_some() {
            submitted_event_ids.push(submitted_event.uuid);
        }
    }
    if submitted_events_len == submitted_event_ids.len()
        && submitted_events_len as u64 <= u32::MAX as u64
    {
        // All events have UUIDs and there are less than the max size of limit.
        let read_result = read_conditional(
            mvcc,
            dirty,
            events_tree_root_id,
            tags_tree_root_id,
            fail_if_events_match,
            start,
            false,
            Some(submitted_events_len as u32),
            false,
        );
        match read_result {
            Ok(found_events) => {
                let mut found_event_ids: Vec<Option<Uuid>> = vec![];
                let found_events_len = found_events.len();
                if found_events_len == submitted_events_len {
                    let last_found_event = &found_events[found_events_len - 1];
                    let last_found_event_position = last_found_event.position;
                    for found_event in found_events {
                        found_event_ids.push(found_event.event.uuid);
                    }
                    if found_event_ids == submitted_event_ids {
                        // It's an idempotent request.
                        return Ok(Some(last_found_event_position));
                        // results.push(Ok(last_found_event_position));
                        // return true
                    }
                }
            }
            Err(e) => {
                // Propagate read error for this item but continue with others
                return Err(e);
                // results.push(Err(e));
                // return true;
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::Page;
    use serial_test::serial;
    use std::collections::HashMap;
    use tempfile::tempdir;
    use umadb_dcb::{
        DCBAppendCondition, DCBError, DCBEvent, DCBEventStoreSync, DCBQuery, DCBQueryItem,
    };
    use uuid::Uuid;

    #[test]
    #[serial]
    fn tracking_get_none_on_new_db() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("tracking-none.db");
        let uma = UmaDB::new(db_path).unwrap();
        let pos = uma.get_tracking_info("source-A").unwrap();
        assert!(pos.is_none());
    }

    #[test]
    #[serial]
    fn tracking_source_length_too_long_errors() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("tracking-longkey.db");
        let uma = UmaDB::new(db_path).unwrap();
        // Make a 256-byte ASCII string
        let long_key = "a".repeat(256);
        let ev = DCBEvent::new().event_type("T").data([1u8]);
        let err = uma
            .append(
                vec![ev],
                None,
                Some(TrackingInfo { source: long_key, position: 1 }),
            )
            .unwrap_err();
        match err {
            DCBError::InvalidArgument(msg) => assert!(msg.contains("too long")),
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    #[serial]
    fn tracking_leaf_split_creates_internal_root_and_lookups_work() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("tracking-split.db");
        // Use a small page size to trigger splits with few inserts
        let mvcc = Mvcc::new(&db_path, 256, false).unwrap();
        let uma = UmaDB::from_arc(Arc::new(mvcc));

        let base_event = DCBEvent::new().event_type("T").data([0u8]);
        // Insert many different sources to force at least one leaf split
        for i in 0..50u32 {
            let key = format!("k{:03}", i);
            let ev = base_event.clone();
            uma
                .append(
                    vec![ev],
                    None,
                    Some(TrackingInfo { source: key.clone(), position: (i + 1) as u64 }),
                )
                .unwrap();
        }
        // Verify some lookups
        assert_eq!(Some(1), uma.get_tracking_info("k000").unwrap());
        assert_eq!(Some(25), uma.get_tracking_info("k024").unwrap());
        assert_eq!(Some(50), uma.get_tracking_info("k049").unwrap());
        assert_eq!(None, uma.get_tracking_info("k999").unwrap());
    }

    #[test]
    #[serial]
    fn tracking_internal_node_splits_under_load() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("tracking-internal-split.db");
        // Small page size to force both leaf and internal splits quickly
        let mvcc = Mvcc::new(&db_path, 128, false).unwrap();
        let uma = UmaDB::from_arc(Arc::new(mvcc));

        let base_event = DCBEvent::new().event_type("T").data([0u8]);

        // Keep track of every key we send and the expected value (position)
        let mut observed: HashMap<String, u64> = HashMap::new();

        // Insert keys until we observe that the root's first child is also an internal node,
        // which only happens after the root internal itself has split and a new root was created.
        let mut detected_internal_split = false;
        for i in 0..200u32 {
            let key = format!("s{:03}xxxx", i); // 8-byte keys keep node capacities small
            println!("Key: {key}");
            let ev = base_event.clone();
            let pos = (i + 1) as u64;
            uma
                .append(
                    vec![ev],
                    None,
                    Some(TrackingInfo { source: key.clone(), position: pos }),
                )
                .unwrap();
            observed.insert(key, pos);

            if i % 5 == 4 {
                let reader = uma.mvcc.reader().unwrap();
                let root_id = reader.tracking_tree_root_id;
                if root_id != PageID(0) {
                    let root = uma.mvcc.read_page(root_id).unwrap();
                    if let Node::TrackingInternal(root_internal) = &root.node {
                        let first_child_id = root_internal.child_ids[0];
                        let first_child = uma.mvcc.read_page(first_child_id).unwrap();
                        if matches!(first_child.node, Node::TrackingInternal(_)) {
                            detected_internal_split = true;
                            break;
                        }
                    }
                }
            }
        }

        assert!(
            detected_internal_split,
            "Exceeded safety limit without causing tracking internal split"
        );

        // Verify that every key we inserted can be looked up and has the expected value
        for (k, expected_pos) in &observed {
            let got = uma.get_tracking_info(&k).unwrap();
            assert_eq!(Some(*expected_pos), got, "tracking info mismatch for key {k}");
        }

        // Now, for each source, increment the position by 1000 and verify updates are visible
        let mut updated: HashMap<String, u64> = HashMap::new();
        for (k, prev_pos) in &observed {
            let new_pos = *prev_pos + 1000;
            uma
                .append(
                    vec![base_event.clone()],
                    None,
                    Some(TrackingInfo { source: k.clone(), position: new_pos }),
                )
                .unwrap();
            updated.insert(k.clone(), new_pos);
        }
        for (k, expected_pos) in updated {
            let got = uma.get_tracking_info(&k).unwrap();
            assert_eq!(Some(expected_pos), got, "after update: tracking info mismatch for key {k}");
        }
    }

    #[test]
    #[serial]
    fn append_with_tracking_create_and_update_and_monotonic_enforced() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("tracking-append.db");
        let uma = UmaDB::new(db_path).unwrap();

        // Prepare a simple event
        let ev = DCBEvent::new()
            .event_type("T1")
            .data(vec![1, 2, 3])
            .tags(["x", "y"]);

        // First append with tracking position 5 should create tracking leaf
        let last = uma
            .append(
                vec![ev.clone()],
                None,
                Some(TrackingInfo {
                    source: "src1".into(),
                    position: 5,
                }),
            )
            .unwrap();
        assert_eq!(1, last);
        assert_eq!(Some(5), uma.get_tracking_info("src1").unwrap());

        // Non-increasing should fail
        let err = uma
            .append(
                vec![ev.clone()],
                None,
                Some(TrackingInfo {
                    source: "src1".into(),
                    position: 5,
                }),
            )
            .err()
            .expect("expected error");
        match err {
            DCBError::IntegrityError(msg) => {
                assert!(msg.contains("non-increasing tracking position"))
            }
            other => panic!("unexpected error: {:?}", other),
        }

        // Increasing should succeed and update recorded position
        let last2 = uma
            .append(
                vec![ev],
                None,
                Some(TrackingInfo {
                    source: "src1".into(),
                    position: 6,
                }),
            )
            .unwrap();
        assert_eq!(2, last2);
        assert_eq!(Some(6), uma.get_tracking_info("src1").unwrap());
    }

    // Backward-compatible wrapper for tests: call new read_conditional with an empty dirty map
    fn read_conditional(
        mvcc: &Mvcc,
        events_tree_root_id: PageID,
        tags_tree_root_id: PageID,
        query: DCBQuery,
        start: Option<Position>,
        backwards: bool,
        limit: Option<u32>,
    ) -> DCBResult<Vec<DCBSequencedEvent>> {
        super::read_conditional(
            mvcc,
            &HashMap::<PageID, Page>::new(),
            events_tree_root_id,
            tags_tree_root_id,
            query,
            start,
            backwards,
            limit,
            false,
        )
    }

    static VERBOSE: bool = false;

    // Helper to produce a deterministic set of 10 events with shared tags and unique types
    fn standard_events() -> Vec<DCBEvent> {
        let shared_tags = vec![
            "alpha".to_string(),
            "beta".to_string(),
            "gamma".to_string(),
            "delta".to_string(),
            "epsilon".to_string(),
        ];
        let mut input: Vec<DCBEvent> = Vec::new();
        for i in 0..10u8 {
            let t1 = shared_tags[(i % 5) as usize].clone();
            let t2 = shared_tags[((i + 2) % 5) as usize].clone();
            input.push(DCBEvent {
                event_type: format!("Type{}", i),
                data: vec![i, i + 1, i + 2],
                tags: vec![t1, t2],
                uuid: None,
            });
        }
        input
    }

    // Create DB with the standard events; keep temp dir alive by returning it
    fn setup_db_with_standard_events() -> (tempfile::TempDir, Mvcc, Vec<DCBEvent>) {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("mvcc-api-test.db");
        let db = Mvcc::new(db_path.as_ref(), DEFAULT_PAGE_SIZE, VERBOSE).unwrap();
        let input = standard_events();
        let mut writer = db.writer().unwrap();
        let last = unconditional_append(&db, &mut writer, input.clone()).unwrap();
        db.commit(&mut writer).unwrap();
        // Verify last equals committed head
        let (_, header) = db.get_latest_header().unwrap();
        let head = header.next_position.0.saturating_sub(1);
        assert_eq!(last, head);
        (temp_dir, db, input)
    }

    #[test]
    #[serial]
    fn empty_query_after_and_limit() {
        let (_tmp, mut mvcc, input) = setup_db_with_standard_events();

        // after = 0 -> all
        let reader = mvcc.reader().unwrap();

        let all = read_conditional(
            &mut mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            Some(Position(1)),
            false,
            None,
        )
        .unwrap();
        assert_eq!(all.len(), input.len());
        assert!(all.windows(2).all(|w| w[0].position < w[1].position));

        // after = first -> tail
        let first = all[0].position;
        let tail = read_conditional(
            &mut mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            Some(Position(first + 1)),
            false,
            None,
        )
        .unwrap();
        assert_eq!(tail.len(), input.len() - 1);

        // after = last -> empty
        let last = all.last().unwrap().position;
        let none = read_conditional(
            &mut mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            Some(Position(last + 1)),
            false,
            None,
        )
        .unwrap();
        assert!(none.is_empty());

        // limits
        let lim0 = read_conditional(
            &mut mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            Some(Position(1)),
            false,
            Some(0),
        )
        .unwrap();
        assert!(lim0.is_empty());
        let lim3 = read_conditional(
            &mut mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            Some(Position(1)),
            false,
            Some(3),
        )
        .unwrap();
        assert_eq!(lim3.len(), 3);
        let lim20 = read_conditional(
            &mut mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            Some(Position(1)),
            false,
            Some(20),
        )
        .unwrap();
        assert_eq!(lim20.len(), input.len());
    }

    #[test]
    #[serial]
    fn tags_only_single_tag_after_and_limit() {
        let (_tmp, mut db, _input) = setup_db_with_standard_events();
        let qi = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec!["alpha".to_string()],
            }],
        };
        let reader = db.reader().unwrap();
        let res = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Some(Position(1)),
            false,
            None,
        )
        .unwrap();
        assert_eq!(res.len(), 4);
        assert!(
            res.iter()
                .all(|e| e.event.tags.iter().any(|t| t == "alpha"))
        );
        assert!(res.windows(2).all(|w| w[0].position < w[1].position));

        // after combinations
        let positions: Vec<u64> = res.iter().map(|e| e.position).collect();
        let after_first = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Some(Position(positions[0] + 1)),
            false,
            None,
        )
        .unwrap();
        assert_eq!(after_first.len(), positions.len() - 1);
        let after_last = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Some(Position(*positions.last().unwrap() + 1)),
            false,
            None,
        )
        .unwrap();
        assert!(after_last.is_empty());

        // limits
        let lim0 = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Some(Position(1)),
            false,
            Some(0),
        )
        .unwrap();
        assert!(lim0.is_empty());
        let lim1 = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Some(Position(1)),
            false,
            Some(1),
        )
        .unwrap();
        assert_eq!(lim1.len(), 1);
        let lim10 = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi,
            Some(Position(1)),
            false,
            Some(10),
        )
        .unwrap();
        assert_eq!(lim10.len(), 4);
    }

    #[test]
    #[serial]
    fn tags_only_multi_tag_and() {
        let (_tmp, mut db, _input) = setup_db_with_standard_events();
        let qi = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec!["alpha".to_string(), "gamma".to_string()],
            }],
        };
        let reader = db.reader().unwrap();
        let res = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi,
            Some(Position(1)),
            false,
            None,
        )
        .unwrap();
        assert_eq!(res.len(), 2);
        assert!(
            res.iter()
                .all(|e| e.event.tags.iter().any(|t| t == "alpha"))
        );
        assert!(
            res.iter()
                .all(|e| e.event.tags.iter().any(|t| t == "gamma"))
        );
    }

    #[test]
    #[serial]
    fn types_plus_tags_index_path() {
        let (_tmp, mut db, _input) = setup_db_with_standard_events();
        let qi = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec!["Type0".to_string()],
                tags: vec!["alpha".to_string()],
            }],
        };
        let reader = db.reader().unwrap();
        let res = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi,
            Some(Position(1)),
            false,
            None,
        )
        .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].event.event_type, "Type0");
        assert!(res[0].event.tags.iter().any(|t| t == "alpha"));
    }

    #[test]
    #[serial]
    fn or_semantics_and_deduplication() {
        let (_tmp, mut db, _input) = setup_db_with_standard_events();
        let alpha_only = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec!["alpha".to_string()],
            }],
        };
        let reader = db.reader().unwrap();
        let alpha_positions: Vec<u64> = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            alpha_only.clone(),
            Some(Position(1)),
            false,
            None,
        )
        .unwrap()
        .into_iter()
        .map(|e| e.position)
        .collect();

        // Overlapping items: alpha OR (alpha AND gamma) should deduplicate
        let query = DCBQuery {
            items: vec![
                DCBQueryItem {
                    types: vec![],
                    tags: vec!["alpha".to_string()],
                },
                DCBQueryItem {
                    types: vec![],
                    tags: vec!["alpha".to_string(), "gamma".to_string()],
                },
            ],
        };
        let res = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            query,
            Some(Position(1)),
            false,
            None,
        )
        .unwrap();
        let res_positions: Vec<u64> = res.into_iter().map(|e| e.position).collect();
        assert_eq!(res_positions, alpha_positions);
    }

    #[test]
    #[serial]
    fn fallback_types_only_after_and_limit() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("mvcc-fallback-types-only.db");
        let mut db = Mvcc::new(db_path.as_ref(), DEFAULT_PAGE_SIZE, VERBOSE).unwrap();

        // Use a smaller custom set to make counts obvious
        let events = vec![
            DCBEvent {
                event_type: "TypeA".to_string(),
                data: vec![1],
                tags: vec!["x".to_string()],
                uuid: None,
            },
            DCBEvent {
                event_type: "TypeB".to_string(),
                data: vec![2],
                tags: vec!["y".to_string()],
                uuid: None,
            },
            DCBEvent {
                event_type: "TypeA".to_string(),
                data: vec![3],
                tags: vec!["z".to_string()],
                uuid: None,
            },
        ];
        let mut writer = db.writer().unwrap();
        let last = unconditional_append(&db, &mut writer, events).unwrap();
        db.commit(&mut writer).unwrap();
        let (_, header) = db.get_latest_header().unwrap();
        let head = header.next_position.0.saturating_sub(1);
        assert_eq!(last, head);

        // Query item with no tags => forces fallback path; select TypeA only
        let qi = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec!["TypeA".to_string()],
                tags: vec![],
            }],
        };
        let reader = db.reader().unwrap();
        let res = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Some(Position(1)),
            false,
            None,
        )
        .unwrap();
        assert_eq!(res.len(), 2);
        assert!(res.iter().all(|e| e.event.event_type == "TypeA"));

        // After skip first matching
        let first_pos = res[0].position;
        let res_after = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Some(Position(first_pos + 1)),
            false,
            None,
        )
        .unwrap();
        assert_eq!(res_after.len(), 1);

        // Limit 1
        let res_lim1 = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi,
            Some(Position(1)),
            false,
            Some(1),
        )
        .unwrap();
        assert_eq!(res_lim1.len(), 1);
    }

    #[test]
    #[serial]
    fn fallback_empty_item_matches_all() {
        let (_tmp, mut db, input) = setup_db_with_standard_events();
        // An empty item (no types, no tags) should match all events via fallback path
        let qi = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec![],
            }],
        };

        let reader = db.reader().unwrap();
        let all = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Some(Position(1)),
            false,
            None,
        )
        .unwrap();
        assert_eq!(all.len(), input.len());

        // After and limit still apply
        let first = all[1].position;
        let tail = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Some(Position(first)),
            false,
            None,
        )
        .unwrap();
        assert_eq!(tail.len(), input.len() - 1);
        let lim5 = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi,
            Some(Position(1)),
            false,
            Some(5),
        )
        .unwrap();
        assert_eq!(lim5.len(), 5);
    }

    #[test]
    #[serial]
    fn test_event_store() {
        let temp_dir = tempdir().unwrap();
        let store = UmaDB::new(temp_dir.path()).unwrap();

        // Head is None on empty store
        assert_eq!(None, store.head().unwrap());

        // Append a couple of events
        let events = vec![
            DCBEvent {
                event_type: "TypeA".to_string(),
                data: vec![1],
                tags: vec!["foo".to_string()],
                uuid: None,
            },
            DCBEvent {
                event_type: "TypeB".to_string(),
                data: vec![2],
                tags: vec!["bar".to_string(), "foo".to_string()],
                uuid: None,
            },
        ];
        let last = store.append(events.clone(), None, None).unwrap();
        assert!(last > 0);
        assert_eq!(store.head().unwrap(), Some(last));

        // Read all
        let mut resp = store.read(None, None, false, None, false).unwrap();
        let (all, head) = resp.collect_with_head().unwrap();
        assert_eq!(head, Some(last));
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].event.event_type, "TypeA");
        assert_eq!(all[1].event.event_type, "TypeB");

        // Limit semantics: only first event returned and head equals that position
        let mut resp_lim1 = store.read(None, None, false, Some(1), false).unwrap();
        let (only_one, head_lim1) = resp_lim1.collect_with_head().unwrap();
        assert_eq!(only_one.len(), 1);
        assert_eq!(only_one[0].event.event_type, "TypeA");
        assert_eq!(head_lim1, Some(only_one[0].position));

        // Tag-filtered read ("foo")
        let query = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec!["foo".to_string()],
            }],
        };
        let mut resp2 = store.read(Some(query), None, false, None, false).unwrap();
        let out2 = resp2.next_batch().unwrap();
        assert_eq!(out2.len(), 2);
        assert!(out2.iter().all(|e| e.event.tags.iter().any(|t| t == "foo")));

        // From semantics: skip the first event
        let first_pos = all[0].position + 1;
        let mut resp3 = store
            .read(None, Some(first_pos), false, None, false)
            .unwrap();
        let out3 = resp3.next_batch().unwrap();
        assert_eq!(out3.len(), 1);
        assert_eq!(out3[0].event.event_type, "TypeB");

        // Append with a condition that should PASS: query matches existing 'foo' but after = last
        let cond_pass = DCBAppendCondition {
            fail_if_events_match: DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec!["foo".to_string()],
                }],
            },
            after: Some(last),
        };
        let ok_last = store
            .append(
                vec![DCBEvent {
                    event_type: "TypeC".to_string(),
                    data: vec![3],
                    tags: vec!["baz".to_string()],
                    uuid: None,
                }],
                Some(cond_pass),
                None,
            )
            .expect("append with passing condition should succeed");
        assert!(ok_last > last);
        assert_eq!(store.head().unwrap(), Some(ok_last));

        // Append with a condition that should FAIL: same query but after = 0
        let cond_fail = DCBAppendCondition {
            fail_if_events_match: DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec!["foo".to_string()],
                }],
            },
            after: Some(0),
        };
        let before_head = store.head().unwrap();
        let res = store.append(
            vec![DCBEvent {
                event_type: "TypeD".to_string(),
                data: vec![4],
                tags: vec!["qux".to_string()],
                uuid: None,
            }],
            Some(cond_fail),
            None,
        );
        match res {
            Err(DCBError::IntegrityError(_)) => {}
            other => panic!("Expected IntegrityError, got {:?}", other),
        }
        // Ensure head unchanged after failed append
        assert_eq!(store.head().unwrap(), before_head);
    }

    #[test]
    fn test_append_batch_mixed_conditions() {
        let temp_dir = tempdir().unwrap();
        let store = UmaDB::new(temp_dir.path()).unwrap();

        let e1 = DCBEvent {
            event_type: "A".into(),
            data: b"1".to_vec(),
            tags: vec!["t1".into()],
            uuid: None,
        };
        let e2 = DCBEvent {
            event_type: "B".into(),
            data: b"2".to_vec(),
            tags: vec!["t2".into()],
            uuid: None,
        };
        let e3 = DCBEvent {
            event_type: "C".into(),
            data: b"3".to_vec(),
            tags: vec!["t3".into()],
            uuid: None,
        };

        // Batch: first succeeds, second fails due to condition matching any event, third succeeds (after high position)
        let items = vec![
            (vec![e1.clone()], None, None),
            (
                vec![e2.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: DCBQuery::default(),
                    after: None,
                }),
                None,
            ),
            (
                vec![e3.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: DCBQuery::default(),
                    after: Some(10),
                }),
                None,
            ),
        ];

        let results = store.append_batch(items).unwrap();

        assert_eq!(results.len(), 3);
        // First item should succeed with last position 1
        match &results[0] {
            Ok(pos) => assert_eq!(*pos, 1),
            Err(e) => panic!("unexpected error for first item: {:?}", e),
        }
        // Second item should fail integrity
        match &results[1] {
            Ok(pos) => panic!("expected integrity error, got Ok({})", pos),
            Err(e) => assert!(matches!(e, DCBError::IntegrityError(_))),
        }
        // Third item should succeed with last position 2 (since second didn't append)
        match &results[2] {
            Ok(pos) => assert_eq!(*pos, 2),
            Err(e) => panic!("unexpected error for third item: {:?}", e),
        }

        // Verify committed state: only e1 and e3 should be present, head is 2
        let (events, head) = store.read_with_head(None, None, false, None).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event.data, e1.data);
        assert_eq!(events[1].event.data, e3.data);
        assert_eq!(head, Some(2));
    }

    #[test]
    fn test_append_batch_dirty_visibility_with_tags() {
        let temp_dir = tempdir().unwrap();
        let store = UmaDB::new(temp_dir.path()).unwrap();

        // Item1 introduces tag "x"; Item2 condition queries tag "x" and must see it via dirty tags tree; Item3 uses after to ignore it
        let e1 = DCBEvent {
            event_type: "T".into(),
            data: b"one".to_vec(),
            tags: vec!["x".into()],
            uuid: None,
        };
        let e2 = DCBEvent {
            event_type: "T".into(),
            data: b"two".to_vec(),
            tags: vec!["y".into()],
            uuid: None,
        };
        let e3 = DCBEvent {
            event_type: "T".into(),
            data: b"three".to_vec(),
            tags: vec!["z".into()],
            uuid: None,
        };

        let query_tag_x = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec!["x".into()],
            }],
        };

        let items = vec![
            // 1) Append e1 (tag x)
            (vec![e1.clone()], None, None),
            // 2) Attempt append e2, but fail if any events with tag x exist after None (i.e., from the start); should fail due to e1 in dirty pages
            (
                vec![e2.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: query_tag_x.clone(),
                    after: None,
                }),
                None,
            ),
            // 3) Append e3 with condition that ignores position 1 by using after=Some(1); should pass
            (
                vec![e3.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: query_tag_x.clone(),
                    after: Some(1),
                }),
                None,
            ),
        ];

        let results = store.append_batch(items).unwrap();

        assert_eq!(results.len(), 3);
        match &results[0] {
            Ok(pos) => assert_eq!(*pos, 1),
            Err(e) => panic!("unexpected error for first item: {:?}", e),
        }
        match &results[1] {
            Ok(pos) => panic!("expected integrity error, got Ok({})", pos),
            Err(e) => assert!(matches!(e, DCBError::IntegrityError(_))),
        }
        match &results[2] {
            Ok(pos) => assert_eq!(*pos, 2),
            Err(e) => panic!("unexpected error for third item: {:?}", e),
        }

        // Verify committed state and tag index behavior
        let (events, head) = store.read_with_head(None, None, false, None).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event.data, e1.data);
        assert_eq!(events[1].event.data, e3.data);
        assert_eq!(head, Some(2));

        // Query by tag x returns only the first event
        let (tagx_events, _) = store
            .read_with_head(Some(query_tag_x.clone()), None, false, None)
            .unwrap();
        assert_eq!(tagx_events.len(), 1);
        assert_eq!(tagx_events[0].event.data, e1.data);
    }

    #[test]
    fn test_append_batch_dirty_visibility_with_types_small_and_big_overflow() {
        let temp_dir = tempdir().unwrap();
        let store = UmaDB::new(temp_dir.path()).unwrap();

        // Prepare events
        let small = DCBEvent {
            event_type: "S".into(),
            data: b"sm".to_vec(),
            tags: vec!["tS".into()],
            uuid: None,
        };
        // Large data to ensure it spills into event overflow pages
        let big_data_len = DEFAULT_PAGE_SIZE * 3; // 3 pages worth to be safe
        let big = DCBEvent {
            event_type: "B".into(),
            data: vec![0xAB; big_data_len],
            tags: vec!["tB".into()],
            uuid: None,
        };
        let filler1 = DCBEvent {
            event_type: "X".into(),
            data: b"x".to_vec(),
            tags: vec![],
            uuid: None,
        };
        let filler2 = DCBEvent {
            event_type: "Y".into(),
            data: b"y".to_vec(),
            tags: vec![],
            uuid: None,
        };
        let final_ok = DCBEvent {
            event_type: "C".into(),
            data: b"c".to_vec(),
            tags: vec![],
            uuid: None,
        };

        // Queries by type only (no tags) to force fallback path over events tree (which reads from dirty pages)
        let q_type_s = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec!["S".into()],
                tags: vec![],
            }],
        };
        let q_type_b = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec!["B".into()],
                tags: vec![],
            }],
        };

        let items = vec![
            // 1) Append small S
            (vec![small.clone()], None, None),
            // 2) Should fail because type S exists in dirty pages (after None)
            (
                vec![filler1.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: q_type_s.clone(),
                    after: None,
                }),
                None,
            ),
            // 3) Append big B (overflow)
            (vec![big.clone()], None, None),
            // 4) Should fail because type B exists in dirty pages (after None)
            (
                vec![filler2.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: q_type_b.clone(),
                    after: None,
                }),
                None,
            ),
            // 5) Should succeed because after=Some(2) ignores positions <= 2 (small at 1, big at 2)
            (
                vec![final_ok.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: q_type_b.clone(),
                    after: Some(2),
                }),
                None,
            ),
        ];

        let results = store.append_batch(items).unwrap();
        assert_eq!(results.len(), 5);
        match &results[0] {
            Ok(pos) => assert_eq!(*pos, 1),
            other => panic!("unexpected for item0: {:?}", other),
        }
        match &results[1] {
            Err(DCBError::IntegrityError(_)) => {}
            other => {
                panic!("expected IntegrityError for item1, got {:?}", other)
            }
        }
        match &results[2] {
            Ok(pos) => assert_eq!(*pos, 2),
            other => panic!("unexpected for item2: {:?}", other),
        }
        match &results[3] {
            Err(DCBError::IntegrityError(_)) => {}
            other => {
                panic!("expected IntegrityError for item3, got {:?}", other)
            }
        }
        match &results[4] {
            Ok(pos) => assert_eq!(*pos, 3),
            other => panic!("unexpected for item4: {:?}", other),
        }

        // Verify committed state: we should have small (pos1), big (pos2), final_ok (pos3)
        let (events, head) = store.read_with_head(None, None, false, None).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event.event_type, small.event_type);
        assert_eq!(events[1].event.event_type, big.event_type);
        assert_eq!(events[2].event.event_type, final_ok.event_type);
        assert_eq!(head, Some(3));

        // Check type queries and large data integrity
        let (small_by_type, _) = store
            .read_with_head(Some(q_type_s.clone()), None, false, None)
            .unwrap();
        assert_eq!(small_by_type.len(), 1);
        assert_eq!(small_by_type[0].event.event_type, "S");

        let (big_by_type, _) = store
            .read_with_head(Some(q_type_b.clone()), None, false, None)
            .unwrap();
        assert_eq!(big_by_type.len(), 1);
        assert_eq!(big_by_type[0].event.event_type, "B");
        assert_eq!(big_by_type[0].event.data.len(), big_data_len);
        assert!(big_by_type[0].event.data.iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn test_append_batch_dirty_visibility_with_tags_and_types_small_and_big_overflow() {
        let temp_dir = tempdir().unwrap();
        let store = UmaDB::new(temp_dir.path()).unwrap();

        // Small inline event: type "S" with tag "x"
        let small = DCBEvent {
            event_type: "S".into(),
            data: b"sm".to_vec(),
            tags: vec!["x".into()],
            uuid: None,
        };
        // Big overflow event: type "B" with tag "y" and large payload to exercise overflow pages
        let big_data_len = DEFAULT_PAGE_SIZE * 3; // ensure multiple overflow pages
        let big = DCBEvent {
            event_type: "B".into(),
            data: vec![0xCD; big_data_len],
            tags: vec!["y".into()],
            uuid: None,
        };
        // Fillers that will be conditioned out
        let filler1 = DCBEvent {
            event_type: "X".into(),
            data: b"x".to_vec(),
            tags: vec![],
            uuid: None,
        };
        let filler2 = DCBEvent {
            event_type: "Y".into(),
            data: b"y".to_vec(),
            tags: vec![],
            uuid: None,
        };
        let final_ok = DCBEvent {
            event_type: "C".into(),
            data: b"c".to_vec(),
            tags: vec![],
            uuid: None,
        };

        // Conditions combining tags and types so the tags index is used and the type filter applies after lookup
        let q_s_and_x = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec!["S".into()],
                tags: vec!["x".into()],
            }],
        };
        let q_b_and_y = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec!["B".into()],
                tags: vec!["y".into()],
            }],
        };

        let items = vec![
            // 1) Append small S@x
            (vec![small.clone()], None, None),
            // 2) Should fail because S@x exists in dirty pages (tags path + type filter)
            (
                vec![filler1.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: q_s_and_x.clone(),
                    after: None,
                }),
                None,
            ),
            // 3) Append big B@y (overflow)
            (vec![big.clone()], None, None),
            // 4) Should fail because B@y exists in dirty pages (tags path + type filter and overflow read)
            (
                vec![filler2.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: q_b_and_y.clone(),
                    after: None,
                }),
                None,
            ),
            // 5) Should succeed because after=Some(2) ignores positions <= 2 (small at 1, big at 2)
            (
                vec![final_ok.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: q_b_and_y.clone(),
                    after: Some(2),
                }),
                None,
            ),
        ];

        let results = store.append_batch(items).unwrap();
        assert_eq!(results.len(), 5);
        match &results[0] {
            Ok(pos) => assert_eq!(*pos, 1),
            other => panic!("unexpected for item0: {:?}", other),
        }
        match &results[1] {
            Err(DCBError::IntegrityError(_)) => {}
            other => {
                panic!("expected IntegrityError for item1, got {:?}", other)
            }
        }
        match &results[2] {
            Ok(pos) => assert_eq!(*pos, 2),
            other => panic!("unexpected for item2: {:?}", other),
        }
        match &results[3] {
            Err(DCBError::IntegrityError(_)) => {}
            other => {
                panic!("expected IntegrityError for item3, got {:?}", other)
            }
        }
        match &results[4] {
            Ok(pos) => assert_eq!(*pos, 3),
            other => panic!("unexpected for item4: {:?}", other),
        }

        // Verify committed state and order
        let (events, head) = store.read_with_head(None, None, false, None).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event.event_type, small.event_type);
        assert_eq!(events[1].event.event_type, big.event_type);
        assert_eq!(events[2].event.event_type, final_ok.event_type);
        assert_eq!(head, Some(3));

        // Query by combined type+tag should return exactly one for each
        let (small_combined, _) = store
            .read_with_head(Some(q_s_and_x.clone()), None, false, None)
            .unwrap();
        assert_eq!(small_combined.len(), 1);
        assert_eq!(small_combined[0].event.event_type, "S");
        assert!(small_combined[0].event.tags.iter().any(|t| t == "x"));

        let (big_combined, _) = store
            .read_with_head(Some(q_b_and_y.clone()), None, false, None)
            .unwrap();
        assert_eq!(big_combined.len(), 1);
        assert_eq!(big_combined[0].event.event_type, "B");
        assert!(big_combined[0].event.tags.iter().any(|t| t == "y"));
        assert_eq!(big_combined[0].event.data.len(), big_data_len);
        assert!(big_combined[0].event.data.iter().all(|&b| b == 0xCD));
    }

    #[test]
    fn test_append_event_with_uuid_is_maintained_and_activated_append_idempotency() {
        let temp_dir = tempdir().unwrap();
        let store = UmaDB::new(temp_dir.path()).unwrap();

        let condition1 = Some(DCBAppendCondition {
            fail_if_events_match: DCBQuery { items: vec![] },
            after: None,
        });

        let event1 = DCBEvent {
            event_type: "type1".to_string(),
            data: b"data1".to_vec(),
            tags: vec!["tag1".to_string()],
            uuid: Some(Uuid::new_v4()),
        };

        let mut commit_position1 = store
            .append(vec![event1.clone()], condition1.clone(), None)
            .unwrap();
        assert_eq!(1, commit_position1);

        let (result, head) = store.read_with_head(None, None, false, None).unwrap();
        assert_eq!(1, result.len());
        assert_eq!(Some(1), head);
        assert_eq!(event1.uuid, result[0].event.uuid);

        // Test idempotency - retry the same append operation.
        commit_position1 = store
            .append(vec![event1.clone()], condition1.clone(), None)
            .unwrap();

        // Check the response is the same as before.
        assert_eq!(1, commit_position1);

        // Check we still have only one sequenced event.
        let (result, head) = store.read_with_head(None, None, false, None).unwrap();
        assert_eq!(1, result.len());
        assert_eq!(Some(1), head);
        assert_eq!(event1.uuid, result[0].event.uuid);

        // Append another event.
        let event2 = DCBEvent {
            event_type: "type2".to_string(),
            data: b"data2".to_vec(),
            tags: vec!["tag2".to_string()],
            uuid: Some(Uuid::new_v4()),
        };

        let mut commit_position2 = store.append(vec![event2.clone()], None, None).unwrap();
        assert_eq!(2, commit_position2);

        // Check we have two sequenced events.
        let (result, head) = store.read_with_head(None, None, false, None).unwrap();
        assert_eq!(2, result.len());
        assert_eq!(Some(2), head);
        assert_eq!(event1.uuid, result[0].event.uuid);
        assert_eq!(event2.uuid, result[1].event.uuid);

        // Test idempotency - retry the same append operation.
        commit_position1 = store
            .append(vec![event1.clone()], condition1.clone(), None)
            .unwrap();

        // Check the response is the same as before.
        assert_eq!(1, commit_position1);

        // Test idempotency - try an operation with event1 and event2.
        commit_position2 = store
            .append(
                vec![event1.clone(), event2.clone()],
                condition1.clone(),
                None,
            )
            .unwrap();

        // Check the response is the same as before.
        assert_eq!(2, commit_position2);

        // Check we still have two sequenced events.
        let (result, head) = store.read_with_head(None, None, false, None).unwrap();
        assert_eq!(2, result.len());
        assert_eq!(Some(2), head);
        assert_eq!(event1.uuid, result[0].event.uuid);
        assert_eq!(event2.uuid, result[1].event.uuid);

        // Try with event2 and condition1 - should get an error.
        let result = store.append(vec![event2.clone()], condition1.clone(), None);
        assert!(matches!(result, Err(DCBError::IntegrityError(_))));

        // Try with two events in different order - should get an error.
        let result = store.append(
            vec![event2.clone(), event1.clone()],
            condition1.clone(),
            None,
        );
        assert!(matches!(result, Err(DCBError::IntegrityError(_))));
    }

    #[test]
    #[serial]
    fn empty_query_backwards_from_and_limit() {
        let (_tmp, mvcc, _input) = setup_db_with_standard_events();
        let reader = mvcc.reader().unwrap();

        // Forwards: all events starting from position 1
        let fwd = read_conditional(
            &mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            Some(Position(1)),
            false,
            None,
        )
        .unwrap();
        let fwd_pos: Vec<u64> = fwd.iter().map(|e| e.position).collect();
        assert!(!fwd_pos.is_empty());
        assert!(fwd_pos.windows(2).all(|w| w[0] < w[1]));

        // Backwards: all events (from=None) in descending order
        let back_all = read_conditional(
            &mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            None,
            true,
            None,
        )
        .unwrap();
        let back_all_pos: Vec<u64> = back_all.iter().map(|e| e.position).collect();
        let mut fwd_rev = fwd_pos.clone();
        fwd_rev.reverse();
        assert_eq!(fwd_rev, back_all_pos);

        // Backwards with from=last should still return all (<= last)
        let last = *fwd_pos.last().unwrap();
        let back_from_last = read_conditional(
            &mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            Some(Position(last)),
            true,
            None,
        )
        .unwrap();
        let back_from_last_pos: Vec<u64> = back_from_last.iter().map(|e| e.position).collect();
        assert_eq!(back_from_last_pos, fwd_rev);

        // Backwards with from=last-1 should drop the very last element
        let back_from_before_last = read_conditional(
            &mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            Some(Position(last - 1)),
            true,
            None,
        )
        .unwrap();
        let back_from_before_last_pos: Vec<u64> =
            back_from_before_last.iter().map(|e| e.position).collect();
        assert_eq!(back_from_before_last_pos, fwd_rev[1..].to_vec());

        // Limit in backwards order: first 3 of the reversed forward vector
        let back_lim3 = read_conditional(
            &mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            None,
            true,
            Some(3),
        )
        .unwrap();
        let back_lim3_pos: Vec<u64> = back_lim3.iter().map(|e| e.position).collect();
        assert_eq!(back_lim3_pos, fwd_rev[..3.min(fwd_rev.len())].to_vec());
    }

    #[test]
    #[serial]
    fn tags_only_single_tag_backwards() {
        let (_tmp, mvcc, _input) = setup_db_with_standard_events();
        let reader = mvcc.reader().unwrap();
        let qi = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec!["alpha".to_string()],
            }],
        };

        let fwd = read_conditional(
            &mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Some(Position(1)),
            false,
            None,
        )
        .unwrap();
        let fwd_pos: Vec<u64> = fwd.iter().map(|e| e.position).collect();
        assert!(!fwd_pos.is_empty());
        assert!(fwd_pos.windows(2).all(|w| w[0] < w[1]));

        let back_all = read_conditional(
            &mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            None,
            true,
            None,
        )
        .unwrap();
        let back_all_pos: Vec<u64> = back_all.iter().map(|e| e.position).collect();
        let mut fwd_rev = fwd_pos.clone();
        fwd_rev.reverse();
        assert_eq!(back_all_pos, fwd_rev);

        // from = just before the last matching position should drop the newest one in backwards order
        let last = *fwd_pos.last().unwrap();
        let back_from_before_last = read_conditional(
            &mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi,
            Some(Position(last - 1)),
            true,
            None,
        )
        .unwrap();
        let back_from_before_last_pos: Vec<u64> =
            back_from_before_last.iter().map(|e| e.position).collect();
        assert_eq!(back_from_before_last_pos, fwd_rev[1..].to_vec());
    }

    #[test]
    #[serial]
    fn tags_only_multi_tag_and_backwards() {
        let (_tmp, db, _input) = setup_db_with_standard_events();
        let reader = db.reader().unwrap();
        let qi = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec!["alpha".to_string(), "gamma".to_string()],
            }],
        };

        // Forwards baseline
        let fwd = read_conditional(
            &db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Some(Position(1)),
            false,
            None,
        )
        .unwrap();
        let fwd_pos: Vec<u64> = fwd.iter().map(|e| e.position).collect();
        assert!(!fwd_pos.is_empty());
        assert!(fwd_pos.windows(2).all(|w| w[0] < w[1]));
        // All should include both tags
        assert!(
            fwd.iter()
                .all(|e| e.event.tags.iter().any(|t| t == "alpha"))
        );
        assert!(
            fwd.iter()
                .all(|e| e.event.tags.iter().any(|t| t == "gamma"))
        );

        // Backwards from=None should equal reverse of forwards
        let back_all = read_conditional(
            &db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            None,
            true,
            None,
        )
        .unwrap();
        let mut fwd_rev = fwd_pos.clone();
        fwd_rev.reverse();
        let back_all_pos: Vec<u64> = back_all.iter().map(|e| e.position).collect();
        assert_eq!(back_all_pos, fwd_rev);

        // Backwards from=last should still return full reverse (<= last)
        let last = *fwd_pos.last().unwrap();
        let back_from_last = read_conditional(
            &db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Some(Position(last)),
            true,
            None,
        )
        .unwrap();
        let back_from_last_pos: Vec<u64> = back_from_last.iter().map(|e| e.position).collect();
        assert_eq!(back_from_last_pos, fwd_rev);

        // Backwards from just before last should drop newest
        let back_from_before_last = read_conditional(
            &db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Some(Position(last - 1)),
            true,
            None,
        )
        .unwrap();
        let back_from_before_last_pos: Vec<u64> =
            back_from_before_last.iter().map(|e| e.position).collect();
        assert_eq!(back_from_before_last_pos, fwd_rev[1..].to_vec());

        // Backwards with limit
        let back_lim2 = read_conditional(
            &db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi,
            None,
            true,
            Some(2),
        )
        .unwrap();
        let back_lim2_pos: Vec<u64> = back_lim2.iter().map(|e| e.position).collect();
        assert_eq!(back_lim2_pos, fwd_rev[..2.min(fwd_rev.len())].to_vec());
    }

    #[test]
    #[serial]
    fn tags_multi_item_two_tags_each_backwards() {
        let (_tmp, db, _input) = setup_db_with_standard_events();
        let reader = db.reader().unwrap();
        // Two items: (alpha & gamma) OR (beta & delta)
        let qi = DCBQuery {
            items: vec![
                DCBQueryItem {
                    types: vec![],
                    tags: vec!["alpha".to_string(), "gamma".to_string()],
                },
                DCBQueryItem {
                    types: vec![],
                    tags: vec!["beta".to_string(), "delta".to_string()],
                },
            ],
        };

        // Forwards baseline
        let fwd = read_conditional(
            &db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Some(Position(1)),
            false,
            None,
        )
        .unwrap();
        let fwd_pos: Vec<u64> = fwd.iter().map(|e| e.position).collect();
        assert!(!fwd_pos.is_empty());
        assert!(fwd_pos.windows(2).all(|w| w[0] < w[1]));
        // Each event must satisfy one of the items fully
        assert!(fwd.iter().all(|e| {
            let tags = &e.event.tags;
            let has = |a: &str| tags.iter().any(|t| t == a);
            (has("alpha") && has("gamma")) || (has("beta") && has("delta"))
        }));

        // Backwards with None should be reverse
        let back_all = read_conditional(
            &db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            None,
            true,
            None,
        )
        .unwrap();
        let mut fwd_rev = fwd_pos.clone();
        fwd_rev.reverse();
        let back_all_pos: Vec<u64> = back_all.iter().map(|e| e.position).collect();
        assert_eq!(back_all_pos, fwd_rev);

        // Backwards with limit 1 should return newest matching
        let back_lim1 = read_conditional(
            &db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi,
            None,
            true,
            Some(1),
        )
        .unwrap();
        assert_eq!(back_lim1.len(), 1);
        assert_eq!(back_lim1[0].position, *fwd_rev.first().unwrap());
    }
}
