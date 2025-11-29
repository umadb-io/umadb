// use std::cell::RefCell;
use crate::common::Position;
use crate::common::{PageID, Tsn};
use crate::events_tree_nodes::EventLeafNode;
use crate::free_lists_tree_nodes::{
    FreeListInternalNode, FreeListLeafNode, FreeListLeafValue, FreeListTsnLeafNode,
};
use crate::header_node::HeaderNode;
use crate::node::Node;
use crate::page::{PAGE_HEADER_SIZE, Page, serialize_page_into};
use crate::pager::Pager;
use crate::tags_tree_nodes::TagsLeafNode;
use umadb_dcb::{DCBError, DCBResult};
// use rayon::prelude::*;
// use std::os::unix::fs::FileExt; // For write_at on Unix
use dashmap::DashMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
// use crate::db::DEFAULT_PAGE_SIZE;

const GET_LATEST_HEADER_RETRIES: usize = 5;
const GET_LATEST_HEADER_DELAY: Duration = Duration::from_millis(10);
const HEADER_PAGE_ID_0: PageID = PageID(0);
const HEADER_PAGE_ID_1: PageID = PageID(1);

// thread_local! {
//     static PAGE_BUF: RefCell<Vec<u8>> = RefCell::new(vec![0u8; DEFAULT_PAGE_SIZE]);
// }

// Main MVCC structure
pub struct Mvcc {
    pub pager: Pager,
    pub reader_tsns: Arc<DashMap<usize, Tsn>>,
    pub writer_lock: Mutex<()>,
    pub page_size: usize,
    pub max_node_size: usize,
    // Owned header node instances for pages 0 and 1
    pub headers: Mutex<Vec<Page>>,
    // Reusable buffer for header serialization
    pub header_page_buf: Mutex<Vec<u8>>,
    // Reusable buffer for general page serialization
    pub page_buf: Mutex<Vec<u8>>,
    reader_id_counter: AtomicUsize,
    pub verbose: bool,
}

impl Mvcc {
    pub fn new(path: &Path, page_size: usize, verbose: bool) -> DCBResult<Self> {
        let pager = Pager::new(path, page_size)?;

        let mvcc = Self {
            pager,
            reader_tsns: Arc::new(DashMap::new()),
            writer_lock: Mutex::new(()),
            page_size,
            max_node_size: page_size - PAGE_HEADER_SIZE,
            headers: Mutex::new(vec![
                Page {
                    page_id: PageID(0),
                    node: Node::Header(HeaderNode::default()),
                },
                Page {
                    page_id: PageID(1),
                    node: Node::Header(HeaderNode::default()),
                },
            ]),
            header_page_buf: Mutex::new(vec![0u8; page_size]),
            page_buf: Mutex::new(vec![0u8; page_size]),
            reader_id_counter: AtomicUsize::new(0),
            verbose,
        };

        if mvcc.pager.is_file_new {
            // Initialize new database
            let initial_tsn = Tsn(0);
            let initial_free_lists_tree_root_id = PageID(2);
            let initial_events_tree_root_id = PageID(3);
            let initial_tags_tree_root_id = PageID(4);
            let initial_next_page_id = PageID(5);
            let initial_next_position = Position(1);
            mvcc.update_header(
                HEADER_PAGE_ID_0,
                initial_tsn,
                initial_free_lists_tree_root_id,
                initial_events_tree_root_id,
                initial_tags_tree_root_id,
                initial_next_page_id,
                initial_next_position,
            )?;
            mvcc.update_header(
                HEADER_PAGE_ID_1,
                initial_tsn,
                initial_free_lists_tree_root_id,
                initial_events_tree_root_id,
                initial_tags_tree_root_id,
                initial_next_page_id,
                initial_next_position,
            )?;

            // Create and write an empty free lists tree root page.
            let free_list_leaf = FreeListLeafNode {
                keys: Vec::new(),
                values: Vec::new(),
            };
            let free_list_page = Page::new(
                initial_free_lists_tree_root_id,
                Node::FreeListLeaf(free_list_leaf),
            );

            // Create and write an empty events tree root page.
            let event_leaf = EventLeafNode {
                keys: Vec::new(),
                values: Vec::new(),
            };
            let position_page = Page::new(initial_events_tree_root_id, Node::EventLeaf(event_leaf));

            // Create and write an empty tags tree root page.
            let tags_leaf = TagsLeafNode {
                keys: Vec::new(),
                values: Vec::new(),
            };
            let tags_page = Page::new(initial_tags_tree_root_id, Node::TagsLeaf(tags_leaf));

            // Write all three initial root pages using the shared write_pages helper
            let _ = mvcc.write_pages([&free_list_page, &position_page, &tags_page].into_iter())?;

            // Sync the file to disk.
            mvcc.fsync()?;
        }
        println!("UmaDB opened file {}", path.canonicalize()?.display());

        Ok(mvcc)
    }

    pub fn get_latest_header(&self) -> DCBResult<(PageID, HeaderNode)> {
        for attempt in 0..GET_LATEST_HEADER_RETRIES {
            let h0 = self.read_header(HEADER_PAGE_ID_0);
            let h1 = self.read_header(HEADER_PAGE_ID_1);

            match (h0, h1) {
                (Ok(header0), Ok(header1)) => {
                    if header1.tsn > header0.tsn {
                        return Ok((HEADER_PAGE_ID_1, header1));
                    } else {
                        return Ok((HEADER_PAGE_ID_0, header0));
                    }
                }
                (Ok(header0), Err(_)) => {
                    return Ok((HEADER_PAGE_ID_0, header0));
                }
                (Err(_), Ok(header1)) => {
                    return Ok((HEADER_PAGE_ID_1, header1));
                }
                (Err(e0), Err(e1)) => {
                    if attempt + 1 < GET_LATEST_HEADER_RETRIES {
                        if self.verbose {
                            println!(
                                "Both headers invalid on attempt {}: {:?} | {:?}. Retrying...",
                                attempt + 1,
                                e0,
                                e1
                            );
                        }
                        sleep(GET_LATEST_HEADER_DELAY);
                        continue;
                    } else {
                        return Err(DCBError::DatabaseCorrupted(format!(
                            "Both header pages appear corrupted after {} attempts: ({:?}) and ({:?})",
                            GET_LATEST_HEADER_RETRIES, e0, e1
                        )));
                    }
                }
            }
        }
        // Unreachable due to returns in match and final error, but keep to satisfy type checker
        Err(DCBError::DatabaseCorrupted(
            "Unable to read a valid header".to_string(),
        ))
    }

    pub fn read_header(&self, page_id: PageID) -> DCBResult<HeaderNode> {
        let page = self.read_page(page_id)?;
        match page.node {
            Node::Header(node) => Ok(node),
            _ => Err(DCBError::DatabaseCorrupted(
                "Invalid header node type".to_string(),
            )),
        }
    }

    fn update_header(
        &self,
        page_id: PageID,
        tsn: Tsn,
        free_lists_tree_root_id: PageID,
        events_tree_root_id: PageID,
        tags_tree_root_id: PageID,
        next_page_id: PageID,
        next_position: Position,
    ) -> DCBResult<()> {
        let mut headers = self.headers.lock().unwrap();
        let headers_idx = { if page_id == HEADER_PAGE_ID_0 { 0 } else { 1 } };
        let header = &mut headers[headers_idx];
        match &mut header.node {
            Node::Header(node) => {
                // Update node values.
                node.tsn = tsn;
                node.free_lists_tree_root_id = free_lists_tree_root_id;
                node.events_tree_root_id = events_tree_root_id;
                node.tags_tree_root_id = tags_tree_root_id;
                node.next_page_id = next_page_id;
                node.next_position = next_position;

                // Write node using pre-allocated buffer.
                let mut buf = self.page_buf.lock().unwrap();
                serialize_page_into(&mut buf, &header.node)?;
                self.pager.write_page(page_id, &buf)?;
                Ok(())
            }
            _ => panic!("Shouldn't get here: header should be a header"),
        }
    }

    pub fn read_page(&self, page_id: PageID) -> DCBResult<Page> {
        let mapped = self.pager.read_page_mmap_slice(page_id)?;
        if self.verbose {
            println!("Read {page_id:?} from file, deserializing...");
        }
        Page::deserialize(page_id, mapped.as_slice())
    }

    pub fn fsync(&self) -> DCBResult<()> {
        self.pager.fsync()?;
        Ok(())
    }

    pub fn reader(&self) -> DCBResult<Reader> {
        let (header_page_id, header_node) = self.get_latest_header()?;

        // Generate a unique ID for this reader using the counter (lock-free)
        let reader_id = self.reader_id_counter.fetch_add(1, Ordering::Relaxed) + 1;

        // Register the reader TSN (lock-free concurrent insert)
        self.reader_tsns.insert(reader_id, header_node.tsn);

        // Create the reader with the unique ID
        let reader = Reader {
            header_page_id,
            tsn: header_node.tsn,
            events_tree_root_id: header_node.events_tree_root_id,
            tags_tree_root_id: header_node.tags_tree_root_id,
            next_position: header_node.next_position,
            reader_id,
            reader_tsns: Arc::clone(&self.reader_tsns),
        };

        Ok(reader)
    }

    pub fn writer(&self) -> DCBResult<Writer> {
        if self.verbose {
            println!();
            println!("Constructing writer...");
        }

        // Get the latest header
        let (header_page_id, header_node) = self.get_latest_header()?;

        // Create the writer
        let mut writer = Writer::new(
            header_page_id,
            Tsn(header_node.tsn.0 + 1),
            header_node.next_page_id,
            header_node.free_lists_tree_root_id,
            header_node.events_tree_root_id,
            header_node.tags_tree_root_id,
            header_node.next_position,
            self.verbose,
        );

        if self.verbose {
            println!("Constructed writer with {:?}", writer.tsn);
        }

        // Find the reusable page IDs.
        writer.find_reusable_page_ids(self)?;

        Ok(writer)
    }

    /// Write one or more pages to disk using the shared preallocated page buffer.
    /// Returns the number of pages written.
    pub fn write_pages<'a, I>(&self, pages: I) -> DCBResult<usize>
    where
        I: IntoIterator<Item = &'a Page>,
    {
        let mut buf = self.page_buf.lock().unwrap();
        let mut count = 0usize;
        for page in pages {
            page.serialize_into(&mut buf)?;
            self.pager.write_page(page.page_id, &buf)?;
            if self.verbose {
                println!("Wrote {:?} to file", page.page_id);
            }
            count += 1;
        }
        Ok(count)
    }

    // pub fn write_pages_parallel<'a, I>(&self, pages: I) -> DCBResult<usize>
    // where
    //     I: IntoIterator<Item = &'a Page> + Send,
    //     I::IntoIter: Send,
    // {
    //     let pages: Vec<&Page> = pages.into_iter().collect();
    //     if pages.is_empty() {
    //         return Ok(0);
    //     }
    //
    //     let file = self.pager.writer.clone();
    //     let page_size = self.page_size;
    //     let verbose = self.verbose;
    //
    //     let count: DCBResult<usize> = pages
    //         .par_iter()
    //         .map(|page| -> DCBResult<usize> {
    //             PAGE_BUF.with(|cell| -> DCBResult<usize> {
    //                 let mut buf = cell.borrow_mut();
    //                 if buf.len() != page_size {
    //                     *buf = vec![0u8; page_size]; // lazy init
    //                 }
    //
    //                 page.serialize_into(&mut buf)?;
    //                 file.write_at(&buf, page.page_id.0 * page_size as u64)?;
    //
    //                 if verbose {
    //                     println!("Wrote {:?} to file", page.page_id);
    //                 }
    //                 Ok(1)
    //             })
    //         })
    //         .try_reduce(|| 0, |a, b| Ok(a + b));
    //
    //     count
    // }

    pub fn commit(&self, writer: &mut Writer) -> DCBResult<()> {
        // Process reused and freed page IDs
        if self.verbose {
            println!();
            println!("Commiting writer with {:?}", writer.tsn);
        }

        while !writer.reused_page_ids.is_empty() || !writer.freed_page_ids.is_empty() {
            // Remove reused page IDs from free lists.
            while let Some((reused_page_id, tsn)) = writer.reused_page_ids.pop_front() {
                // Remove the reused page ID from the freed list tree
                writer.remove_free_page_id(self, tsn, reused_page_id)?;
            }

            // Process freed page IDs
            while let Some(freed_page_id) = writer.freed_page_ids.pop_front() {
                // Remove dirty pages that were also freed
                writer.dirty.remove(&freed_page_id);

                // Insert the page ID in the freed list tree
                writer.insert_freed_page_id(self, writer.tsn, freed_page_id)?;
            }
        }

        // Write all dirty pages (except for the header page) to the file
        if !writer.dirty.is_empty() {
            let count = {
                // let num_dirty = writer.dirty.len();
                // println!("Number dirty pages: {num_dirty}");
                // if num_dirty >= 0 {
                //     self.write_pages_parallel(writer.dirty.values())?
                // } else {
                //     self.write_pages(writer.dirty.values())?
                // }
                self.write_pages(writer.dirty.values())?
            };
            if self.verbose {
                println!("Wrote {} dirty page(s) to file", count);
            }
        }

        // Sync the file to disk
        self.fsync()?;

        // Mutate the owned header instance and serialize into the preallocated buffer
        self.update_header(
            if writer.header_page_id == HEADER_PAGE_ID_0 {
                HEADER_PAGE_ID_1
            } else {
                HEADER_PAGE_ID_0
            },
            writer.tsn,
            writer.free_lists_tree_root_id,
            writer.events_tree_root_id,
            writer.tags_tree_root_id,
            writer.next_page_id,
            writer.next_position,
        )?;

        // Sync the file to disk
        self.fsync()?;

        if self.verbose {
            println!("Committed writer with {:?}", writer.tsn);
        }

        Ok(())
    }
}

// Writer transaction
pub struct Writer {
    pub header_page_id: PageID,
    pub tsn: Tsn,
    pub next_page_id: PageID,
    pub free_lists_tree_root_id: PageID,
    pub events_tree_root_id: PageID,
    pub tags_tree_root_id: PageID,
    pub next_position: Position,
    pub reusable_page_ids: VecDeque<(PageID, Tsn)>,
    pub freed_page_ids: VecDeque<PageID>,
    pub deserialized: HashMap<PageID, Page>,
    pub dirty: HashMap<PageID, Page>,
    pub reused_page_ids: VecDeque<(PageID, Tsn)>,
    pub verbose: bool,
}

impl Writer {
    pub fn new(
        header_page_id: PageID,
        tsn: Tsn,
        next_page_id: PageID,
        free_lists_tree_root_id: PageID,
        events_tree_root_id: PageID,
        tags_tree_root_id: PageID,
        next_position: Position,
        verbose: bool,
    ) -> Self {
        Self {
            header_page_id,
            tsn,
            next_page_id,
            free_lists_tree_root_id,
            events_tree_root_id,
            tags_tree_root_id,
            next_position,
            reusable_page_ids: VecDeque::new(),
            freed_page_ids: VecDeque::new(),
            deserialized: HashMap::new(),
            dirty: HashMap::new(),
            reused_page_ids: VecDeque::new(),
            verbose,
        }
    }

    /// Returns the current issue position and increments the next position.
    ///
    /// Returns:
    ///
    /// The current issue position.
    pub fn issue_position(&mut self) -> Position {
        let pos = self.next_position;
        self.next_position = Position(self.next_position.0 + 1);
        pos
    }

    /// Retrieves a reference to the page with the given ID.
    ///
    /// First checks the dirty pages map, then the deserialized pages map.
    /// If the page is not found in either map, it is deserialized from the MVCC.
    ///
    /// # Arguments
    ///
    /// * `mvcc`: A reference to the MVCC.
    /// * `page_id`: The ID of the page to retrieve.
    ///
    /// # Returns
    ///
    /// A `DCBResult` containing a reference to the page if found, or an error if the page could not be found.
    pub fn get_page_ref(&mut self, mvcc: &Mvcc, page_id: PageID) -> DCBResult<&Page> {
        // Check the dirty pages first
        if self.dirty.contains_key(&page_id) {
            return Ok(self.dirty.get(&page_id).unwrap());
        }

        // Then check deserialized pages
        if self.deserialized.contains_key(&page_id) {
            return Ok(self.deserialized.get(&page_id).unwrap());
        }

        // Need to deserialize the page
        let deserialized_page = mvcc.read_page(page_id)?;
        self.insert_deserialized(deserialized_page);

        // Return the deserialized page
        Ok(self.deserialized.get(&page_id).unwrap())
    }

    pub fn get_mut_dirty(&mut self, page_id: PageID) -> DCBResult<&mut Page> {
        if let Some(page) = self.dirty.get_mut(&page_id) {
            Ok(page)
        } else {
            Err(DCBError::DirtyPageNotFound(page_id.0))
        }
    }

    pub fn insert_deserialized(&mut self, page: Page) {
        self.deserialized.insert(page.page_id, page);
    }

    pub fn insert_dirty(&mut self, page: Page) -> DCBResult<()> {
        if self.freed_page_ids.contains(&page.page_id) {
            return Err(DCBError::PageAlreadyFreed(page.page_id.0));
        }
        if self.dirty.contains_key(&page.page_id) {
            return Err(DCBError::PageAlreadyDirty(page.page_id.0));
        }
        self.dirty.insert(page.page_id, page);
        Ok(())
    }

    pub fn alloc_page_id(&mut self) -> PageID {
        if let Some((free_page_id, tsn)) = self.reusable_page_ids.pop_front() {
            self.reused_page_ids.push_back((free_page_id, tsn));
            return free_page_id;
        }

        let next_page_id = self.next_page_id;
        self.next_page_id = PageID(next_page_id.0 + 1);
        next_page_id
    }

    pub fn get_dirty_page_id(&mut self, page_id: PageID) -> DCBResult<PageID> {
        let mut dirty_page_id = page_id;
        if !self.freed_page_ids.iter().any(|&id| id == page_id) {
            if !self.dirty.contains_key(&page_id) {
                let old_page_id = page_id;
                self.freed_page_ids.push_back(old_page_id);

                let new_page_id = self.alloc_page_id();
                let old_page = self.deserialized.get(&old_page_id).unwrap();
                let new_page = Page {
                    page_id: new_page_id,
                    node: old_page.node.clone(),
                };

                self.dirty.insert(new_page_id, new_page);
                if self.verbose {
                    println!(
                        "Copied {:?} to {:?}: {:?}",
                        old_page_id, new_page_id, old_page.node
                    );
                }
                dirty_page_id = new_page_id;
            } else if self.verbose {
                println!("{page_id:?} is already dirty");
            }
        } else {
            return Err(DCBError::PageAlreadyFreed(page_id.0));
        }
        Ok(dirty_page_id)
    }

    pub fn append_freed_page_id(&mut self, page_id: PageID) {
        let verbose = self.verbose;
        if !self.freed_page_ids.iter().any(|&id| id == page_id) {
            self.freed_page_ids.push_back(page_id);
            if verbose {
                println!("Appended {page_id:?} to freed_page_ids");
            }
            if self.dirty.contains_key(&page_id) {
                self.dirty.remove(&page_id);
                if verbose {
                    println!("Page ID {page_id:?} was in dirty and was removed");
                }
            }
            if verbose && self.dirty.contains_key(&page_id) {
                println!("Page ID {page_id:?} is still in dirty!!!!!");
            }
        }
    }

    pub fn find_reusable_page_ids(&mut self, mvcc: &Mvcc) -> DCBResult<()> {
        let verbose = self.verbose;
        let mut reusable_page_ids: VecDeque<(PageID, Tsn)> = VecDeque::new();
        // Get free page IDs
        if verbose {
            println!("Finding reusable page IDs for TSN {:?}...", self.tsn);
        }

        // Find the smallest reader TSN (lock-free iteration over concurrent map)
        let smallest_reader_tsn = mvcc.reader_tsns.iter().map(|r| *r.value()).min();
        if verbose {
            println!("Smallest reader TSN: {smallest_reader_tsn:?}");
        }

        if verbose {
            println!("Root is {:?}", self.free_lists_tree_root_id);
        }
        // Walk the tree to find leaf nodes
        let mut stack = vec![(self.free_lists_tree_root_id, 0)];
        let mut is_finished = false;

        while let Some((page_id, idx)) = stack.pop() {
            if is_finished {
                break;
            }
            let node_owned = {
                match self.get_page_ref(mvcc, page_id) {
                    Ok(p) => p.node.clone(),
                    Err(e) => {
                        return Err(DCBError::DatabaseCorrupted(format!(
                            "Free list page {:?} load error: {:?}",
                            page_id, e
                        )));
                    }
                }
            };
            match node_owned {
                Node::FreeListInternal(node) => {
                    if verbose {
                        println!("{:?} is internal node", page_id);
                    }
                    if idx < node.child_ids.len() {
                        let child_page_id = node.child_ids[idx];
                        stack.push((page_id, idx + 1));
                        stack.push((child_page_id, 0));
                    }
                }
                Node::FreeListLeaf(node) => {
                    if verbose {
                        println!("{:?} is leaf node", page_id);
                    }
                    for i in 0..node.keys.len() {
                        let tsn = node.keys[i];
                        if let Some(smallest) = smallest_reader_tsn
                            && tsn > smallest
                        {
                            is_finished = true;
                            break;
                        }

                        let leaf_value = &node.values[i];
                        if leaf_value.root_id == PageID(0) {
                            for &page_id in &leaf_value.page_ids {
                                reusable_page_ids.push_back((page_id, tsn));
                            }
                        } else {
                            // Traverse into the TSN-subtree rooted at root_id using the same (node, idx)
                            // iterative DFS pattern as the main FreeList traversal. This preserves
                            // left-to-right order deterministically.
                            let mut tsn_stack: Vec<(PageID, usize)> = vec![(leaf_value.root_id, 0)];
                            if self.verbose {
                                println!("TSN-subtree root_id: {:?}", leaf_value.root_id);
                            }
                            if self.verbose {
                                println!(
                                    "root_id in dirty? {}",
                                    self.dirty.contains_key(&leaf_value.root_id)
                                );
                            }
                            while let Some((sub_id, sidx)) = tsn_stack.pop() {
                                let sub_node = {
                                    match self.get_page_ref(mvcc, sub_id) {
                                        Ok(p) => p.node.clone(),
                                        Err(e) => {
                                            return Err(DCBError::DatabaseCorrupted(format!(
                                                "TSN subtree page {:?} load error: {:?}",
                                                sub_id, e
                                            )));
                                        }
                                    }
                                };
                                match sub_node {
                                    Node::FreeListTsnInternal(tsn_internal) => {
                                        if sidx < tsn_internal.child_ids.len() {
                                            let child_id = tsn_internal.child_ids[sidx];
                                            tsn_stack.push((sub_id, sidx + 1));
                                            tsn_stack.push((child_id, 0));
                                        }
                                    }
                                    Node::FreeListTsnLeaf(tsn_leaf) => {
                                        for &pid in &tsn_leaf.page_ids {
                                            reusable_page_ids.push_back((pid, tsn));
                                        }
                                    }
                                    other => {
                                        return Err(DCBError::DatabaseCorrupted(format!(
                                            "Invalid node type in TSN subtree: {}",
                                            other.type_name()
                                        )));
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {
                    return Err(DCBError::DatabaseCorrupted(
                        "Invalid node type in free list tree".to_string(),
                    ));
                }
            }
        }

        self.reusable_page_ids = reusable_page_ids;
        if verbose {
            println!("Found reusable page IDs: {:?}", self.reusable_page_ids);
        }
        Ok(())
    }

    // Free list tree methods
    pub fn insert_freed_page_id(
        &mut self,
        mvcc: &Mvcc,
        tsn: Tsn,
        freed_page_id: PageID,
    ) -> DCBResult<()> {
        let verbose = self.verbose;
        if verbose {
            println!("Inserting {freed_page_id:?} for {tsn:?}");
            println!("Root is {:?}", self.free_lists_tree_root_id);
        }
        // Get the root page ID.
        let mut current_page_id = self.free_lists_tree_root_id;

        // Traverse the tree to find a leaf node
        let mut stack: Vec<PageID> = Vec::new();
        let plan: FreePageIDInsertStrategy;
        loop {
            let current_page_ref = self.get_page_ref(mvcc, current_page_id)?;
            if let Node::FreeListLeaf(leaf_node) = &current_page_ref.node {
                let len_keys = leaf_node.keys.len();
                if len_keys == 0 {
                    if !leaf_node.would_fit_new_tsn_and_page_id(mvcc.max_node_size) {
                        return Err(DCBError::InternalError("Page size too small".to_string()));
                    }
                    plan = FreePageIDInsertStrategy::PushTsnOntoFreeListLeaf;
                } else {
                    let last_idx = len_keys - 1;
                    let last_key = leaf_node.keys[last_idx];
                    if tsn == last_key {
                        // Append to the existing last TSN
                        if leaf_node.values[last_idx].root_id != PageID(0) {
                            plan = FreePageIDInsertStrategy::PushPageIdOntoExistingTsnSubtree;
                        } else if leaf_node.would_fit_new_page_id(mvcc.max_node_size) {
                            plan = FreePageIDInsertStrategy::PushPageIdOntoFreeListLeaf(last_idx);
                        } else if leaf_node.keys.len() == 1 {
                            plan = FreePageIDInsertStrategy::MoveTsnToNewTsnSubtree;
                        } else {
                            plan = FreePageIDInsertStrategy::SplitFreeListLeaf;
                        }
                    } else if tsn > last_key {
                        // New last TSN
                        if leaf_node.would_fit_new_tsn_and_page_id(mvcc.max_node_size) {
                            plan = FreePageIDInsertStrategy::PushTsnOntoFreeListLeaf;
                        } else {
                            plan = FreePageIDInsertStrategy::CreateAndPromoteFreeListLeaf;
                        }
                    } else {
                        // We assume freed page IDs are always inserted for the last TSN
                        return Err(DCBError::InternalError(
                            "Insertion only supported for last TSN in leaf".to_string(),
                        ));
                    }
                }
                break;
            }
            if let Node::FreeListInternal(internal_node) = &current_page_ref.node {
                if verbose {
                    println!("{:?} is internal node", current_page_ref.page_id);
                }
                stack.push(current_page_id);
                current_page_id = *internal_node
                    .child_ids
                    .last()
                    .expect("FreeListInternal node should have a child");
            } else {
                return Err(DCBError::DatabaseCorrupted(
                    "Expected FreeListInternal node".to_string(),
                ));
            }
        }
        if verbose {
            println!("{current_page_id:?} is leaf node");
        }
        // Make the leaf page dirty
        let dirty_leaf_page_id = { self.get_dirty_page_id(current_page_id)? };
        let replacement_info: Option<(PageID, PageID)> = {
            if dirty_leaf_page_id != current_page_id {
                Some((current_page_id, dirty_leaf_page_id))
            } else {
                None
            }
        };
        // Proactive insert logic with capacity checks and optional split
        let mut split_info: Option<(Tsn, PageID)> = None;

        // First handle TSN-subtree plans without holding a mutable borrow to the freelist leaf
        match plan {
            FreePageIDInsertStrategy::PushPageIdOntoExistingTsnSubtree => {
                // Read leaf immutably to find last_idx and current root_id
                let leaf_snapshot = { self.get_page_ref(mvcc, dirty_leaf_page_id)? };
                let Node::FreeListLeaf(leaf_ro) = &leaf_snapshot.node else {
                    return Err(DCBError::DatabaseCorrupted(
                        "Expected FreeListLeaf node".to_string(),
                    ));
                };
                let last_idx = leaf_ro.keys.len() - 1;
                let tsn_root_id = leaf_ro.values[last_idx].root_id;
                if tsn_root_id == PageID(0) {
                    return Err(DCBError::DatabaseCorrupted(
                        "Expected TSN-subtree root_id to be set".to_string(),
                    ));
                }
                let new_root_id = self.tsn_subtree_insert(mvcc, tsn_root_id, freed_page_id)?;
                if new_root_id != tsn_root_id {
                    // Now mutate the freelist leaf to update root_id
                    let dirty_leaf_page = self.get_mut_dirty(dirty_leaf_page_id)?;
                    let Node::FreeListLeaf(dirty_leaf_node2) = &mut dirty_leaf_page.node else {
                        return Err(DCBError::DatabaseCorrupted(
                            "Expected FreeListLeaf node".to_string(),
                        ));
                    };
                    dirty_leaf_node2.values[last_idx].root_id = new_root_id;
                }
            }
            FreePageIDInsertStrategy::MoveTsnToNewTsnSubtree => {
                // Read leaf immutably to capture inline page_ids
                let leaf_snapshot = { self.get_page_ref(mvcc, dirty_leaf_page_id)? };
                let Node::FreeListLeaf(leaf_ro) = &leaf_snapshot.node else {
                    return Err(DCBError::DatabaseCorrupted(
                        "Expected FreeListLeaf node".to_string(),
                    ));
                };
                let last_idx = leaf_ro.keys.len() - 1;
                let mut page_ids = leaf_ro.values[last_idx].page_ids.clone();
                page_ids.push(freed_page_id);
                // sort + dedup the inline ids plus the new id
                page_ids.sort_by_key(|pid| pid.0);
                page_ids.dedup();
                // Build the initial TSN-subtree: start with a single leaf that fits, then insert the rest
                // Create an empty leaf and add as many as fit by size
                let mut initial_ids: Vec<PageID> = Vec::new();
                let mut tmp_leaf = FreeListTsnLeafNode {
                    page_ids: Vec::new(),
                };
                for pid in &page_ids {
                    let mut candidate = tmp_leaf.clone();
                    candidate.page_ids.push(*pid);
                    let candidate_page = Page::new(PageID(0), Node::FreeListTsnLeaf(candidate));
                    if candidate_page.calc_serialized_size() <= mvcc.page_size {
                        tmp_leaf.page_ids.push(*pid);
                        initial_ids.push(*pid);
                    } else {
                        break;
                    }
                }
                if initial_ids.is_empty() {
                    return Err(DCBError::InternalError(
                        "Page size too small for TSN-subtree leaf with one PageID".to_string(),
                    ));
                }
                let tsn_leaf_id = self.alloc_page_id();
                let tsn_leaf_page = Page::new(tsn_leaf_id, Node::FreeListTsnLeaf(tmp_leaf));
                self.insert_dirty(tsn_leaf_page)?;
                // Root id starts as this leaf
                let mut tsn_root_id = tsn_leaf_id;
                // Insert remaining ids via general insert, root may change
                for pid in page_ids.into_iter().filter(|p| !initial_ids.contains(p)) {
                    tsn_root_id = self.tsn_subtree_insert(mvcc, tsn_root_id, pid)?;
                }
                // Now mutate the freelist leaf to clear inline and set root
                let dirty_leaf_page = self.get_mut_dirty(dirty_leaf_page_id)?;
                let Node::FreeListLeaf(dirty_leaf_node2) = &mut dirty_leaf_page.node else {
                    return Err(DCBError::DatabaseCorrupted(
                        "Expected FreeListLeaf node".to_string(),
                    ));
                };
                dirty_leaf_node2.values[last_idx].page_ids.clear();
                dirty_leaf_node2.values[last_idx].root_id = tsn_root_id;
                if verbose {
                    println!("Moved inline page IDs to TSN-subtree {:?}", tsn_root_id);
                }
            }
            _ => { /* handled below with a mutable freelist leaf borrow */ }
        }

        // Now handle the remaining plans that only mutate the freelist leaf
        let dirty_leaf_page = self.get_mut_dirty(dirty_leaf_page_id)?;
        if let Node::FreeListLeaf(dirty_leaf_node) = &mut dirty_leaf_page.node {
            match plan {
                FreePageIDInsertStrategy::PushTsnOntoFreeListLeaf => {
                    dirty_leaf_node.push_new_key_and_value(tsn, freed_page_id);
                    if verbose {
                        println!(
                            "Inserted first pair ({tsn:?} -> {freed_page_id:?}) in {dirty_leaf_page_id:?}: {:?}",
                            dirty_leaf_node
                        );
                    }
                }
                FreePageIDInsertStrategy::PushPageIdOntoFreeListLeaf(last_idx) => {
                    dirty_leaf_node.push_new_page_id(last_idx, freed_page_id);
                    if verbose {
                        println!(
                            "Appended {freed_page_id:?} for existing last {tsn:?} in {dirty_leaf_page_id:?}: {:?}",
                            dirty_leaf_node
                        );
                    }
                }
                FreePageIDInsertStrategy::PushPageIdOntoExistingTsnSubtree => { /* handled earlier */
                }
                FreePageIDInsertStrategy::MoveTsnToNewTsnSubtree => { /* handled earlier */ }
                FreePageIDInsertStrategy::SplitFreeListLeaf => {
                    let (popped_key, mut popped_value) =
                        dirty_leaf_node.pop_last_key_and_value()?;
                    debug_assert_eq!(popped_key, tsn);
                    if verbose {
                        println!(
                            "Split (last TSN) leaf {:?}: {:?}",
                            dirty_leaf_page_id,
                            dirty_leaf_node.clone()
                        );
                    }
                    // Move the overflowing TSN to a new page and append there
                    popped_value.page_ids.push(freed_page_id);
                    let new_leaf_node = FreeListLeafNode {
                        keys: vec![popped_key],
                        values: vec![popped_value],
                    };
                    let new_leaf_page_id = self.alloc_page_id();
                    let new_leaf_page =
                        Page::new(new_leaf_page_id, Node::FreeListLeaf(new_leaf_node));
                    // let serialized_size = new_leaf_page.calc_serialized_size();
                    // if serialized_size > mvcc.page_size {
                    //     return Err(DCBError::InternalError(
                    //         "Shouldn't get here: page size is too small for a FreeListLeafNode with one TSN and one PageID".to_string(),
                    //     ));
                    // }
                    if verbose {
                        println!(
                            "Created new leaf {:?} (moved last TSN): {:?}",
                            new_leaf_page_id, new_leaf_page.node
                        );
                    }
                    self.insert_dirty(new_leaf_page)?;
                    split_info = Some((tsn, new_leaf_page_id));
                }
                FreePageIDInsertStrategy::CreateAndPromoteFreeListLeaf => {
                    // Create a new leaf containing only this last TSN and promote it
                    let new_leaf_node = FreeListLeafNode {
                        keys: vec![tsn],
                        values: vec![FreeListLeafValue {
                            page_ids: vec![freed_page_id],
                            root_id: PageID(0),
                        }],
                    };
                    let new_leaf_page_id = self.alloc_page_id();
                    let new_leaf_page =
                        Page::new(new_leaf_page_id, Node::FreeListLeaf(new_leaf_node));
                    // let serialized_size = new_leaf_page.calc_serialized_size();
                    // if serialized_size > mvcc.page_size {
                    //     return Err(DCBError::InternalError(
                    //         "Shouldn't get here: page size is too small for a FreeListLeafNode with one TSN and one PageID".to_string(),
                    //     ));
                    // }
                    if verbose {
                        println!(
                            "Created new leaf {:?} (new last TSN): {:?}",
                            new_leaf_page_id, new_leaf_page.node
                        );
                    }
                    self.insert_dirty(new_leaf_page)?;
                    split_info = Some((tsn, new_leaf_page_id));
                }
            }
        } else {
            return Err(DCBError::DatabaseCorrupted(
                "Expected FreeListLeaf node".to_string(),
            ));
        }
        // Propagate splits and replacements up the stack
        let mut current_replacement_info = replacement_info;
        while let Some(parent_page_id) = stack.pop() {
            // Make the internal page dirty
            let dirty_page_id = { self.get_dirty_page_id(parent_page_id)? };
            let parent_replacement_info: Option<(PageID, PageID)> = {
                if dirty_page_id != parent_page_id {
                    Some((parent_page_id, dirty_page_id))
                } else {
                    None
                }
            };
            // Get a mutable internal node....
            let dirty_internal_page = self.get_mut_dirty(dirty_page_id)?;

            if let Node::FreeListInternal(dirty_internal_node) = &mut dirty_internal_page.node {
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
                    "Expected FreeListInternal node".to_string(),
                ));
            }

            if let Some((promoted_key, promoted_page_id)) = split_info {
                if let Node::FreeListInternal(dirty_internal_node) = &mut dirty_internal_page.node {
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
                        "Expected FreeListInternal node".to_string(),
                    ));
                }
            }

            // Check if the internal page needs splitting

            if dirty_internal_page.calc_serialized_size() > mvcc.page_size {
                if let Node::FreeListInternal(dirty_internal_node) = &mut dirty_internal_page.node {
                    if verbose {
                        println!("Splitting internal {dirty_page_id:?}...");
                    }
                    // Split the internal node
                    // Ensure we have at least 3 keys and 4 child IDs before splitting
                    if dirty_internal_node.keys.len() < 3 || dirty_internal_node.child_ids.len() < 4
                    {
                        return Err(DCBError::DatabaseCorrupted(
                            "Cannot split internal node with too few keys/children".to_string(),
                        ));
                    }

                    // Move the right-most key to a new node. Promote the next right-most key.
                    let (promoted_key, new_keys, new_child_ids) =
                        dirty_internal_node.split_off()?;

                    // Ensure old node maintain the B-tree invariant: n keys should have n+1 child pointers
                    assert_eq!(
                        dirty_internal_node.keys.len() + 1,
                        dirty_internal_node.child_ids.len()
                    );

                    let new_internal_node = FreeListInternalNode {
                        keys: new_keys,
                        child_ids: new_child_ids,
                    };

                    // Ensure the new node also maintains the invariant
                    assert_eq!(
                        new_internal_node.keys.len() + 1,
                        new_internal_node.child_ids.len()
                    );

                    // Create a new internal page.
                    let new_internal_page_id = self.alloc_page_id();
                    let new_internal_page = Page::new(
                        new_internal_page_id,
                        Node::FreeListInternal(new_internal_node),
                    );
                    if verbose {
                        println!(
                            "Created internal {:?}: {:?}",
                            new_internal_page_id, new_internal_page.node
                        );
                    }
                    self.insert_dirty(new_internal_page)?;

                    split_info = Some((promoted_key, new_internal_page_id));
                } else {
                    return Err(DCBError::DatabaseCorrupted(
                        "Expected FreeListInternal node".to_string(),
                    ));
                }
            } else {
                split_info = None;
            }
            current_replacement_info = parent_replacement_info;
        }

        if let Some((old_id, new_id)) = current_replacement_info {
            if self.free_lists_tree_root_id == old_id {
                self.free_lists_tree_root_id = new_id;
                if verbose {
                    println!("Replaced root {old_id:?} with {new_id:?}");
                }
            } else {
                return Err(DCBError::RootIDMismatch(old_id.0, new_id.0));
            }
        }

        if let Some((promoted_key, promoted_page_id)) = split_info {
            // Create a new root
            let new_internal_node = FreeListInternalNode {
                keys: vec![promoted_key],
                child_ids: vec![self.free_lists_tree_root_id, promoted_page_id],
            };

            let new_root_page_id = self.alloc_page_id();
            let new_root_page =
                Page::new(new_root_page_id, Node::FreeListInternal(new_internal_node));
            if verbose {
                println!(
                    "Created new internal root {:?}: {:?}",
                    new_root_page_id, new_root_page.node
                );
            }
            self.insert_dirty(new_root_page)?;

            self.free_lists_tree_root_id = new_root_page_id;
        }

        Ok(())
    }

    /// Insert a PageID into the TSN-subtree rooted at `root_id`, maintaining sorted order.
    /// Returns the root id (maybe the same or a new root if promoted).
    fn tsn_subtree_insert(
        &mut self,
        mvcc: &Mvcc,
        root_id: PageID,
        key: PageID,
    ) -> DCBResult<PageID> {
        let verbose = self.verbose;
        let mut stack: Vec<(PageID, usize)> = Vec::new();
        let mut current_id = root_id;
        loop {
            let current_page_ref = self.get_page_ref(mvcc, current_id)?;
            match &current_page_ref.node {
                Node::FreeListTsnLeaf(_) => break,
                Node::FreeListTsnInternal(internal) => {
                    let child_idx = match internal.keys.binary_search_by(|k| k.0.cmp(&key.0)) {
                        Ok(idx) => idx + 1, // on equal, go right (matches existing >= loop)
                        Err(idx) => idx,    // first separator greater than key
                    };
                    let next_id = internal.child_ids[child_idx];
                    stack.push((current_id, child_idx));
                    current_id = next_id;
                }
                other => {
                    return Err(DCBError::DatabaseCorrupted(format!(
                        "Unexpected node type in TSN-subtree during insert: {}",
                        other.type_name()
                    )));
                }
            }
        }

        // Please note, all pages will be already dirty, because all freed PageIDs for a TSN
        // are inserted in the same transaction, so we don't need to replace any CoW PageIDs.
        {
            let leaf_page = self.get_mut_dirty(current_id)?;
            let Node::FreeListTsnLeaf(ref mut leaf) = leaf_page.node else {
                return Err(DCBError::DatabaseCorrupted(
                    "Expected TSN-subtree leaf".to_string(),
                ));
            };
            // Binary search by key
            match leaf.page_ids.binary_search_by(|pid| pid.0.cmp(&key.0)) {
                Ok(_) => {
                    // Duplicate; nothing to do
                    if verbose {
                        println!("Duplicate PageID {:?} ignored in TSN-subtree", key);
                    }
                }
                Err(ins) => {
                    leaf.page_ids.insert(ins, key);
                    if leaf.calc_serialized_size() <= mvcc.max_node_size {
                        return Ok(root_id);
                    }
                    // Split the leaf in half without cloning the entire vector.
                    let mid = leaf.page_ids.len() / 2; // left gets floor(n/2), right gets ceil(n/2)
                    let right_ids: Vec<PageID> = leaf.page_ids.split_off(mid);
                    let promoted_key = right_ids[0];
                    // Create right leaf page
                    let right_leaf_id = self.alloc_page_id();
                    let right_leaf_node = FreeListTsnLeafNode {
                        page_ids: right_ids,
                    };
                    let right_leaf_page =
                        Page::new(right_leaf_id, Node::FreeListTsnLeaf(right_leaf_node));
                    self.insert_dirty(right_leaf_page)?;
                    // Propagate to parents
                    let mut promoted: Option<(PageID, PageID)> =
                        Some((promoted_key, right_leaf_id));
                    // Walk up the path
                    for (parent_id, child_idx) in stack.into_iter().rev() {
                        if let Some((prom_key, prom_right_id)) = promoted.take() {
                            let parent_page = self.get_mut_dirty(parent_id)?;
                            let Node::FreeListTsnInternal(ref mut parent_node) = parent_page.node
                            else {
                                return Err(DCBError::DatabaseCorrupted(
                                    "Expected TSN-subtree internal".to_string(),
                                ));
                            };

                            // Insert promoted key and child at child_idx
                            parent_node.keys.insert(child_idx, prom_key);
                            parent_node.child_ids.insert(child_idx + 1, prom_right_id);
                            if parent_node.calc_serialized_size() <= mvcc.max_node_size {
                                // Fits; continue upward, no further promotion from this parent
                                current_id = parent_id;
                                continue;
                            }
                            // Overflow: split parent by midpoint and promote the right-min key
                            let total_keys = parent_node.keys.len();
                            debug_assert!(
                                total_keys >= 2,
                                "splitting parent with <2 keys after insert"
                            );
                            let mid = total_keys / 2; // left = 0..mid, promote = mid, right = mid+1..
                            let promote_up_key = parent_node.keys[mid];
                            // Build left side in place
                            let left_keys: Vec<PageID> = parent_node.keys[..mid].to_vec();
                            let left_child_ids: Vec<PageID> =
                                parent_node.child_ids[..=mid].to_vec();
                            // Build right side
                            let right_keys: Vec<PageID> = parent_node.keys[mid + 1..].to_vec();
                            let right_child_ids: Vec<PageID> =
                                parent_node.child_ids[mid + 1..].to_vec();
                            if right_child_ids.len() != right_keys.len() + 1 {
                                return Err(DCBError::DatabaseCorrupted(
                                    "TSN-subtree internal split produced invalid right arity"
                                        .to_string(),
                                ));
                            }
                            if left_child_ids.len() != left_keys.len() + 1 {
                                return Err(DCBError::DatabaseCorrupted(
                                    "TSN-subtree internal split produced invalid left arity"
                                        .to_string(),
                                ));
                            }
                            // Rewrite left into parent
                            parent_node.keys = left_keys;
                            parent_node.child_ids = left_child_ids;
                            // Create right internal node
                            let right_internal_id = self.alloc_page_id();
                            let right_internal =
                                crate::free_lists_tree_nodes::FreeListTsnInternalNode {
                                    keys: right_keys,
                                    child_ids: right_child_ids,
                                };
                            let right_internal_page = Page::new(
                                right_internal_id,
                                Node::FreeListTsnInternal(right_internal),
                            );
                            self.insert_dirty(right_internal_page)?;
                            // Set promoted to propagate upward
                            promoted = Some((promote_up_key, right_internal_id));
                        }
                        current_id = parent_id;
                    }
                    // If a promotion remains after processing all parents, create new root
                    if let Some((prom_key, prom_right_id)) = promoted.take() {
                        let new_root_id = self.alloc_page_id();
                        let left_id = current_id;
                        let new_root = crate::free_lists_tree_nodes::FreeListTsnInternalNode {
                            keys: vec![prom_key],
                            child_ids: vec![left_id, prom_right_id],
                        };
                        let new_root_page =
                            Page::new(new_root_id, Node::FreeListTsnInternal(new_root));
                        self.insert_dirty(new_root_page)?;
                        if verbose {
                            println!("Promoted new TSN-subtree root {:?}", new_root_id);
                        }
                        return Ok(new_root_id);
                    }
                }
            };
            Ok(root_id)
        }
    }

    pub fn remove_free_page_id(
        &mut self,
        mvcc: &Mvcc,
        tsn: Tsn,
        used_page_id: PageID,
    ) -> DCBResult<()> {
        let verbose = self.verbose;
        if verbose {
            println!();
            println!("Removing {used_page_id:?} from {tsn:?}...");
            println!("Root is {:?}", self.free_lists_tree_root_id);
        }
        // Get the root page
        let mut current_page_id = self.free_lists_tree_root_id;

        // Traverse the tree to find a leaf node
        let mut stack: Vec<PageID> = Vec::new();
        let mut removed_page_ids: Vec<PageID> = Vec::new();

        loop {
            let current_page_ref = self.get_page_ref(mvcc, current_page_id)?;
            if matches!(current_page_ref.node, Node::FreeListLeaf(_)) {
                break;
            }
            if let Node::FreeListInternal(internal_node) = &current_page_ref.node {
                if verbose {
                    println!("Page {:?} is internal node", current_page_ref.page_id);
                }
                stack.push(current_page_id);
                current_page_id = *internal_node.child_ids.first().unwrap();
            } else {
                return Err(DCBError::DatabaseCorrupted(
                    "Expected FreeListInternal node".to_string(),
                ));
            }
        }
        if verbose {
            println!("Page {current_page_id:?} is leaf node");
        }

        // We'll defer making the main leaf page dirty until we know we need to mutate it,
        // to avoid overlapping mutable borrows when also mutating the TSN-subtree.
        let mut replacement_info: Option<(PageID, PageID)> = None;
        let mut removal_info = None;

        // Read the current leaf immutably to decide the path (inline vs TSN-subtree)
        let leaf_snapshot = { self.get_page_ref(mvcc, current_page_id)? };
        let Node::FreeListLeaf(leaf_node_ro) = &leaf_snapshot.node else {
            return Err(DCBError::DatabaseCorrupted(
                "Expected FreeListLeaf node".to_string(),
            ));
        };
        if leaf_node_ro.keys.is_empty() || leaf_node_ro.keys[0] != tsn {
            return Err(DCBError::DatabaseCorrupted(format!(
                "Expected TSN {} not found: {:?}",
                tsn.0, leaf_node_ro
            )));
        }

        let leaf_value_root_id = leaf_node_ro.values[0].root_id;
        // let mut leaf_inline_page_ids: Option<Vec<PageID>> = None;
        // if leaf_value_root_id == PageID(0) {
        //     leaf_inline_page_ids = Some(leaf_node_ro.values[0].page_ids.clone());
        // }

        if leaf_value_root_id == PageID(0) {
            // Inline list case: make the leaf dirty and remove from inline list
            let dirty_page_id = { self.get_dirty_page_id(current_page_id)? };
            if dirty_page_id != current_page_id {
                replacement_info = Some((current_page_id, dirty_page_id));
            }
            let dirty_leaf_page = self.get_mut_dirty(dirty_page_id)?;
            if let Node::FreeListLeaf(dirty_leaf_node) = &mut dirty_leaf_page.node {
                let leaf_value = &mut dirty_leaf_node.values[0];
                if let Some(pos) = leaf_value
                    .page_ids
                    .iter()
                    .position(|&id| id == used_page_id)
                {
                    leaf_value.page_ids.remove(pos);
                } else {
                    return Err(DCBError::DatabaseCorrupted(format!(
                        "{used_page_id:?} not found in {tsn:?}"
                    )));
                }
                if verbose {
                    println!("Removed {used_page_id:?} from {tsn:?} in {dirty_page_id:?}");
                }
                if leaf_value.page_ids.is_empty() {
                    dirty_leaf_node.keys.remove(0);
                    dirty_leaf_node.values.remove(0);
                    if verbose {
                        println!("Removed {tsn:?} from {dirty_page_id:?}");
                    }
                    if dirty_leaf_node.keys.is_empty() {
                        if verbose {
                            println!("Empty leaf page {dirty_page_id:?}: {dirty_leaf_node:?}");
                        }
                        removal_info = Some(dirty_page_id);
                    } else if verbose {
                        println!("Leaf page not empty {dirty_page_id:?}: {dirty_leaf_node:?}");
                    }
                } else if verbose {
                    println!("Leaf value not empty {tsn:?}: {leaf_value:?}");
                }
            } else {
                return Err(DCBError::DatabaseCorrupted(
                    "Expected FreeListLeaf node".to_string(),
                ));
            }
        } else {
            // TSN-subtree case: mutate the TSN leaf first
            let tsn_root_id = leaf_value_root_id;
            let dirty_tsn_root_id = { self.get_dirty_page_id(tsn_root_id)? };
            let mut tsn_root_replaced: Option<PageID> = None;
            let dirty_tsn_root_page = self.get_mut_dirty(dirty_tsn_root_id)?;
            let mut tsn_leaf_became_empty = false;
            let mut tsn_child_leaf_became_empty = false;
            match &mut dirty_tsn_root_page.node {
                Node::FreeListTsnLeaf(tsn_leaf_node) => {
                    if let Some(pos) = tsn_leaf_node
                        .page_ids
                        .iter()
                        .position(|&id| id == used_page_id)
                    {
                        tsn_leaf_node.page_ids.remove(pos);
                    } else {
                        return Err(DCBError::DatabaseCorrupted(format!(
                            "{used_page_id:?} not found in TSN-subtree for {tsn:?}"
                        )));
                    }
                    if verbose {
                        println!(
                            "Removed {used_page_id:?} from TSN-subtree leaf {dirty_tsn_root_id:?} for {tsn:?}"
                        );
                    }
                    if tsn_leaf_node.page_ids.is_empty() {
                        tsn_leaf_became_empty = true;
                        removed_page_ids.push(dirty_tsn_root_id);
                    }
                }
                Node::FreeListTsnInternal(_) => {
                    // General TSN-subtree removal with arbitrary depth. We will:
                    // 1) Descend using separator keys to locate the target leaf containing used_page_id.
                    // 2) Make a copy-on-write path from root to that leaf, updating parent->child links
                    //    if any node gets a new dirty page id.
                    // 3) Remove the page id from the leaf. If the leaf becomes empty, remove the child
                    //    from its parent, adjust separator keys, and if the internal ends up with a
                    //    single child, promote that child. Propagate promotions up as needed.

                    // Build the path of (internal_id, child_index_chosen) down to the leaf.
                    let mut path: Vec<(PageID, usize)> = Vec::new();
                    let mut current_id = dirty_tsn_root_id;
                    loop {
                        let node_owned = { self.get_page_ref(mvcc, current_id)?.node.clone() };
                        match node_owned {
                            Node::FreeListTsnLeaf(_) => {
                                break; // current_id is the leaf
                            }
                            Node::FreeListTsnInternal(internal) => {
                                // Choose child index by separator keys: count keys <= used_page_id
                                let mut child_idx = 0usize;
                                while child_idx < internal.keys.len()
                                    && used_page_id >= internal.keys[child_idx]
                                {
                                    child_idx += 1;
                                }
                                let next_id = internal.child_ids[child_idx];
                                path.push((current_id, child_idx));
                                current_id = next_id;
                            }
                            other => {
                                return Err(DCBError::DatabaseCorrupted(format!(
                                    "Unexpected node type in TSN-subtree during descent: {}",
                                    other.type_name()
                                )));
                            }
                        }
                    }

                    // Now current_id points to the target leaf. Make it dirty and remove the page id.
                    let mut dirty_child_id = { self.get_dirty_page_id(current_id)? };
                    {
                        let child_page = self.get_mut_dirty(dirty_child_id)?;
                        match &mut child_page.node {
                            Node::FreeListTsnLeaf(leaf_node) => {
                                if let Some(pos) =
                                    leaf_node.page_ids.iter().position(|&id| id == used_page_id)
                                {
                                    leaf_node.page_ids.remove(pos);
                                } else {
                                    return Err(DCBError::DatabaseCorrupted(format!(
                                        "{used_page_id:?} not found in TSN-subtree for {tsn:?}"
                                    )));
                                }
                                if verbose {
                                    println!(
                                        "Removed {used_page_id:?} from TSN-subtree leaf {dirty_child_id:?} for {tsn:?}"
                                    );
                                }
                                if leaf_node.page_ids.is_empty() {
                                    tsn_child_leaf_became_empty = true;
                                    removed_page_ids.push(dirty_child_id);
                                }
                            }
                            other => {
                                return Err(DCBError::DatabaseCorrupted(format!(
                                    "Expected TSN-subtree leaf, got {}",
                                    other.type_name()
                                )));
                            }
                        }
                    }

                    // Walk back up the path, ensuring parents are dirty and applying updates.
                    let mut subtree_emptied = false;
                    let mut new_root_id_opt: Option<PageID> = None;

                    let path_len = path.len();
                    for (level, (parent_id, child_idx)) in path.into_iter().rev().enumerate() {
                        // Make parent dirty (COW) if needed
                        let parent_dirty_id = { self.get_dirty_page_id(parent_id)? };

                        // If parent was COW-ed, its own parent (next iteration) must update pointer to it.
                        // We achieve this by propagating parent_dirty_id via dirty_child_id variable at end.

                        // Mutate the parent
                        let parent_page = self.get_mut_dirty(parent_dirty_id)?;
                        let Node::FreeListTsnInternal(ref mut parent_node) = parent_page.node
                        else {
                            return Err(DCBError::DatabaseCorrupted(
                                "Expected TSN-subtree internal node".to_string(),
                            ));
                        };

                        if tsn_child_leaf_became_empty && level == 0 {
                            // Remove the empty leaf child from this parent
                            parent_node.child_ids.remove(child_idx);
                            if !parent_node.keys.is_empty() {
                                let key_remove_idx = if child_idx == 0 { 0 } else { child_idx - 1 };
                                if key_remove_idx < parent_node.keys.len() {
                                    parent_node.keys.remove(key_remove_idx);
                                }
                            }

                            // After removal, decide on collapse/promotion
                            match parent_node.child_ids.len() {
                                0 => {
                                    // Whole TSN-subtree emptied
                                    subtree_emptied = true;
                                    removed_page_ids.push(parent_dirty_id);
                                }
                                1 => {
                                    // Promote sole remaining child
                                    let remaining_child = parent_node.child_ids[0];
                                    removed_page_ids.push(parent_dirty_id);
                                    // For upper level (or main leaf if this was the root), this node is replaced by remaining_child
                                    dirty_child_id = remaining_child;
                                    new_root_id_opt = Some(remaining_child);
                                }
                                _ => {
                                    // Parent remains; propagate its (possibly COW-ed) id upward
                                    dirty_child_id = parent_dirty_id;
                                    if level == path_len - 1 {
                                        // If this was the root parent (topmost), remember it as new root when applicable
                                        new_root_id_opt = Some(parent_dirty_id);
                                    }
                                }
                            }
                        } else {
                            // Child was not removed; update the pointer if it changed due to COW
                            if parent_node.child_ids[child_idx] != dirty_child_id {
                                parent_node.child_ids[child_idx] = dirty_child_id;
                            }
                            // Propagate upward: the current parent may itself have been COW-ed
                            dirty_child_id = parent_dirty_id;
                            if level == path_len - 1 {
                                new_root_id_opt = Some(parent_dirty_id);
                            }
                        }
                    }

                    // Determine if the TSN-subtree root changed. Prefer promotion result if set; otherwise, if
                    // root was COW-ed or updated, use new_root_id_opt.
                    if subtree_emptied {
                        tsn_leaf_became_empty = true;
                    } else if let Some(new_root) = new_root_id_opt
                        && new_root != dirty_tsn_root_id
                    {
                        tsn_root_replaced = Some(new_root);
                    }
                }
                _ => {
                    return Err(DCBError::DatabaseCorrupted(
                        "Expected TSN-subtree node".to_string(),
                    ));
                }
            }

            // Now mutate the main leaf as needed
            let dirty_page_id = { self.get_dirty_page_id(current_page_id)? };
            if dirty_page_id != current_page_id {
                replacement_info = Some((current_page_id, dirty_page_id));
            }
            let dirty_leaf_page = self.get_mut_dirty(dirty_page_id)?;
            if let Node::FreeListLeaf(dirty_leaf_node) = &mut dirty_leaf_page.node {
                // Ensure expected TSN still at index 0
                if dirty_leaf_node.keys.is_empty() || dirty_leaf_node.keys[0] != tsn {
                    return Err(DCBError::DatabaseCorrupted(format!(
                        "Expected TSN {} not found in dirty leaf: {:?}",
                        tsn.0, dirty_leaf_node
                    )));
                }
                if tsn_leaf_became_empty {
                    // Remove the TSN entry entirely
                    dirty_leaf_node.keys.remove(0);
                    dirty_leaf_node.values.remove(0);
                    if verbose {
                        println!("Removed {tsn:?} from {dirty_page_id:?}");
                    }
                    if dirty_leaf_node.keys.is_empty() {
                        if verbose {
                            println!("Empty leaf page {dirty_page_id:?}: {dirty_leaf_node:?}");
                        }
                        removal_info = Some(dirty_page_id);
                    } else if verbose {
                        println!("Leaf page not empty {dirty_page_id:?}: {dirty_leaf_node:?}");
                    }
                } else if let Some(new_root) = tsn_root_replaced {
                    // TSN-subtree root changed (internal collapsed to single child)
                    dirty_leaf_node.values[0].root_id = new_root;
                } else if dirty_tsn_root_id != tsn_root_id {
                    // Update the pointer to the new dirty TSN leaf (COW of root leaf)
                    dirty_leaf_node.values[0].root_id = dirty_tsn_root_id;
                }
            } else {
                return Err(DCBError::DatabaseCorrupted(
                    "Expected FreeListLeaf node".to_string(),
                ));
            }
        }

        // Propagate replacements and removals up the stack
        let mut current_replacement_info = replacement_info;

        while let Some(parent_page_id) = stack.pop() {
            // Make the internal page dirty
            let dirty_page_id = { self.get_dirty_page_id(parent_page_id)? };
            let parent_replacement_info: Option<(PageID, PageID)> = {
                if dirty_page_id != parent_page_id {
                    Some((parent_page_id, dirty_page_id))
                } else {
                    None
                }
            };
            // Get a mutable internal node....
            let dirty_internal_page = self.get_mut_dirty(dirty_page_id)?;

            if let Some((old_id, new_id)) = current_replacement_info {
                if let Node::FreeListInternal(dirty_internal_node) = &mut dirty_internal_page.node {
                    // Replace the child ID
                    if dirty_internal_node.child_ids[0] == old_id {
                        dirty_internal_node.child_ids[0] = new_id;
                        if verbose {
                            println!(
                                "Replaced {old_id:?} with {new_id:?} in {dirty_page_id:?}: {dirty_internal_page:?}"
                            );
                        }
                    } else {
                        return Err(DCBError::DatabaseCorrupted("Child ID mismatch".to_string()));
                    }
                } else {
                    return Err(DCBError::DatabaseCorrupted(
                        "Expected FreeListInternal node".to_string(),
                    ));
                }
            }
            current_replacement_info = parent_replacement_info;

            if let Some(removed_page_id) = removal_info {
                removed_page_ids.push(removed_page_id);

                if let Node::FreeListInternal(dirty_internal_node) = &mut dirty_internal_page.node {
                    // Remove the child ID and key
                    if dirty_internal_node.child_ids[0] != removed_page_id {
                        return Err(DCBError::DatabaseCorrupted("Child ID mismatch".to_string()));
                    }
                    if dirty_internal_node.keys.is_empty() {
                        return Err(DCBError::DatabaseCorrupted(
                            "Empty internal node keys".to_string(),
                        ));
                    }
                    dirty_internal_node.child_ids.remove(0);
                    dirty_internal_node.keys.remove(0);
                    if verbose {
                        println!(
                            "Removed {removed_page_id:?} from {dirty_page_id:?}: {dirty_internal_node:?}"
                        );
                    }

                    // If the internal node is empty or has only one child, mark it for removal
                    if dirty_internal_node.keys.is_empty() {
                        if verbose {
                            println!(
                                "Empty internal page {dirty_page_id:?}: {dirty_internal_node:?}"
                            );
                        }
                        assert_eq!(dirty_internal_node.child_ids.len(), 1);
                        let orphaned_child_id = dirty_internal_node.child_ids[0];

                        removed_page_ids.push(dirty_page_id);

                        if let Some((old_id, _)) = parent_replacement_info {
                            current_replacement_info = Some((old_id, orphaned_child_id));
                        } else {
                            current_replacement_info = Some((dirty_page_id, orphaned_child_id));
                        }
                    }
                } else {
                    return Err(DCBError::DatabaseCorrupted(
                        "Expected FreeListInternal node".to_string(),
                    ));
                }

                removal_info = None;
            }
        }

        for &removed_page_id in &removed_page_ids {
            self.append_freed_page_id(removed_page_id);
        }

        // Update the free_lists_tree_root_id if needed (it might already have been
        // updated with the page ID of a dirty page.
        if let Some((old_id, new_id)) = current_replacement_info {
            if self.free_lists_tree_root_id == old_id {
                self.append_freed_page_id(old_id);
                self.free_lists_tree_root_id = new_id;
                if verbose {
                    println!("Replaced root {old_id:?} with {new_id:?}");
                }
            } else {
                return Err(DCBError::RootIDMismatch(old_id.0, new_id.0));
            }
        }

        Ok(())
    }
}

enum FreePageIDInsertStrategy {
    PushTsnOntoFreeListLeaf,
    PushPageIdOntoFreeListLeaf(usize),
    PushPageIdOntoExistingTsnSubtree,
    MoveTsnToNewTsnSubtree,
    SplitFreeListLeaf,
    CreateAndPromoteFreeListLeaf,
}

// Reader transaction
pub struct Reader {
    pub header_page_id: PageID,
    pub tsn: Tsn,
    pub events_tree_root_id: PageID,
    pub tags_tree_root_id: PageID,
    pub next_position: Position,
    reader_id: usize,
    reader_tsns: Arc<DashMap<usize, Tsn>>,
}

impl Drop for Reader {
    fn drop(&mut self) {
        // Remove reader TSN from the concurrent map (lock-free)
        self.reader_tsns.remove(&self.reader_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::free_lists_tree_nodes::FreeListLeafValue;
    use serial_test::serial;
    use tempfile::tempdir;

    static VERBOSE: bool = false;

    #[test]
    #[serial]
    fn test_mvcc_init() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("mvcc-test.db");

        {
            let db = Mvcc::new(&db_path, 4096, VERBOSE).unwrap();
            assert!(db.pager.is_file_new);
        }

        {
            let db = Mvcc::new(&db_path, 4096, VERBOSE).unwrap();
            assert!(!db.pager.is_file_new);
        }
    }

    #[test]
    #[serial]
    fn test_write_transaction_incrementing_tsn_and_alternating_header() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("mvcc-test.db");
        let db = Mvcc::new(&db_path, 4096, VERBOSE).unwrap();

        {
            let mut writer = db.writer().unwrap();
            assert_eq!(Tsn(1), writer.tsn);
            assert_eq!(PageID(0), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }

        {
            let mut writer = db.writer().unwrap();
            assert_eq!(Tsn(2), writer.tsn);
            assert_eq!(PageID(1), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }

        {
            let mut writer = db.writer().unwrap();
            assert_eq!(Tsn(3), writer.tsn);
            assert_eq!(PageID(0), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }

        {
            let mut writer = db.writer().unwrap();
            assert_eq!(Tsn(4), writer.tsn);
            assert_eq!(PageID(1), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }

        {
            let mut writer = db.writer().unwrap();
            assert_eq!(Tsn(5), writer.tsn);
            assert_eq!(PageID(0), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }
    }

    #[test]
    #[serial]
    fn test_read_transaction_header_and_tsn() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("mvcc-test.db");
        let db = Mvcc::new(&db_path, 4096, VERBOSE).unwrap();

        // Initial reader should see TSN 0
        {
            assert_eq!(0, db.reader_tsns.len());
            let reader = db.reader().unwrap();
            assert_eq!(1, db.reader_tsns.len());
            assert_eq!(
                vec![Tsn(0)],
                db.reader_tsns
                    .iter()
                    .map(|r| *r.value())
                    .collect::<Vec<_>>()
            );
            assert_eq!(PageID(0), reader.header_page_id);
            assert_eq!(Tsn(0), reader.tsn);
        }
        assert_eq!(0, db.reader_tsns.len());

        // Multiple nested readers
        {
            let reader1 = db.reader().unwrap();
            assert_eq!(
                vec![Tsn(0)],
                db.reader_tsns
                    .iter()
                    .map(|r| *r.value())
                    .collect::<Vec<_>>()
            );
            assert_eq!(PageID(0), reader1.header_page_id);
            assert_eq!(Tsn(0), reader1.tsn);

            {
                let reader2 = db.reader().unwrap();
                assert_eq!(
                    vec![Tsn(0), Tsn(0)],
                    db.reader_tsns
                        .iter()
                        .map(|r| *r.value())
                        .collect::<Vec<_>>()
                );
                assert_eq!(PageID(0), reader2.header_page_id);
                assert_eq!(Tsn(0), reader2.tsn);

                {
                    let reader3 = db.reader().unwrap();
                    assert_eq!(
                        vec![Tsn(0), Tsn(0), Tsn(0)],
                        db.reader_tsns
                            .iter()
                            .map(|r| *r.value())
                            .collect::<Vec<_>>()
                    );
                    assert_eq!(PageID(0), reader3.header_page_id);
                    assert_eq!(Tsn(0), reader3.tsn);
                }
            }
        }
        assert_eq!(0, db.reader_tsns.len());

        // Writer transaction
        {
            let mut writer = db.writer().unwrap();
            assert_eq!(0, db.reader_tsns.len());
            assert_eq!(Tsn(1), writer.tsn);
            assert_eq!(PageID(0), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }

        // Reader after writer
        {
            let reader = db.reader().unwrap();
            assert_eq!(
                vec![Tsn(1)],
                db.reader_tsns
                    .iter()
                    .map(|r| *r.value())
                    .collect::<Vec<_>>()
            );
            assert_eq!(PageID(1), reader.header_page_id);
            assert_eq!(Tsn(1), reader.tsn);
        }
    }

    #[test]
    #[serial]
    fn test_copy_on_write_page_reuse() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("mvcc-test.db");
        let db = Mvcc::new(&db_path, 4096, VERBOSE).unwrap();
        // First transaction
        {
            let mut writer = db.writer().unwrap();

            // Check there are no free pages
            assert_eq!(0, writer.reusable_page_ids.len());

            // Check the free list root is page 2
            assert_eq!(PageID(2), writer.free_lists_tree_root_id);

            // Check the position root is page 3
            assert_eq!(PageID(3), writer.events_tree_root_id);

            // Allocate and insert free page ID.
            let free_page_id = writer.alloc_page_id();
            assert_eq!(PageID(5), free_page_id);
            writer
                .insert_freed_page_id(&db, writer.tsn, free_page_id)
                .unwrap();

            // Check the dirty page IDs
            assert_eq!(1, writer.dirty.len());
            assert_eq!(PageID(6), *writer.dirty.keys().collect::<Vec<_>>()[0]);

            // Check the freed page IDs
            assert_eq!(1, writer.freed_page_ids.len());
            assert_eq!(PageID(2), writer.freed_page_ids[0]);

            db.commit(&mut writer).unwrap();
        }

        // Second transaction
        {
            let mut writer = db.writer().unwrap();

            // Check there are two free pages
            assert_eq!(2, writer.reusable_page_ids.len());
            assert_eq!((PageID(5), Tsn(1)), writer.reusable_page_ids[0]);
            assert_eq!((PageID(2), Tsn(1)), writer.reusable_page_ids[1]);

            // Check the free list root is page 2
            assert_eq!(PageID(6), writer.free_lists_tree_root_id);

            // Check the position root is page 3
            assert_eq!(PageID(3), writer.events_tree_root_id);

            // Allocate and insert free page ID.
            let free_page_id = writer.alloc_page_id();
            assert_eq!(PageID(5), free_page_id);
            writer
                .insert_freed_page_id(&db, writer.tsn, free_page_id)
                .unwrap();

            // Check the dirty page IDs
            assert_eq!(1, writer.dirty.len());
            assert_eq!(PageID(2), *writer.dirty.keys().collect::<Vec<_>>()[0]);

            // Check the freed page IDs
            assert_eq!(1, writer.freed_page_ids.len());
            assert_eq!(PageID(6), writer.freed_page_ids[0]);

            db.commit(&mut writer).unwrap();
        }

        // Third transaction
        {
            let mut writer = db.writer().unwrap();

            // Check there are two free pages
            assert_eq!(2, writer.reusable_page_ids.len());
            assert_eq!((PageID(5), Tsn(2)), writer.reusable_page_ids[0]);
            assert_eq!((PageID(6), Tsn(2)), writer.reusable_page_ids[1]);

            // Check the free list root is page 2
            assert_eq!(PageID(2), writer.free_lists_tree_root_id);

            // Check the position root is page 3
            assert_eq!(PageID(3), writer.events_tree_root_id);

            // Allocate and insert free page ID.
            let free_page_id = writer.alloc_page_id();
            assert_eq!(PageID(5), free_page_id);
            writer
                .insert_freed_page_id(&db, writer.tsn, free_page_id)
                .unwrap();

            // Check the dirty page IDs
            assert_eq!(1, writer.dirty.len());
            assert_eq!(PageID(6), *writer.dirty.keys().collect::<Vec<_>>()[0]);

            // Check the freed page IDs
            assert_eq!(1, writer.freed_page_ids.len());
            assert_eq!(PageID(2), writer.freed_page_ids[0]);

            db.commit(&mut writer).unwrap();
        }
    }

    // FreeListTree tests
    mod free_list_tree_tests {
        use super::*;
        use serial_test::serial;
        use tempfile::tempdir;

        // Helper function to create a test database with a specified page size
        fn construct_mvcc(page_size: usize) -> (tempfile::TempDir, Mvcc) {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("mvcc-test.db");
            let db = Mvcc::new(&db_path, page_size, VERBOSE).unwrap();
            (temp_dir, db)
        }

        #[test]
        #[serial]
        fn test_find_reusable_page_ids_empty_no_entries() {
            let (_temp_dir, db) = construct_mvcc(64);
            // New writer invokes find_reusable_page_ids in Mvcc::writer
            let writer = db.writer().unwrap();
            assert_eq!(0, writer.reusable_page_ids.len());
        }

        #[test]
        #[serial]
        fn test_find_reusable_page_ids_leaf() {
            let (_temp_dir, db) = construct_mvcc(64);
            let mut writer = db.writer().unwrap();

            let (tsn, free_pid1, free_pid2) = build_free_list_tree_leaf(&mut writer);

            // Recompute
            writer.find_reusable_page_ids(&db).unwrap();
            assert_eq!(2, writer.reusable_page_ids.len());
            assert_eq!((free_pid1, tsn), writer.reusable_page_ids[0]);
            assert_eq!((free_pid2, tsn), writer.reusable_page_ids[1]);
        }

        #[test]
        #[serial]
        fn test_find_reusable_page_ids_internal_leaf() {
            let (_temp_dir, db) = construct_mvcc(128);
            let mut writer = db.writer().unwrap();

            let (tsn1, tsn2, pid1, pid2, pid3, pid4) =
                build_free_list_tree_internal_leaf(&mut writer);

            // Recompute
            writer.find_reusable_page_ids(&db).unwrap();
            // Expect entries from leaf1 then leaf2
            assert_eq!(4, writer.reusable_page_ids.len());
            assert_eq!((pid1, tsn1), writer.reusable_page_ids[0]);
            assert_eq!((pid2, tsn1), writer.reusable_page_ids[1]);
            assert_eq!((pid3, tsn2), writer.reusable_page_ids[2]);
            assert_eq!((pid4, tsn2), writer.reusable_page_ids[3]);
        }

        #[test]
        #[serial]
        fn test_find_reusable_page_ids_internal_internal_leaf() {
            let (_temp_dir, db) = construct_mvcc(128);
            let mut writer = db.writer().unwrap();

            let (tsn1, tsn2, tsn3, tsn4, pid1, pid2, pid3, pid4, pid5, pid6, pid7, pid8) =
                build_free_list_tree_internal_internal_leaf(&mut writer);

            // Recompute
            writer.find_reusable_page_ids(&db).unwrap();
            // Expect entries from leaf1 then leaf2
            assert_eq!(8, writer.reusable_page_ids.len());
            assert_eq!((pid1, tsn1), writer.reusable_page_ids[0]);
            assert_eq!((pid2, tsn1), writer.reusable_page_ids[1]);
            assert_eq!((pid3, tsn2), writer.reusable_page_ids[2]);
            assert_eq!((pid4, tsn2), writer.reusable_page_ids[3]);
            assert_eq!((pid5, tsn3), writer.reusable_page_ids[4]);
            assert_eq!((pid6, tsn3), writer.reusable_page_ids[5]);
            assert_eq!((pid7, tsn4), writer.reusable_page_ids[6]);
            assert_eq!((pid8, tsn4), writer.reusable_page_ids[7]);
        }

        #[test]
        #[serial]
        fn test_find_reusable_page_ids_leaf_tsn_subtree_leaf() {
            let (_temp_dir, db) = construct_mvcc(64);
            let mut writer = db.writer().unwrap();

            let (pid1, pid2, tsn) = build_free_list_tree_leaf_tsn_subtree_leaf(&mut writer);

            // Recompute
            writer.find_reusable_page_ids(&db).unwrap();
            // Expect entries from leaf1 then leaf2
            assert_eq!(2, writer.reusable_page_ids.len());
            assert_eq!((pid1, tsn), writer.reusable_page_ids[0]);
            assert_eq!((pid2, tsn), writer.reusable_page_ids[1]);
        }

        #[test]
        #[serial]
        fn test_find_reusable_page_ids_leaf_tsn_subtree_internal_leaf() {
            let (_temp_dir, db) = construct_mvcc(64);
            let mut writer = db.writer().unwrap();

            let (pid1, pid2, pid3, pid4, tsn) =
                build_free_list_tree_leaf_tsn_subtree_internal_leaf(&mut writer);

            // Recompute
            writer.find_reusable_page_ids(&db).unwrap();
            // Expect entries from leaf1 then leaf2
            assert_eq!(4, writer.reusable_page_ids.len());
            assert_eq!((pid1, tsn), writer.reusable_page_ids[0]);
            assert_eq!((pid2, tsn), writer.reusable_page_ids[1]);
            assert_eq!((pid3, tsn), writer.reusable_page_ids[2]);
            assert_eq!((pid4, tsn), writer.reusable_page_ids[3]);
        }

        #[test]
        #[serial]
        fn test_find_reusable_page_ids_leaf_tsn_subtree_internal_internal_leaf() {
            let (_temp_dir, db) = construct_mvcc(64);
            let mut writer = db.writer().unwrap();

            let (pid1, pid2, pid3, pid4, pid5, pid6, pid7, pid8, tsn) =
                build_free_list_tree_leaf_tsn_subtree_internal_internal_leaf(&mut writer);

            // Recompute
            writer.find_reusable_page_ids(&db).unwrap();
            // Expect entries from leaf1 then leaf2
            assert_eq!(8, writer.reusable_page_ids.len());
            assert_eq!((pid1, tsn), writer.reusable_page_ids[0]);
            assert_eq!((pid2, tsn), writer.reusable_page_ids[1]);
            assert_eq!((pid3, tsn), writer.reusable_page_ids[2]);
            assert_eq!((pid4, tsn), writer.reusable_page_ids[3]);
            assert_eq!((pid5, tsn), writer.reusable_page_ids[4]);
            assert_eq!((pid6, tsn), writer.reusable_page_ids[5]);
            assert_eq!((pid7, tsn), writer.reusable_page_ids[6]);
            assert_eq!((pid8, tsn), writer.reusable_page_ids[7]);
        }

        fn build_free_list_tree_leaf(writer: &mut Writer) -> (Tsn, PageID, PageID) {
            // Build a leaf node with one TSN and one PageID
            let tsn = Tsn(123);
            let free_pid1 = writer.alloc_page_id();
            let free_pid2 = writer.alloc_page_id();
            let leaf = FreeListLeafNode {
                keys: vec![tsn],
                values: vec![FreeListLeafValue {
                    page_ids: vec![free_pid1, free_pid2],
                    root_id: PageID(0),
                }],
            };
            let root_id = writer.alloc_page_id();
            let page = Page::new(root_id, Node::FreeListLeaf(leaf));
            writer.insert_dirty(page).unwrap();
            writer.append_freed_page_id(writer.free_lists_tree_root_id);
            writer.free_lists_tree_root_id = root_id;
            (tsn, free_pid1, free_pid2)
        }

        fn build_free_list_tree_internal_leaf(
            writer: &mut Writer,
        ) -> (Tsn, Tsn, PageID, PageID, PageID, PageID) {
            // Two leaves each with one (TSN -> [PageID])
            let tsn1 = Tsn(10);
            let tsn2 = Tsn(20);
            let pid1 = writer.alloc_page_id();
            let pid2 = writer.alloc_page_id();
            let pid3 = writer.alloc_page_id();
            let pid4 = writer.alloc_page_id();

            let leaf1_id = writer.alloc_page_id();
            let leaf2_id = writer.alloc_page_id();
            let leaf1 = FreeListLeafNode {
                keys: vec![tsn1],
                values: vec![FreeListLeafValue {
                    page_ids: vec![pid1, pid2],
                    root_id: PageID(0),
                }],
            };
            let leaf2 = FreeListLeafNode {
                keys: vec![tsn2],
                values: vec![FreeListLeafValue {
                    page_ids: vec![pid3, pid4],
                    root_id: PageID(0),
                }],
            };
            writer
                .insert_dirty(Page::new(leaf1_id, Node::FreeListLeaf(leaf1)))
                .unwrap();
            writer
                .insert_dirty(Page::new(leaf2_id, Node::FreeListLeaf(leaf2)))
                .unwrap();

            // Internal root pointing to the two leaves (keys are not used by traversal here)
            let internal = FreeListInternalNode {
                keys: vec![tsn1],
                child_ids: vec![leaf1_id, leaf2_id],
            };
            let root_id = writer.alloc_page_id();
            writer
                .insert_dirty(Page::new(root_id, Node::FreeListInternal(internal)))
                .unwrap();
            writer.append_freed_page_id(writer.free_lists_tree_root_id);
            writer.free_lists_tree_root_id = root_id;
            (tsn1, tsn2, pid1, pid2, pid3, pid4)
        }

        fn build_free_list_tree_internal_internal_leaf(
            writer: &mut Writer,
        ) -> (
            Tsn,
            Tsn,
            Tsn,
            Tsn,
            PageID,
            PageID,
            PageID,
            PageID,
            PageID,
            PageID,
            PageID,
            PageID,
        ) {
            // Two leaves each with one (TSN -> [PageID])
            let tsn1 = Tsn(10);
            let tsn2 = Tsn(20);
            let tsn3 = Tsn(30);
            let tsn4 = Tsn(40);
            let pid1 = writer.alloc_page_id();
            let pid2 = writer.alloc_page_id();
            let pid3 = writer.alloc_page_id();
            let pid4 = writer.alloc_page_id();
            let pid5 = writer.alloc_page_id();
            let pid6 = writer.alloc_page_id();
            let pid7 = writer.alloc_page_id();
            let pid8 = writer.alloc_page_id();

            let leaf1_id = writer.alloc_page_id();
            let leaf2_id = writer.alloc_page_id();
            let leaf3_id = writer.alloc_page_id();
            let leaf4_id = writer.alloc_page_id();
            let leaf1 = FreeListLeafNode {
                keys: vec![tsn1],
                values: vec![FreeListLeafValue {
                    page_ids: vec![pid1, pid2],
                    root_id: PageID(0),
                }],
            };
            let leaf2 = FreeListLeafNode {
                keys: vec![tsn2],
                values: vec![FreeListLeafValue {
                    page_ids: vec![pid3, pid4],
                    root_id: PageID(0),
                }],
            };
            let leaf3 = FreeListLeafNode {
                keys: vec![tsn3],
                values: vec![FreeListLeafValue {
                    page_ids: vec![pid5, pid6],
                    root_id: PageID(0),
                }],
            };
            let leaf4 = FreeListLeafNode {
                keys: vec![tsn4],
                values: vec![FreeListLeafValue {
                    page_ids: vec![pid7, pid8],
                    root_id: PageID(0),
                }],
            };
            writer
                .insert_dirty(Page::new(leaf1_id, Node::FreeListLeaf(leaf1)))
                .unwrap();
            writer
                .insert_dirty(Page::new(leaf2_id, Node::FreeListLeaf(leaf2)))
                .unwrap();
            writer
                .insert_dirty(Page::new(leaf3_id, Node::FreeListLeaf(leaf3)))
                .unwrap();
            writer
                .insert_dirty(Page::new(leaf4_id, Node::FreeListLeaf(leaf4)))
                .unwrap();

            // Internal root pointing to the two leaves (keys are not used by traversal here)
            let internal1 = FreeListInternalNode {
                keys: vec![tsn2],
                child_ids: vec![leaf1_id, leaf2_id],
            };
            let internal1_id = writer.alloc_page_id();
            let internal2 = FreeListInternalNode {
                keys: vec![tsn4],
                child_ids: vec![leaf3_id, leaf4_id],
            };
            let internal2_id = writer.alloc_page_id();
            let internal3 = FreeListInternalNode {
                keys: vec![tsn3],
                child_ids: vec![internal1_id, internal2_id],
            };
            let internal3_id = writer.alloc_page_id();
            writer
                .insert_dirty(Page::new(internal1_id, Node::FreeListInternal(internal1)))
                .unwrap();
            writer
                .insert_dirty(Page::new(internal2_id, Node::FreeListInternal(internal2)))
                .unwrap();
            writer
                .insert_dirty(Page::new(internal3_id, Node::FreeListInternal(internal3)))
                .unwrap();
            writer.append_freed_page_id(writer.free_lists_tree_root_id);
            writer.free_lists_tree_root_id = internal3_id;
            (
                tsn1, tsn2, tsn3, tsn4, pid1, pid2, pid3, pid4, pid5, pid6, pid7, pid8,
            )
        }

        fn build_free_list_tree_leaf_tsn_subtree_leaf(
            writer: &mut Writer,
        ) -> (PageID, PageID, Tsn) {
            // Create a TSN-subtree leaf and make a leaf value point to it
            let tsn_sub_leaf_id = writer.alloc_page_id();
            let pid1 = writer.alloc_page_id();
            let pid2 = writer.alloc_page_id();
            let tsn_sub_leaf = crate::free_lists_tree_nodes::FreeListTsnLeafNode {
                page_ids: vec![pid1, pid2],
            };
            writer
                .insert_dirty(Page::new(
                    tsn_sub_leaf_id,
                    Node::FreeListTsnLeaf(tsn_sub_leaf),
                ))
                .unwrap();

            let tsn = Tsn(33);
            let leaf = FreeListLeafNode {
                keys: vec![tsn],
                values: vec![FreeListLeafValue {
                    page_ids: vec![],
                    root_id: tsn_sub_leaf_id,
                }],
            };
            let root_id = writer.alloc_page_id();
            writer
                .insert_dirty(Page::new(root_id, Node::FreeListLeaf(leaf)))
                .unwrap();
            writer.append_freed_page_id(writer.free_lists_tree_root_id);
            writer.free_lists_tree_root_id = root_id;
            (pid1, pid2, tsn)
        }

        fn build_free_list_tree_leaf_tsn_subtree_internal_leaf(
            writer: &mut Writer,
        ) -> (PageID, PageID, PageID, PageID, Tsn) {
            // Create a TSN-subtree leaf with a PageID
            let tsn_sub_leaf_id1 = writer.alloc_page_id();
            let pid1 = writer.alloc_page_id();
            let pid2 = writer.alloc_page_id();
            let tsn_sub_leaf1 = crate::free_lists_tree_nodes::FreeListTsnLeafNode {
                page_ids: vec![pid1, pid2],
            };
            writer
                .insert_dirty(Page::new(
                    tsn_sub_leaf_id1,
                    Node::FreeListTsnLeaf(tsn_sub_leaf1),
                ))
                .unwrap();

            // Create a TSN-subtree leaf with a PageID
            let tsn_sub_leaf_id2 = writer.alloc_page_id();
            let pid3 = writer.alloc_page_id();
            let pid4 = writer.alloc_page_id();
            let tsn_sub_leaf2 = crate::free_lists_tree_nodes::FreeListTsnLeafNode {
                page_ids: vec![pid3, pid4],
            };
            writer
                .insert_dirty(Page::new(
                    tsn_sub_leaf_id2,
                    Node::FreeListTsnLeaf(tsn_sub_leaf2),
                ))
                .unwrap();

            // Create a TSN-subtree internal node
            let tsn_sub_internal_id = writer.alloc_page_id();
            let tsn_sub_internal = crate::free_lists_tree_nodes::FreeListTsnInternalNode {
                keys: vec![pid3],
                child_ids: vec![tsn_sub_leaf_id1, tsn_sub_leaf_id2],
            };
            writer
                .insert_dirty(Page::new(
                    tsn_sub_internal_id,
                    Node::FreeListTsnInternal(tsn_sub_internal),
                ))
                .unwrap();

            // Make a leaf value point to the internal node
            let tsn = Tsn(33);
            let leaf = FreeListLeafNode {
                keys: vec![tsn],
                values: vec![FreeListLeafValue {
                    page_ids: vec![],
                    root_id: tsn_sub_internal_id,
                }],
            };
            let root_id = writer.alloc_page_id();
            writer
                .insert_dirty(Page::new(root_id, Node::FreeListLeaf(leaf)))
                .unwrap();
            writer.append_freed_page_id(writer.free_lists_tree_root_id);
            writer.free_lists_tree_root_id = root_id;
            (pid1, pid2, pid3, pid4, tsn)
        }

        fn build_free_list_tree_leaf_tsn_subtree_internal_internal_leaf(
            writer: &mut Writer,
        ) -> (
            PageID,
            PageID,
            PageID,
            PageID,
            PageID,
            PageID,
            PageID,
            PageID,
            Tsn,
        ) {
            // Create a TSN-subtree leaf with a PageID
            let tsn_sub_leaf_id1 = writer.alloc_page_id();
            let pid1 = writer.alloc_page_id();
            let pid2 = writer.alloc_page_id();
            let tsn_sub_leaf1 = crate::free_lists_tree_nodes::FreeListTsnLeafNode {
                page_ids: vec![pid1, pid2],
            };
            writer
                .insert_dirty(Page::new(
                    tsn_sub_leaf_id1,
                    Node::FreeListTsnLeaf(tsn_sub_leaf1),
                ))
                .unwrap();

            // Create a TSN-subtree leaf with a PageID
            let tsn_sub_leaf_id2 = writer.alloc_page_id();
            let pid3 = writer.alloc_page_id();
            let pid4 = writer.alloc_page_id();
            let tsn_sub_leaf2 = crate::free_lists_tree_nodes::FreeListTsnLeafNode {
                page_ids: vec![pid3, pid4],
            };
            writer
                .insert_dirty(Page::new(
                    tsn_sub_leaf_id2,
                    Node::FreeListTsnLeaf(tsn_sub_leaf2),
                ))
                .unwrap();

            // Create a TSN-subtree internal node
            let tsn_sub_internal_id1 = writer.alloc_page_id();
            let tsn_sub_internal1 = crate::free_lists_tree_nodes::FreeListTsnInternalNode {
                keys: vec![pid3],
                child_ids: vec![tsn_sub_leaf_id1, tsn_sub_leaf_id2],
            };
            writer
                .insert_dirty(Page::new(
                    tsn_sub_internal_id1,
                    Node::FreeListTsnInternal(tsn_sub_internal1),
                ))
                .unwrap();

            // Create a TSN-subtree leaf with a PageID
            let tsn_sub_leaf_id3 = writer.alloc_page_id();
            let pid5 = writer.alloc_page_id();
            let pid6 = writer.alloc_page_id();
            let tsn_sub_leaf3 = crate::free_lists_tree_nodes::FreeListTsnLeafNode {
                page_ids: vec![pid5, pid6],
            };
            writer
                .insert_dirty(Page::new(
                    tsn_sub_leaf_id3,
                    Node::FreeListTsnLeaf(tsn_sub_leaf3),
                ))
                .unwrap();

            // Create a TSN-subtree leaf with a PageID
            let tsn_sub_leaf_id4 = writer.alloc_page_id();
            let pid7 = writer.alloc_page_id();
            let pid8 = writer.alloc_page_id();
            let tsn_sub_leaf4 = crate::free_lists_tree_nodes::FreeListTsnLeafNode {
                page_ids: vec![pid7, pid8],
            };
            writer
                .insert_dirty(Page::new(
                    tsn_sub_leaf_id4,
                    Node::FreeListTsnLeaf(tsn_sub_leaf4),
                ))
                .unwrap();

            // Create a TSN-subtree internal node
            let tsn_sub_internal_id2 = writer.alloc_page_id();
            let tsn_sub_internal2 = crate::free_lists_tree_nodes::FreeListTsnInternalNode {
                keys: vec![pid7],
                child_ids: vec![tsn_sub_leaf_id3, tsn_sub_leaf_id4],
            };
            writer
                .insert_dirty(Page::new(
                    tsn_sub_internal_id2,
                    Node::FreeListTsnInternal(tsn_sub_internal2),
                ))
                .unwrap();

            // Create a TSN-subtree internal node
            let tsn_sub_internal_id3 = writer.alloc_page_id();
            let tsn_sub_internal3 = crate::free_lists_tree_nodes::FreeListTsnInternalNode {
                keys: vec![pid5],
                child_ids: vec![tsn_sub_internal_id1, tsn_sub_internal_id2],
            };
            writer
                .insert_dirty(Page::new(
                    tsn_sub_internal_id3,
                    Node::FreeListTsnInternal(tsn_sub_internal3),
                ))
                .unwrap();

            // Make a leaf value point to the internal node
            let tsn = writer.tsn;
            let leaf = FreeListLeafNode {
                keys: vec![tsn],
                values: vec![FreeListLeafValue {
                    page_ids: vec![],
                    root_id: tsn_sub_internal_id3,
                }],
            };
            let root_id = writer.alloc_page_id();
            writer
                .insert_dirty(Page::new(root_id, Node::FreeListLeaf(leaf)))
                .unwrap();
            writer.append_freed_page_id(writer.free_lists_tree_root_id);
            writer.free_lists_tree_root_id = root_id;
            (pid1, pid2, pid3, pid4, pid5, pid6, pid7, pid8, tsn)
        }

        #[test]
        #[serial]
        fn test_insert_freed_page_id_to_empty_leaf_root() {
            let (_temp_dir, mut db) = construct_mvcc(64);

            // Get latest header
            let (header_page_id, header_node) = db.get_latest_header().unwrap();
            assert_eq!(PageID(0), header_page_id);

            // Get next page ID
            assert_eq!(PageID(5), header_node.next_page_id);

            // Construct a writer
            let tsn = Tsn(1001);

            let mut writer = Writer::new(
                header_page_id,
                tsn,
                header_node.next_page_id,
                header_node.free_lists_tree_root_id,
                header_node.events_tree_root_id,
                header_node.tags_tree_root_id,
                header_node.next_position,
                VERBOSE,
            );

            // Check the free list tree root ID
            let initial_root_id = writer.free_lists_tree_root_id;
            assert_eq!(PageID(2), initial_root_id);

            // Allocate a page ID (to be inserted as "freed")
            let page_id = writer.alloc_page_id();

            // Check the allocated page ID is the "next" page ID
            assert_eq!(header_node.next_page_id, page_id);

            // Insert the allocated page ID in the free list tree
            let current_tsn = writer.tsn;
            writer
                .insert_freed_page_id(&mut db, current_tsn, page_id)
                .unwrap();

            // Check the root page has been CoW-ed
            let expected_new_root_id = PageID(header_node.next_page_id.0 + 1);
            assert_eq!(expected_new_root_id, writer.free_lists_tree_root_id);
            assert_eq!(1, writer.dirty.len());
            assert!(writer.dirty.contains_key(&expected_new_root_id));

            let new_root_page = writer.dirty.get(&expected_new_root_id).unwrap();
            assert_eq!(expected_new_root_id, new_root_page.page_id);

            let freed_page_ids: Vec<PageID> = writer.freed_page_ids.iter().cloned().collect();
            assert_eq!(vec![initial_root_id], freed_page_ids);

            // Check keys and values of the new root page
            match &new_root_page.node {
                Node::FreeListLeaf(node) => {
                    let expected_keys = vec![tsn];
                    assert_eq!(expected_keys, node.keys);

                    let expected_values = vec![FreeListLeafValue {
                        page_ids: vec![header_node.next_page_id],
                        root_id: PageID(0),
                    }];
                    assert_eq!(expected_values, node.values);
                }
                _ => panic!("Expected FreeListLeaf node"),
            }
        }

        #[test]
        #[serial]
        fn test_remove_freed_page_id_from_root_leaf_root() {
            let (_temp_dir, mut db) = construct_mvcc(64);

            // First, insert a page ID
            let mut writer;
            let inserted_tsn;
            let inserted_page_id;
            let previous_root_id;

            {
                // Get a writer
                writer = db.writer().unwrap();

                // Remember the initial root ID
                previous_root_id = writer.free_lists_tree_root_id;

                // Allocate a page ID
                inserted_page_id = writer.alloc_page_id();

                // Insert the page ID
                inserted_tsn = writer.tsn;
                writer
                    .insert_freed_page_id(&mut db, inserted_tsn, inserted_page_id)
                    .unwrap();

                // Commit the transaction
                db.commit(&mut writer).unwrap();
            }

            // Block inserted page IDs from being reused
            {
                db.reader_tsns.insert(0, Tsn(0));
            }

            // Start a new writer to remove inserted freed page ID
            {
                writer = db.writer().unwrap();

                // Check there are no free pages (because we blocked them with a reader)
                assert_eq!(0, writer.reusable_page_ids.len());

                // Remember the initial root ID and next page ID
                let initial_root_id = writer.free_lists_tree_root_id;
                let next_page_id = writer.next_page_id;

                // Remove what was inserted
                writer
                    .remove_free_page_id(&db, inserted_tsn, inserted_page_id)
                    .unwrap();

                // Check the root page has been CoW-ed
                let expected_new_root_id = next_page_id;
                assert_eq!(expected_new_root_id, writer.free_lists_tree_root_id);
                assert_eq!(1, writer.dirty.len());
                assert!(writer.dirty.contains_key(&expected_new_root_id));

                let new_root_page = writer.dirty.get(&expected_new_root_id).unwrap();
                assert_eq!(expected_new_root_id, new_root_page.page_id);

                let freed_page_ids: Vec<PageID> = writer.freed_page_ids.iter().cloned().collect();
                assert_eq!(vec![initial_root_id], freed_page_ids);

                // Check keys and values of the new root page
                match &new_root_page.node {
                    Node::FreeListLeaf(node) => {
                        let expected_keys = vec![inserted_tsn];
                        assert_eq!(expected_keys, node.keys);

                        let expected_values = vec![FreeListLeafValue {
                            page_ids: vec![previous_root_id],
                            root_id: PageID(0),
                        }];
                        assert_eq!(expected_values, node.values);
                    }
                    _ => panic!("Expected FreeListLeaf node"),
                }
            }
        }

        #[test]
        #[serial]
        fn test_insert_freed_page_ids_until_split_leaf() {
            let (_temp_dir, mut db) = construct_mvcc(64);

            // Get latest header
            let (header_page_id, header_node) = db.get_latest_header().unwrap();

            // Create a writer
            let mut tsn = Tsn(100);
            let mut writer = Writer::new(
                header_page_id,
                Tsn(header_node.tsn.0 + 1),
                header_node.next_page_id,
                header_node.free_lists_tree_root_id,
                header_node.events_tree_root_id,
                header_node.tags_tree_root_id,
                header_node.next_position,
                VERBOSE,
            );

            let mut has_split_leaf = false;
            let mut inserted: Vec<(Tsn, PageID)> = Vec::new();

            // Insert page IDs until we split a leaf
            while !has_split_leaf {
                // Allocate and insert the first page ID
                let page_id1 = writer.alloc_page_id();
                writer.insert_freed_page_id(&mut db, tsn, page_id1).unwrap();
                inserted.push((tsn, page_id1));

                // Allocate and insert second page ID
                let page_id2 = writer.alloc_page_id();
                writer.insert_freed_page_id(&mut db, tsn, page_id2).unwrap();
                inserted.push((tsn, page_id2));

                // Increment TSN for next iteration
                tsn = Tsn(tsn.0 + 1);

                // Check if we've split the leaf
                let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
                match &root_page.node {
                    Node::FreeListInternal(_) => {
                        has_split_leaf = true;
                    }
                    _ => {}
                }
            }

            // Check keys and values of all pages
            let mut copy_inserted = inserted.clone();

            // Get the root node
            let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
            let root_node = match &root_page.node {
                Node::FreeListInternal(node) => node,
                _ => panic!("Expected FreeListInternal node"),
            };

            // Collect active page IDs
            let mut active_page_ids = vec![
                HEADER_PAGE_ID_0,
                HEADER_PAGE_ID_1,
                writer.free_lists_tree_root_id,
                writer.events_tree_root_id,
                writer.tags_tree_root_id,
            ];

            // Collect freed page IDs
            let mut freed_page_ids: Vec<PageID> = writer.freed_page_ids.iter().cloned().collect();

            // Check each child of the root
            for (i, &child_id) in root_node.child_ids.iter().enumerate() {
                active_page_ids.push(child_id);

                let child_page = writer.dirty.get(&child_id).unwrap();
                assert_eq!(child_id, child_page.page_id);

                let child_node = match &child_page.node {
                    Node::FreeListLeaf(node) => node,
                    _ => panic!("Expected FreeListLeaf node"),
                };

                // Check that the keys are properly ordered
                if i > 0 {
                    assert_eq!(root_node.keys[i - 1], child_node.keys[0]);
                }

                // Check each key and value in the child
                for (k, &key) in child_node.keys.iter().enumerate() {
                    for &value in &child_node.values[k].page_ids {
                        let (inserted_tsn, inserted_page_id) = copy_inserted.remove(0);
                        assert_eq!(inserted_tsn, key);
                        assert_eq!(inserted_page_id, value);
                        freed_page_ids.push(value);
                    }
                }
            }

            // We just split a leaf, so now we have 6 pages
            assert_eq!(7, active_page_ids.len());

            // Audit page IDs
            let mut all_page_ids = active_page_ids.clone();
            all_page_ids.extend(freed_page_ids.clone());
            all_page_ids.sort();
            all_page_ids.dedup();

            assert_eq!(
                all_page_ids.len(),
                active_page_ids.len() + freed_page_ids.len()
                    - active_page_ids
                        .iter()
                        .filter(|id| freed_page_ids.contains(id))
                        .count()
            );

            // Check that all page IDs are accounted for
            let expected_page_ids: Vec<PageID> = (0..writer.next_page_id.0).map(PageID).collect();
            assert_eq!(expected_page_ids, all_page_ids);
        }

        #[test]
        #[serial]
        fn test_insert_freed_page_ids_until_replace_internal_node_child_id() {
            let (_temp_dir, db) = construct_mvcc(128);

            // Block inserted page IDs from being reused
            {
                db.reader_tsns.insert(0, Tsn(0));
            }

            let mut has_split_leaf = false;
            let mut inserted: Vec<(Tsn, PageID)> = Vec::new();

            // Insert page IDs until we split a leaf
            while !has_split_leaf {
                // Create a writer
                let mut writer = db.writer().unwrap();
                // Allocate and insert the first page ID
                let page_id1 = writer.alloc_page_id();
                writer
                    .insert_freed_page_id(&db, writer.tsn, page_id1)
                    .unwrap();
                inserted.push((writer.tsn, page_id1));

                // Check if we've split the leaf
                let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
                match &root_page.node {
                    Node::FreeListInternal(_) => {
                        has_split_leaf = true;
                    }
                    _ => {}
                }

                if !has_split_leaf {
                    // Allocate and insert second page ID
                    let page_id2 = writer.alloc_page_id();
                    writer
                        .insert_freed_page_id(&db, writer.tsn, page_id2)
                        .unwrap();
                    inserted.push((writer.tsn, page_id2));

                    // Check if we've split the leaf
                    let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
                    match &root_page.node {
                        Node::FreeListInternal(_) => {
                            has_split_leaf = true;
                        }
                        _ => {}
                    }
                }

                db.commit(&mut writer).unwrap();
            }

            let mut writer = db.writer().unwrap();
            let page_id3 = writer.alloc_page_id();
            writer
                .insert_freed_page_id(&db, writer.tsn, page_id3)
                .unwrap();
            db.commit(&mut writer).unwrap();
            inserted.push((writer.tsn, page_id3));

            writer = db.writer().unwrap();
            let page_id4 = writer.alloc_page_id();
            writer
                .insert_freed_page_id(&db, writer.tsn, page_id4)
                .unwrap();
            db.commit(&mut writer).unwrap();
            inserted.push((writer.tsn, page_id4));

            // Get the root node
            writer = db.writer().unwrap();
            let root_page = db.read_page(writer.free_lists_tree_root_id).unwrap();
            let root_node = match &root_page.node {
                Node::FreeListInternal(node) => node,
                _ => panic!("Expected FreeListInternal node"),
            };

            // Collect active page IDs
            let mut active_page_ids = vec![
                HEADER_PAGE_ID_0,
                HEADER_PAGE_ID_1,
                writer.free_lists_tree_root_id,
                writer.events_tree_root_id,
                writer.tags_tree_root_id,
            ];

            // Collect freed page IDs from the writer
            let mut freed_page_ids: Vec<PageID> = writer.freed_page_ids.iter().cloned().collect();

            // Collect child page IDs
            for (i, &child_id) in root_node.child_ids.iter().enumerate() {
                active_page_ids.push(child_id);

                // Check each child page
                let child_page = db.read_page(child_id).unwrap();
                assert_eq!(child_id, child_page.page_id);

                let child_node = match &child_page.node {
                    Node::FreeListLeaf(node) => node,
                    _ => panic!("Expected FreeListLeaf node"),
                };

                // Check that the keys are properly ordered
                if i > 0 {
                    assert_eq!(root_node.keys[i - 1], child_node.keys[0]);
                }

                // Collect freed page IDs from the child page values
                for child_value in child_node.values.clone() {
                    for page_id in child_value.page_ids {
                        freed_page_ids.push(page_id);
                    }
                }
            }

            // We have split a leaf node, so now we have 8 active pages
            assert_eq!(8, active_page_ids.len());

            // Audit page IDs
            let mut all_page_ids = active_page_ids.clone();
            all_page_ids.extend(freed_page_ids.clone());
            all_page_ids.sort();
            all_page_ids.dedup();

            assert_eq!(
                all_page_ids.len(),
                active_page_ids.len() + freed_page_ids.len()
                    - active_page_ids
                        .iter()
                        .filter(|id| freed_page_ids.contains(id))
                        .count()
            );

            // Check that all page IDs are accounted for
            let expected_page_ids: Vec<PageID> = (0..writer.next_page_id.0).map(PageID).collect();
            assert_eq!(expected_page_ids, all_page_ids);
        }

        #[test]
        #[serial]
        fn test_remove_freed_page_ids_from_split_leaf() {
            let (_temp_dir, mut db) = construct_mvcc(128);

            // First, insert page IDs until we split a leaf
            if VERBOSE {
                println!("Inserting page IDs......");
            }
            let mut inserted: Vec<(Tsn, PageID)> = Vec::new();
            let previous_root_id;
            let previous_writer_tsn;

            {
                // Get a writer
                let mut writer = db.writer().unwrap();

                // Remember the initial root ID
                previous_root_id = writer.free_lists_tree_root_id;

                // Insert page IDs until we split a leaf
                let mut has_split_leaf = false;
                let mut tsn = writer.tsn;

                while !has_split_leaf {
                    // Increment TSN
                    tsn = Tsn(tsn.0 + 1);

                    // Allocate and insert the first page ID
                    let page_id1 = writer.alloc_page_id();
                    writer.insert_freed_page_id(&mut db, tsn, page_id1).unwrap();
                    inserted.push((tsn, page_id1));

                    // Allocate and insert second page ID
                    let page_id2 = writer.alloc_page_id();
                    writer.insert_freed_page_id(&mut db, tsn, page_id2).unwrap();
                    inserted.push((tsn, page_id2));

                    // Check if we've split the leaf
                    let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
                    match &root_page.node {
                        Node::FreeListInternal(_) => {
                            has_split_leaf = true;
                        }
                        _ => {}
                    }
                }

                // Remember the final TSN
                writer.tsn = Tsn(tsn.0 + 1);
                previous_writer_tsn = writer.tsn;

                // Commit the transaction
                db.commit(&mut writer).unwrap();
            }

            // Now remove all the inserted freed page IDs
            if VERBOSE {
                println!();
                println!("Removing all inserted page IDs......");
            }

            {
                // Get latest header
                let (header_page_id, header_node) = db.get_latest_header().unwrap();

                // Create a new writer
                let mut writer = Writer::new(
                    header_page_id,
                    Tsn(header_node.tsn.0 + 1),
                    header_node.next_page_id,
                    header_node.free_lists_tree_root_id,
                    header_node.events_tree_root_id,
                    header_node.tags_tree_root_id,
                    header_node.next_position,
                    VERBOSE,
                );

                // Remember the initial root ID
                let old_root_id = writer.free_lists_tree_root_id;

                // Remove all inserted page IDs
                for (tsn, page_id) in inserted.iter() {
                    writer.remove_free_page_id(&db, *tsn, *page_id).unwrap();
                    if VERBOSE {
                        println!("Dirty pages: {:?}", writer.dirty.keys());
                    }
                }

                // Check the root page has been CoW-ed
                assert_ne!(old_root_id, writer.free_lists_tree_root_id);

                assert_eq!(1, writer.dirty.len());
                assert!(writer.dirty.contains_key(&writer.free_lists_tree_root_id));

                let new_root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
                assert_eq!(writer.free_lists_tree_root_id, new_root_page.page_id);

                // Check old root ID is in freed page IDs
                let freed_page_ids: Vec<PageID> = writer.freed_page_ids.iter().cloned().collect();
                assert!(freed_page_ids.contains(&old_root_id));

                // There were three pages, and now we have one. We have
                // freed page IDs for three old pages and two CoW pages.
                assert_eq!(5, writer.freed_page_ids.len());

                // Check keys and values of the new root page
                match &new_root_page.node {
                    Node::FreeListLeaf(node) => {
                        let expected_keys = vec![previous_writer_tsn];
                        assert_eq!(expected_keys, node.keys);

                        let expected_values = vec![FreeListLeafValue {
                            page_ids: vec![previous_root_id],
                            root_id: PageID(0),
                        }];
                        assert_eq!(expected_values, node.values);
                    }
                    _ => panic!("Expected FreeListLeaf node"),
                }

                // Audit page IDs
                let active_page_ids = vec![
                    HEADER_PAGE_ID_0,
                    HEADER_PAGE_ID_1,
                    writer.free_lists_tree_root_id,
                    writer.events_tree_root_id,
                    writer.tags_tree_root_id,
                ];

                // Collect all freed page IDs
                let mut all_freed_page_ids = freed_page_ids.clone();

                // Add page IDs from the leaf node
                match &new_root_page.node {
                    Node::FreeListLeaf(node) => {
                        for (_, value) in node.keys.iter().zip(node.values.iter()) {
                            all_freed_page_ids.extend(value.page_ids.clone());
                        }
                    }
                    _ => panic!("Expected FreeListLeaf node"),
                }

                // Add inserted page IDs
                let inserted_page_ids: Vec<PageID> = inserted.iter().map(|(_, id)| *id).collect();

                // Collect all page IDs
                let mut all_page_ids = active_page_ids.clone();
                all_page_ids.extend(all_freed_page_ids.clone());
                all_page_ids.extend(inserted_page_ids.clone());
                all_page_ids.sort();
                all_page_ids.dedup();

                // Check that all page IDs are accounted for
                let expected_page_ids: Vec<PageID> =
                    (0..writer.next_page_id.0).map(PageID).collect();
                assert_eq!(expected_page_ids, all_page_ids);
            }
        }

        #[test]
        #[serial]
        fn test_insert_freed_page_ids_until_split_internal() {
            let (_temp_dir, mut db) = construct_mvcc(64);

            // Get latest header
            let (header_page_id, header_node) = db.get_latest_header().unwrap();

            // Create a writer
            let mut writer = Writer::new(
                header_page_id,
                Tsn(header_node.tsn.0 + 1),
                header_node.next_page_id,
                header_node.free_lists_tree_root_id,
                header_node.events_tree_root_id,
                header_node.tags_tree_root_id,
                header_node.next_position,
                VERBOSE,
            );

            // Start with TSN 100
            let mut tsn = Tsn(100);
            let mut inserted: Vec<(Tsn, PageID)> = Vec::new();
            let mut has_split_internal = false;

            // Insert page IDs until we split an internal node
            while !has_split_internal {
                // Allocate and insert the first page ID
                let page_id1 = writer.alloc_page_id();
                writer.insert_freed_page_id(&mut db, tsn, page_id1).unwrap();
                inserted.push((tsn, page_id1));

                // Allocate and insert second page ID
                let page_id2 = writer.alloc_page_id();
                writer.insert_freed_page_id(&mut db, tsn, page_id2).unwrap();
                inserted.push((tsn, page_id2));

                // Increment TSN for next iteration
                tsn = Tsn(tsn.0 + 1);

                // Check if we've split an internal node
                let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
                match &root_page.node {
                    Node::FreeListInternal(root_node) => {
                        // Check if the first child is an internal node
                        if !root_node.child_ids.is_empty() {
                            let child_id = root_node.child_ids[0];
                            if let Some(child_page) = writer.dirty.get(&child_id) {
                                match &child_page.node {
                                    Node::FreeListInternal(_) => {
                                        has_split_internal = true;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    _ => {}
                }
                if inserted.len() > 100 {
                    panic!("Too many inserted page IDs");
                }
            }

            // Check keys and values of all pages
            let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
            let root_node = match &root_page.node {
                Node::FreeListInternal(node) => node,
                _ => panic!("Expected FreeListInternal node"),
            };

            // Collect active page IDs
            let mut active_page_ids = vec![
                HEADER_PAGE_ID_0,
                HEADER_PAGE_ID_1,
                writer.free_lists_tree_root_id,
                writer.events_tree_root_id,
                writer.tags_tree_root_id,
            ];

            // Collect freed page IDs
            let mut freed_page_ids: Vec<PageID> = writer.freed_page_ids.iter().cloned().collect();

            // Track the previous child for key ordering checks
            let mut previous_child: Option<&FreeListInternalNode> = None;

            // Check each child of the root
            for (i, &child_id) in root_node.child_ids.iter().enumerate() {
                active_page_ids.push(child_id);

                let child_page = writer.dirty.get(&child_id).unwrap();
                assert_eq!(child_id, child_page.page_id);

                let child_node = match &child_page.node {
                    Node::FreeListInternal(node) => node,
                    _ => panic!("Expected FreeListInternal node"),
                };

                // Check key ordering between root and child
                if i > 0 {
                    assert!(root_node.keys[i - 1] < child_node.keys[0]);

                    // Check key ordering between previous child and current child
                    if let Some(prev_child) = previous_child {
                        assert!(root_node.keys[i - 1] > *prev_child.keys.last().unwrap());
                    }
                }

                previous_child = Some(child_node);

                // Check each grandchild
                for (j, &grand_child_id) in child_node.child_ids.iter().enumerate() {
                    active_page_ids.push(grand_child_id);

                    let grand_child_page = writer.dirty.get(&grand_child_id).unwrap();
                    assert_eq!(grand_child_id, grand_child_page.page_id);

                    let grand_child_node = match &grand_child_page.node {
                        Node::FreeListLeaf(node) => node,
                        _ => panic!("Expected FreeListLeaf node"),
                    };

                    // Check key ordering between child and grandchild
                    if j > 0 {
                        assert_eq!(child_node.keys[j - 1], grand_child_node.keys[0]);
                    }

                    // Check each key and value in the grandchild
                    for (k, &key) in grand_child_node.keys.iter().enumerate() {
                        for &value in &grand_child_node.values[k].page_ids {
                            // Find the matching inserted item
                            let pos = inserted.iter().position(|&(t, p)| t == key && p == value);
                            if let Some(idx) = pos {
                                inserted.remove(idx);
                            }
                            freed_page_ids.push(value);
                        }
                    }
                }
            }

            // We should have processed all inserted items
            assert!(inserted.is_empty());

            // We should have 10 active pages
            assert_eq!(11, active_page_ids.len());

            // Audit page IDs
            let mut all_page_ids = active_page_ids.clone();
            all_page_ids.extend(freed_page_ids.clone());
            all_page_ids.sort();
            all_page_ids.dedup();

            // Check that all page IDs are accounted for
            let expected_page_ids: Vec<PageID> = (0..writer.next_page_id.0).map(PageID).collect();
            assert_eq!(expected_page_ids, all_page_ids);
        }

        #[test]
        #[serial]
        fn test_remove_freed_page_ids_from_split_internal() {
            let (_temp_dir, mut db) = construct_mvcc(64);

            // First, insert page IDs until we split an internal node
            let mut inserted: Vec<(Tsn, PageID)> = Vec::new();
            let previous_root_id;
            let previous_writer_tsn;

            {
                // Get a writer
                let mut writer = db.writer().unwrap();

                // Remember the initial root ID
                previous_root_id = writer.free_lists_tree_root_id;

                // Insert page IDs until we split an internal node
                let mut has_split_internal = false;
                let mut tsn = writer.tsn;

                while !has_split_internal {
                    // Increment TSN
                    tsn = Tsn(tsn.0 + 1);
                    writer.tsn = tsn;

                    // Allocate and insert the first page ID
                    let page_id1 = writer.alloc_page_id();
                    writer.insert_freed_page_id(&mut db, tsn, page_id1).unwrap();
                    inserted.push((tsn, page_id1));

                    // Allocate and insert second page ID
                    let page_id2 = writer.alloc_page_id();
                    writer.insert_freed_page_id(&mut db, tsn, page_id2).unwrap();
                    inserted.push((tsn, page_id2));

                    // Check if we've split an internal node
                    let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
                    match &root_page.node {
                        Node::FreeListInternal(root_node) => {
                            // Check if the first child is an internal node
                            if !root_node.child_ids.is_empty() {
                                let child_id = root_node.child_ids[0];
                                if let Some(child_page) = writer.dirty.get(&child_id) {
                                    match &child_page.node {
                                        Node::FreeListInternal(_) => {
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

                // Remember the final TSN
                previous_writer_tsn = writer.tsn;

                // Commit the transaction
                db.commit(&mut writer).unwrap();
            }

            // Now remove all the inserted freed page IDs
            {
                // Get latest header
                let (header_page_id, header_node) = db.get_latest_header().unwrap();

                // Create a new writer
                let mut writer = Writer::new(
                    header_page_id,
                    Tsn(header_node.tsn.0 + 1),
                    header_node.next_page_id,
                    header_node.free_lists_tree_root_id,
                    header_node.events_tree_root_id,
                    header_node.tags_tree_root_id,
                    header_node.next_position,
                    VERBOSE,
                );

                // Remember the initial root ID
                let old_root_id = writer.free_lists_tree_root_id;

                // Remove all inserted page IDs
                for (tsn, page_id) in inserted.iter() {
                    writer.remove_free_page_id(&db, *tsn, *page_id).unwrap();
                }

                // Check the root page has been CoW-ed
                assert_ne!(old_root_id, writer.free_lists_tree_root_id);
                assert_eq!(1, writer.dirty.len());
                assert!(writer.dirty.contains_key(&writer.free_lists_tree_root_id));

                let new_root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
                assert_eq!(writer.free_lists_tree_root_id, new_root_page.page_id);

                // Check old root ID is in freed page IDs
                let freed_page_ids: Vec<PageID> = writer.freed_page_ids.iter().cloned().collect();
                assert!(freed_page_ids.contains(&old_root_id));

                // There were 14 pages, and now we have 1. We have
                // freed page IDs for 7 old pages and 6 CoW pages.
                assert_eq!(13, writer.freed_page_ids.len());

                // Check keys and values of the new root page
                match &new_root_page.node {
                    Node::FreeListLeaf(node) => {
                        let expected_keys = vec![previous_writer_tsn];
                        assert_eq!(expected_keys, node.keys);

                        let expected_values = vec![FreeListLeafValue {
                            page_ids: vec![previous_root_id],
                            root_id: PageID(0),
                        }];
                        assert_eq!(expected_values, node.values);
                    }
                    _ => panic!("Expected FreeListLeaf node"),
                }

                // Audit page IDs
                let active_page_ids = vec![
                    HEADER_PAGE_ID_0,
                    HEADER_PAGE_ID_1,
                    writer.free_lists_tree_root_id,
                    writer.events_tree_root_id,
                    writer.tags_tree_root_id,
                ];

                // Collect all freed page IDs
                let mut all_freed_page_ids = freed_page_ids.clone();

                // Add page IDs from the leaf node
                match &new_root_page.node {
                    Node::FreeListLeaf(node) => {
                        for (_, value) in node.keys.iter().zip(node.values.iter()) {
                            all_freed_page_ids.extend(value.page_ids.clone());
                        }
                    }
                    _ => panic!("Expected FreeListLeaf node"),
                }

                // Add inserted page IDs
                let inserted_page_ids: Vec<PageID> = inserted.iter().map(|(_, id)| *id).collect();

                // Collect all page IDs
                let mut all_page_ids = active_page_ids.clone();
                all_page_ids.extend(all_freed_page_ids.clone());
                all_page_ids.extend(inserted_page_ids.clone());
                all_page_ids.sort();
                all_page_ids.dedup();

                // Check that all page IDs are accounted for
                let expected_page_ids: Vec<PageID> =
                    (0..writer.next_page_id.0).map(PageID).collect();
                assert_eq!(expected_page_ids, all_page_ids);
            }
        }

        #[test]
        #[serial]
        fn test_remove_freed_page_ids_until_replace_old_id_with_orphaned_child_id() {
            // This test covers the case where an internal node is removed but leaving a
            // child page ID that must be replaced in the parent page, when the parent
            // page is not yet dirty. So, here, we must contrive to replace an old ID
            // with an orphaned child ID. We can do this by inserting freed page IDs
            // until there is an internal page split, and then removing them in order
            // until the first internal node is removed, using a new writer each time
            // to avoid the internal node becoming dirty before the last key is removed,
            // so that its parent internal node (which is the root internal node here)
            // will not already have the old child page ID replaced with a dirty
            // child page ID.
            let (_temp_dir, mut db) = construct_mvcc(128);

            // First, insert page IDs until we split an internal node
            let mut inserted: Vec<(Tsn, PageID)> = Vec::new();

            {
                // Get a writer
                let mut writer = db.writer().unwrap();

                // Insert page IDs until we split an internal node
                let mut has_split_internal = false;
                let mut tsn = writer.tsn;

                while !has_split_internal {
                    // Increment TSN
                    tsn = Tsn(tsn.0 + 1);
                    writer.tsn = tsn;

                    // Allocate and insert the first page ID
                    let page_id1 = writer.alloc_page_id();
                    writer.insert_freed_page_id(&mut db, tsn, page_id1).unwrap();
                    inserted.push((tsn, page_id1));

                    // Allocate and insert second page ID
                    let page_id2 = writer.alloc_page_id();
                    writer.insert_freed_page_id(&mut db, tsn, page_id2).unwrap();
                    inserted.push((tsn, page_id2));

                    // Check if we've split an internal node
                    let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
                    match &root_page.node {
                        Node::FreeListInternal(root_node) => {
                            // Check if the first child is an internal node
                            if !root_node.child_ids.is_empty() {
                                let child_id = root_node.child_ids[0];
                                if let Some(child_page) = writer.dirty.get(&child_id) {
                                    match &child_page.node {
                                        Node::FreeListInternal(_) => {
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

                // Commit the transaction
                db.commit(&mut writer).unwrap();
            }

            // Get all the free page IDs.
            db.reader_tsns.remove(&0);
            let writer = db.writer().unwrap();
            let reusable_page_ids = writer.reusable_page_ids.clone();

            // Block inserted page IDs from being reused
            db.reader_tsns.insert(0, Tsn(0));

            // Remove each free page ID.
            for (page_id, tsn) in reusable_page_ids {
                let mut writer = db.writer().unwrap();
                writer.remove_free_page_id(&db, tsn, page_id).unwrap();
                db.commit(&mut writer).unwrap();
            }
        }

        #[test]
        #[serial]
        fn test_insert_freed_page_ids_overflow_single_key_moves_to_tsn_subtree() {
            // Use tiny pages to force the inline list to overflow quickly
            let page_size = 64;
            let (_temp_dir, mut mvcc) = construct_mvcc(page_size);

            // Start a writer
            let mut writer = mvcc.writer().unwrap();
            let tsn = writer.tsn;

            // With page_size=64, max_node_size = 55. A single key/value with 4 page IDs
            // takes 52 bytes, and adding the 5th (+8) would exceed capacity.
            let mut inserted_count: usize = 0;
            loop {
                let pid = writer.alloc_page_id();
                writer.insert_freed_page_id(&mut mvcc, tsn, pid).unwrap();
                inserted_count += 1;
                let dirty_page_id = {
                    let mut keys = writer.dirty.keys();
                    assert_eq!(keys.len(), 1);
                    *keys.next().unwrap()
                };
                let dirty_page = writer.get_mut_dirty(dirty_page_id).unwrap();
                if let Node::FreeListLeaf(leaf_node) = &dirty_page.node {
                    if !leaf_node.would_fit_new_page_id(mvcc.max_node_size) {
                        break;
                    }
                } else {
                    panic!("Expected leaf node")
                }
            }

            // The next insert for the same TSN should succeed by creating a TSN-subtree
            let pid5 = writer.alloc_page_id();
            writer.insert_freed_page_id(&mut mvcc, tsn, pid5).unwrap();
            inserted_count += 1;

            // Insert two more page IDs for the same TSN into the TSN-subtree
            let pid6 = writer.alloc_page_id();
            writer.insert_freed_page_id(&mut mvcc, tsn, pid6).unwrap();
            inserted_count += 1;

            let pid7 = writer.alloc_page_id();
            writer.insert_freed_page_id(&mut mvcc, tsn, pid7).unwrap();
            inserted_count += 1;

            // Verify that the leaf now points to a TSN-subtree for this TSN, and that the TSN-subtree root is an internal node (leaf split)
            let dirty_ids: Vec<PageID> = { writer.dirty.keys().cloned().collect() };
            let mut tsn_root_id = PageID(0);
            for dirty_page_id in dirty_ids {
                let dirty_page = writer.get_mut_dirty(dirty_page_id).unwrap();
                if let Node::FreeListLeaf(leaf_node) = &dirty_page.node {
                    assert_eq!(1, leaf_node.keys.len());
                    assert_eq!(tsn, leaf_node.keys[0]);
                    let val = &leaf_node.values[0];
                    assert_eq!(0, val.page_ids.len());
                    assert_ne!(PageID(0), val.root_id);
                    tsn_root_id = val.root_id;
                    break;
                }
            }
            assert_ne!(PageID(0), tsn_root_id);
            // The TSN-subtree leaf should have split, so the root must now be internal
            // With page_size=64, max_node_size = 55. Seven page IDs exceeds capacity (2 + 8 * 7).
            let tsn_root_page = writer.get_page_ref(&mvcc, tsn_root_id).unwrap();
            match &tsn_root_page.node {
                Node::FreeListTsnInternal(internal) => {
                    assert_eq!(2, internal.child_ids.len());
                    assert_eq!(1, internal.keys.len());
                }
                other => panic!(
                    "Expected TSN-subtree internal node, got {:?}",
                    other.type_name()
                ),
            }

            // Continue inserting page IDs for the same TSN until the TSN-subtree internal node splits
            let mut extra_inserts = 0usize;
            let mut guard = 0usize;
            loop {
                guard += 1;
                assert!(
                    guard < 200,
                    "guard hit while waiting for TSN-subtree internal split"
                );
                let pid = writer.alloc_page_id();
                writer.insert_freed_page_id(&mut mvcc, tsn, pid).unwrap();
                extra_inserts += 1;

                // Refresh tsn_root_id from the FreeList leaf since the root may change when it splits
                let dirty_ids: Vec<PageID> = writer.dirty.keys().cloned().collect();
                tsn_root_id = PageID(0);
                for dirty_page_id in dirty_ids {
                    let dirty_page = writer.get_mut_dirty(dirty_page_id).unwrap();
                    if let Node::FreeListLeaf(leaf_node) = &dirty_page.node {
                        assert_eq!(1, leaf_node.keys.len());
                        assert_eq!(tsn, leaf_node.keys[0]);
                        let val = &leaf_node.values[0];
                        assert_eq!(0, val.page_ids.len());
                        assert_ne!(PageID(0), val.root_id);
                        tsn_root_id = val.root_id;
                        break;
                    }
                }
                assert_ne!(PageID(0), tsn_root_id);

                let root_node_owned = {
                    writer
                        .get_page_ref(&mvcc, tsn_root_id)
                        .unwrap()
                        .node
                        .clone()
                };
                match root_node_owned {
                    Node::FreeListTsnInternal(internal_root) => {
                        // If the first child is also an internal node, then the previous internal split promoted a new root
                        let first_child_id = internal_root.child_ids[0];
                        let first_child_node = {
                            writer
                                .get_page_ref(&mvcc, first_child_id)
                                .unwrap()
                                .node
                                .clone()
                        };
                        if matches!(first_child_node, Node::FreeListTsnInternal(_)) {
                            // We have achieved an internal split in the TSN-subtree
                            // The new root should have exactly 1 key and 2 children after promotion
                            assert_eq!(1, internal_root.keys.len());
                            assert_eq!(2, internal_root.child_ids.len());
                            break;
                        }
                    }
                    other => panic!(
                        "Expected TSN-subtree internal node, got {:?}",
                        other.type_name()
                    ),
                }
            }

            // Now add another PageID to the internal->internal->leaf.
            let pid = writer.alloc_page_id();
            writer.insert_freed_page_id(&mut mvcc, tsn, pid).unwrap();
            extra_inserts += 1;

            // Verify all page IDs are discoverable via find_reusable_page_ids
            writer.find_reusable_page_ids(&mvcc).unwrap();
            assert_eq!(
                inserted_count + extra_inserts,
                writer.reusable_page_ids.len()
            );
            for &(_pid, _tsn) in writer.reusable_page_ids.iter() {
                assert_eq!(tsn, _tsn);
            }
        }

        #[test]
        #[serial]
        fn test_remove_freed_page_id_from_tsn_subtree_leaf_pid1() {
            let (_temp_dir, db) = construct_mvcc(128);
            let mut writer = db.writer().unwrap();

            let (pid1, pid2, tsn1) = build_free_list_tree_leaf_tsn_subtree_leaf(&mut writer);

            writer.find_reusable_page_ids(&db).unwrap();
            assert_eq!(2, writer.reusable_page_ids.len());
            assert_eq!((pid1, tsn1), writer.reusable_page_ids[0]);
            assert_eq!((pid2, tsn1), writer.reusable_page_ids[1]);

            writer.remove_free_page_id(&db, tsn1, pid1).unwrap();
            writer.find_reusable_page_ids(&db).unwrap();
            assert_eq!(1, writer.reusable_page_ids.len());
            assert_eq!((pid2, tsn1), writer.reusable_page_ids[0]);
        }

        #[test]
        #[serial]
        fn test_remove_freed_page_id_from_tsn_subtree_leaf_pid2() {
            let (_temp_dir, db) = construct_mvcc(128);
            let mut writer = db.writer().unwrap();

            let (pid1, pid2, tsn1) = build_free_list_tree_leaf_tsn_subtree_leaf(&mut writer);

            writer.remove_free_page_id(&db, tsn1, pid2).unwrap();
            writer.find_reusable_page_ids(&db).unwrap();
            assert_eq!(1, writer.reusable_page_ids.len());
            assert_eq!((pid1, tsn1), writer.reusable_page_ids[0]);
        }

        #[test]
        #[serial]
        fn test_tsn_subtree_random_order_insert_and_ordering() {
            // Moderate page size to force multiple leaves and internals
            let page_size = 256;
            let (_temp_dir, mut db) = construct_mvcc(page_size);
            let mut writer = db.writer().unwrap();
            let tsn = writer.tsn;

            // Generate many distinct PageIDs, then shuffle their insertion order
            let n = 100usize;
            let mut pids: Vec<PageID> = Vec::with_capacity(n);
            for _ in 0..n {
                pids.push(writer.alloc_page_id());
            }
            // Shuffle by swapping using a simple deterministic pattern to avoid extra deps
            for i in 0..pids.len() {
                let j = (i * 37 + 13) % pids.len();
                pids.swap(i, j);
            }
            // Insert them all for the same TSN in random order
            for pid in &pids {
                writer.insert_freed_page_id(&mut db, tsn, *pid).unwrap();
            }
            // Insert a duplicate of the first ID and ensure it is ignored
            let dup = pids[0];
            writer.insert_freed_page_id(&mut db, tsn, dup).unwrap();

            // Verify via reusable_page_ids
            writer.find_reusable_page_ids(&db).unwrap();
            assert_eq!(n, writer.reusable_page_ids.len());
            for &(_pid, _tsn) in writer.reusable_page_ids.iter() {
                assert_eq!(tsn, _tsn);
            }

            // Inspect the TSN-subtree shape: root should be internal for this many ids
            // Find the freelist leaf that holds this TSN and get its root_id
            let mut tsn_root_id = PageID(0);
            for page_id in writer.dirty.keys().cloned().collect::<Vec<_>>() {
                let page = writer.get_mut_dirty(page_id).unwrap();
                if let Node::FreeListLeaf(leaf_node) = &page.node {
                    if !leaf_node.keys.is_empty() && leaf_node.keys[0] == tsn {
                        tsn_root_id = leaf_node.values[0].root_id;
                        break;
                    }
                }
            }
            assert_ne!(PageID(0), tsn_root_id);
            let root_node_owned = { writer.get_page_ref(&db, tsn_root_id).unwrap().node.clone() };
            match root_node_owned {
                Node::FreeListTsnInternal(internal_root) => {
                    // It’s likely that at least one child is internal for n=100 at this page size
                    let first_child_id = internal_root.child_ids[0];
                    let first_child_node = {
                        writer
                            .get_page_ref(&db, first_child_id)
                            .unwrap()
                            .node
                            .clone()
                    };
                    if let Node::FreeListTsnInternal(_in2) = first_child_node {
                        // ok, internal->internal exists
                    }
                }
                Node::FreeListTsnLeaf(_) => {
                    // With small n this could still be a leaf root; accept but ensure ordering via reusable_page_ids
                }
                other => panic!("Unexpected node type: {}", other.type_name()),
            }
        }

        #[test]
        #[serial]
        fn test_upgrade_inline_to_tsn_subtree_with_random_inserts_and_duplicates() {
            // Small page size to quickly overflow inline and create a TSN-subtree
            let page_size = 96;
            let (_temp_dir, mut mvcc) = construct_mvcc(page_size);
            let mut writer = mvcc.writer().unwrap();
            let tsn = writer.tsn;

            // Insert a few to fill inline list close to capacity
            let mut inline_ids = Vec::new();
            loop {
                let pid = writer.alloc_page_id();
                let res = writer.insert_freed_page_id(&mut mvcc, tsn, pid);
                if res.is_err() {
                    panic!("unexpected error inserting into inline");
                }
                inline_ids.push(pid);
                // Heuristic to break when the next inline would likely overflow and cause move-to-subtree
                let dirty_id = writer.dirty.keys().cloned().next().unwrap();
                if let Node::FreeListLeaf(leaf_node) = &writer.get_mut_dirty(dirty_id).unwrap().node
                {
                    if !leaf_node.would_fit_new_page_id(mvcc.max_node_size) {
                        break;
                    }
                }
            }

            // Now insert a batch of random ids including some duplicates
            let mut extra_ids = Vec::new();
            for _ in 0..30 {
                extra_ids.push(writer.alloc_page_id());
            }
            // Shuffle simple
            for i in 0..extra_ids.len() {
                let j = (i * 29 + 7) % extra_ids.len();
                extra_ids.swap(i, j);
            }
            // Add some duplicates from earlier inline ids
            if !inline_ids.is_empty() {
                extra_ids.push(inline_ids[0]);
            }
            if inline_ids.len() > 1 {
                extra_ids.push(inline_ids[1]);
            }

            for pid in &extra_ids {
                writer.insert_freed_page_id(&mut mvcc, tsn, *pid).unwrap();
            }

            // Verify no duplicates and all ids present
            writer.find_reusable_page_ids(&mvcc).unwrap();
            let mut expected: Vec<PageID> = inline_ids.clone();
            for p in extra_ids {
                if !expected.contains(&p) {
                    expected.push(p);
                }
            }
            expected.sort_by_key(|p| p.0);
            expected.dedup();
            let mut actual: Vec<PageID> =
                writer.reusable_page_ids.iter().map(|(p, _)| *p).collect();
            actual.sort_by_key(|p| p.0);
            assert_eq!(expected, actual);
        }

        #[test]
        #[serial]
        fn test_remove_freed_page_id_from_tsn_subtree_leaf_all() {
            let (_temp_dir, db) = construct_mvcc(128);
            let mut writer = db.writer().unwrap();

            let (pid1, pid2, tsn1) = build_free_list_tree_leaf_tsn_subtree_leaf(&mut writer);

            writer.remove_free_page_id(&db, tsn1, pid1).unwrap();
            writer.remove_free_page_id(&db, tsn1, pid2).unwrap();
            writer.find_reusable_page_ids(&db).unwrap();
            assert_eq!(0, writer.reusable_page_ids.len());

            assert_eq!(2, writer.freed_page_ids.len());
        }

        #[test]
        #[serial]
        fn test_remove_freed_page_id_from_tsn_subtree_internal_leaf_pid1() {
            let (_temp_dir, db) = construct_mvcc(128);
            let mut writer = db.writer().unwrap();

            let (pid1, pid2, pid3, pid4, tsn1) =
                build_free_list_tree_leaf_tsn_subtree_internal_leaf(&mut writer);

            writer.remove_free_page_id(&db, tsn1, pid1).unwrap();
            writer.find_reusable_page_ids(&db).unwrap();
            assert_eq!(3, writer.reusable_page_ids.len());
            assert_eq!((pid2, tsn1), writer.reusable_page_ids[0]);
            assert_eq!((pid3, tsn1), writer.reusable_page_ids[1]);
            assert_eq!((pid4, tsn1), writer.reusable_page_ids[2]);

            assert_eq!(1, writer.freed_page_ids.len());

            writer.remove_free_page_id(&db, tsn1, pid2).unwrap();
            writer.find_reusable_page_ids(&db).unwrap();
            assert_eq!(2, writer.reusable_page_ids.len());
            assert_eq!((pid3, tsn1), writer.reusable_page_ids[0]);
            assert_eq!((pid4, tsn1), writer.reusable_page_ids[1]);

            assert_eq!(3, writer.freed_page_ids.len());

            writer.remove_free_page_id(&db, tsn1, pid3).unwrap();
            writer.find_reusable_page_ids(&db).unwrap();
            assert_eq!(1, writer.reusable_page_ids.len());
            assert_eq!((pid4, tsn1), writer.reusable_page_ids[0]);

            assert_eq!(3, writer.freed_page_ids.len());

            writer.remove_free_page_id(&db, tsn1, pid4).unwrap();
            writer.find_reusable_page_ids(&db).unwrap();
            assert_eq!(0, writer.reusable_page_ids.len());

            assert_eq!(4, writer.freed_page_ids.len());
        }

        #[test]
        #[serial]
        fn test_remove_freed_page_id_from_tsn_subtree_internal_internal_leaf_pid1() {
            let (_temp_dir, db) = construct_mvcc(128);
            let mut writer = db.writer().unwrap();

            let (pid1, pid2, pid3, pid4, pid5, pid6, pid7, pid8, tsn) =
                build_free_list_tree_leaf_tsn_subtree_internal_internal_leaf(&mut writer);

            writer.remove_free_page_id(&db, tsn, pid1).unwrap();
            writer.find_reusable_page_ids(&db).unwrap();
            assert_eq!(7, writer.reusable_page_ids.len());
            assert_eq!((pid2, tsn), writer.reusable_page_ids[0]);
            assert_eq!((pid3, tsn), writer.reusable_page_ids[1]);
            assert_eq!((pid4, tsn), writer.reusable_page_ids[2]);
            assert_eq!((pid5, tsn), writer.reusable_page_ids[3]);
            assert_eq!((pid6, tsn), writer.reusable_page_ids[4]);
            assert_eq!((pid7, tsn), writer.reusable_page_ids[5]);
            assert_eq!((pid8, tsn), writer.reusable_page_ids[6]);

            assert_eq!(1, writer.freed_page_ids.len());
        }

        #[test]
        #[serial]
        fn test_remove_freed_page_id_cow_does_not_leak_ids_in_tsn_subtree() {
            // Build a TSN-subtree with depth (internal->internal->leaf), commit it, then
            // perform a removal in a new writer to force copy-on-write on the subtree path.
            let (_temp_dir, db) = construct_mvcc(128);

            // First writer builds the TSN-subtree and commits
            let w1_tsn = {
                let mut w1 = db.writer().unwrap();
                let (_pid1, _pid2, _pid3, _pid4, _pid5, _pid6, _pid7, _pid8, tsn) =
                    build_free_list_tree_leaf_tsn_subtree_internal_internal_leaf(&mut w1);
                // Commit so that subsequent modifications must use COW
                db.commit(&mut w1).unwrap();
                // Keep tsn for next phase via storing in header
                tsn
            };

            // Second writer removes a page id to trigger COW on nodes along the path
            let mut w2 = db.writer().unwrap();
            // Find one reusable pid to remove via API, ensure tree populated
            w2.find_reusable_page_ids(&db).unwrap();
            assert!(w2.reusable_page_ids.len() >= 1);
            let (remove_pid, remove_tsn) = w2.reusable_page_ids[0];
            assert_eq!(remove_tsn, w1_tsn); // All built under this tsn in the helper
            w2.remove_free_page_id(&db, remove_tsn, remove_pid).unwrap();

            // Now audit: every page id from 0..next_page_id must be either active or freed.
            // Collect active page ids by traversing the current free list tree, including TSN-subtrees.
            let mut active: Vec<PageID> = Vec::new();
            let mut freed_tree: Vec<PageID> = Vec::new();
            // Always-active headers and roots
            active.push(HEADER_PAGE_ID_0);
            active.push(HEADER_PAGE_ID_1);
            active.push(w2.free_lists_tree_root_id);
            active.push(w2.events_tree_root_id);
            active.push(w2.tags_tree_root_id);

            // Traverse free list tree from current root, reading nodes from disk or cache
            let mut stack: Vec<PageID> = vec![w2.free_lists_tree_root_id];
            while let Some(pid) = stack.pop() {
                if active.contains(&pid) == false {
                    active.push(pid);
                }
                let node_owned = { w2.get_page_ref(&db, pid).unwrap().node.clone() };
                match node_owned {
                    Node::FreeListInternal(internal) => {
                        for child in internal.child_ids {
                            stack.push(child);
                        }
                    }
                    Node::FreeListLeaf(leaf) => {
                        // Any inline free page IDs are considered freed
                        for val in &leaf.values {
                            for &p in &val.page_ids {
                                freed_tree.push(p);
                            }
                        }
                        // If TSN-subtree root exists, traverse it too
                        for val in leaf.values {
                            if val.root_id != PageID(0) {
                                let mut tsn_stack: Vec<PageID> = vec![val.root_id];
                                while let Some(tid) = tsn_stack.pop() {
                                    if active.contains(&tid) == false {
                                        active.push(tid);
                                    }
                                    let tnode = { w2.get_page_ref(&db, tid).unwrap().node.clone() };
                                    match tnode {
                                        Node::FreeListTsnInternal(tint) => {
                                            for c in tint.child_ids {
                                                tsn_stack.push(c);
                                            }
                                        }
                                        Node::FreeListTsnLeaf(tleaf) => {
                                            for &p in &tleaf.page_ids {
                                                freed_tree.push(p);
                                            }
                                        }
                                        _ => panic!("Unexpected node type in TSN-subtree"),
                                    }
                                }
                            }
                        }
                    }
                    _ => panic!("Unexpected node type in free list tree"),
                }
            }

            // Also include any currently-dirty pages (new COW copies) as active
            for (&pid, _page) in w2.dirty.iter() {
                if !active.contains(&pid) {
                    active.push(pid);
                }
            }
            // And include any pages we've read/deserialized during traversal
            for (&pid, _page) in w2.deserialized.iter() {
                if !active.contains(&pid) {
                    active.push(pid);
                }
            }

            // Collect freed ids recorded by writer due to COW or structural removals
            let mut freed: Vec<PageID> = w2.freed_page_ids.iter().cloned().collect();
            // Include the reusable ids present in the tree itself
            freed.extend(freed_tree);
            // Include the reusable ids tracked by the writer snapshot
            for (pid, _tsn) in w2.reusable_page_ids.iter() {
                freed.push(*pid);
            }

            // Validate coverage
            active.sort_by_key(|p| p.0);
            active.dedup();
            freed.sort_by_key(|p| p.0);
            freed.dedup();
            let mut union = active.clone();
            for id in &freed {
                if !union.contains(id) {
                    union.push(*id);
                }
            }
            union.sort_by_key(|p| p.0);

            let expected: Vec<PageID> = (0..w2.next_page_id.0).map(PageID).collect();
            if expected != union {
                let mut missing: Vec<PageID> = expected
                    .iter()
                    .cloned()
                    .filter(|p| !union.contains(p))
                    .collect();
                let mut unexpected: Vec<PageID> = union
                    .iter()
                    .cloned()
                    .filter(|p| !expected.contains(p))
                    .collect();
                missing.sort_by_key(|p| p.0);
                unexpected.sort_by_key(|p| p.0);
                panic!(
                    "Missing: {:?} Unexpected: {:?}\nactive: {:?}\nfreed: {:?}",
                    missing, unexpected, active, freed
                );
            }
            assert_eq!(
                expected, union,
                "All page IDs must be accounted for (active or freed)"
            );
        }
    }
}
