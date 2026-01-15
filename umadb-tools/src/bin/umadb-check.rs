use std::collections::{BTreeMap, HashMap};
use std::env;
use std::path::PathBuf;
use std::os::unix::fs::FileExt;
use umadb_core::common::{PageID, Position};
use umadb_core::db::{read_conditional, DEFAULT_DB_FILENAME, DEFAULT_PAGE_SIZE, tag_to_hash};
use umadb_core::mvcc::Mvcc;
use umadb_core::page::Page;
use umadb_core::node::Node;
use umadb_core::tags_tree_nodes::{get_tag_key_width, normalize_tag_hash_for_current_width, TAG_HASH_LEN, set_tag_key_width, TagsLeafNode};
use umadb_dcb::{DCBError, DCBResult, DCBEvent, DCBQuery};

fn type_name_from_byte(b: u8) -> Option<&'static str> {
    match b {
        b'1' => Some("Header"),
        b'2' => Some("FreeListLeaf"),
        b'3' => Some("FreeListInternal"),
        b'4' => Some("EventLeaf"),
        b'5' => Some("EventInternal"),
        b'6' => Some("TagsLeaf"),
        b'7' => Some("TagsInternal"),
        b'8' => Some("TagLeaf"),
        b'9' => Some("TagInternal"),
        b'a' => Some("EventOverflow"),
        b'b' => Some("FreeListTsnLeaf"),
        b'c' => Some("FreeListTsnInternal"),
        b'd' => Some("TrackingLeaf"),
        _ => None,
    }
}

fn print_usage(program: &str) {
    eprintln!(
        "Usage: {program} [--page-size <bytes>] [--verbose] [--print-events] <path-to-db-or-directory>\n\n\
        Options:\n  --page-size <bytes>   Page size in bytes (defaults to UmaDB default).\n  --verbose             Print per-page details.\n  --print-events        Print all events found during Stage 3 with one attribute per line, including data.\n\n\
        The tool scans the database file page-by-page and reports any CRC/parse errors,\n        along with a summary of page counts by node type. It never modifies the file."
    );
}

#[derive(Default)]
struct Args {
    page_size: Option<usize>,
    verbose: bool,
    print_events: bool,
    path: Option<PathBuf>,
}

fn parse_args() -> Result<Args, String> {
    let mut args = env::args().skip(1);
    let mut res = Args::default();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--page-size" => {
                let v = args
                    .next()
                    .ok_or_else(|| "--page-size requires a value".to_string())?;
                let ps: usize = v
                    .parse()
                    .map_err(|_| format!("Invalid page size: {v}"))?;
                if ps == 0 || ps % 512 != 0 {
                    return Err("Page size must be a positive multiple of 512".to_string());
                }
                res.page_size = Some(ps);
            }
            "--verbose" => res.verbose = true,
            "--print-events" | "--events" => res.print_events = true,
            "-h" | "--help" => {
                print_usage(&env::args().next().unwrap_or_else(|| "umadb-check".into()));
                std::process::exit(0);
            }
            other => {
                if res.path.is_none() {
                    res.path = Some(PathBuf::from(other));
                } else {
                    return Err(format!("Unexpected argument: {other}"));
                }
            }
        }
    }
    if res.path.is_none() {
        return Err("Missing path".to_string());
    }
    Ok(res)
}

fn main() {
    if let Err(e) = real_main() {
        match e {
            DCBError::InternalError(s) => eprintln!("Error: {}", s),
            other => eprintln!("Error: {}", other),
        }
        std::process::exit(2);
    }
}

fn real_main() -> DCBResult<()> {
    let args = parse_args().map_err(|e| DCBError::InternalError(e))?;

    let path = args.path.unwrap();
    let p = if path.is_dir() {
        path.join(DEFAULT_DB_FILENAME)
    } else {
        path
    };

    // Quick existence check so we can fail fast with a friendly message
    if !p.exists() {
        return Err(DCBError::InternalError(format!(
            "Database file not found: {}",
            p.display()
        )));
    }

    let page_size = args.page_size.unwrap_or(DEFAULT_PAGE_SIZE);

    // Open MVCC in normal mode; the library controls file access and reads.
    // No writes are performed by this tool.
    let mvcc = Mvcc::new(&p.as_path(), page_size, args.verbose)?;

    // Progressive report prelude
    println!("UmaDB file: {}", p.display());
    println!("Page size: {} bytes", page_size);
    println!("Stage 1: Checking headers...");

    // Prepare counters and buckets before any page reads
    let mut counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut event_leaf_page_ids: Vec<PageID> = Vec::new();
    let mut deser_errors: Vec<String> = Vec::new();
    let mut event_errors: Vec<String> = Vec::new();
    let mut header_errors: Vec<String> = Vec::new();
    let mut header_infos: Vec<String> = Vec::new();
    // Collect raw bytes of TagsLeaf pages that fail to deserialize for Stage 4 forensic analysis
    struct ForensicPage { page_id: u64, bytes: Vec<u8>, source: &'static str }
    let mut forensic_tags_pages: Vec<ForensicPage> = Vec::new();

    // Bootstrap: attempt to deserialize header pages 0 and 1 before reading the latest header
    for pid in 0..=1u64 {
        let page_id = PageID(pid);
        match mvcc.pager.read_page_mmap_slice(page_id) {
            Ok(mapped) => {
                let bytes = mapped.as_slice();
                match Page::deserialize(page_id, bytes) {
                    Ok(page) => {
                        let t = page.node.type_name();
                        *counts.entry(t).or_insert(0) += 1;
                        match &page.node {
                            Node::EventLeaf(_) => {
                                event_leaf_page_ids.push(page_id);
                            }
                            Node::Header(h) => {
                                let flags_str = if h.tracking_root_page_id.0 != 0 {
                                    "HAS_TRACKING_ROOT_ID"
                                } else {
                                    "none"
                                };
                                header_infos.push(format!(
                                    "Header page {}: tsn={} next_page_id={} next_position={} schema_version={} flags={}",
                                    pid, h.tsn.0, h.next_page_id.0, h.next_position.0, h.schema_version, flags_str
                                ));
                            }
                            _ => {}
                        }
                        if args.verbose { println!("ok page_id={} type={} (bootstrap)", pid, t); }
                    }
                    Err(err) => {
                        let type_str = type_name_from_byte(bytes[0]).unwrap_or("unknown");
                        *counts.entry(type_str).or_insert(0) += 1;
                        let msg = match &err {
                            DCBError::DeserializationError(_) => format!("Page {}: {} {:?}", pid, type_str, err),
                            DCBError::DatabaseCorrupted(s) => format!("Page {}: {} {}", pid, type_str, s),
                            other => format!("Page {}: {} {:?}", pid, type_str, other),
                        };
                        if args.verbose { eprintln!("{}", msg); }
                        header_errors.push(msg);
                    }
                }
            }
            Err(e) => {
                let msg = format!("Page {}: unknown I/O error while reading header page: {}", pid, e);
                if args.verbose { eprintln!("{}", msg); }
                deser_errors.push(msg);
            }
        }
    }

    // Now read the latest header as usual
    let (header_page_id, header) = mvcc.get_latest_header()?;

    let total_pages = header.next_page_id.0;
    if args.verbose {
        eprintln!(
            "Header at page {:?}: next_page_id = {}, next_position = {}",
            header_page_id, header.next_page_id.0, header.next_position.0
        );
        eprintln!(
            "Root pages: free_lists = {:?}, events = {:?}, tags = {:?}, tracking = {:?}",
            header.free_lists_tree_root_id,
            header.events_tree_root_id,
            header.tags_tree_root_id,
            header.tracking_root_page_id
        );
    }
    // Stage 1 results
    if !header_infos.is_empty() {
        for info in &header_infos {
            println!("{}", info);
        }
    }
    println!("Latest header: page={} tsn={} next_page_id={} next_position={} schema_version={} flags={}",
        header_page_id.0,
        header.tsn.0,
        header.next_page_id.0,
        header.next_position.0,
        header.schema_version,
        if header.tracking_root_page_id.0 != 0 { "HAS_TRACKING_ROOT_ID" } else { "none" }
    );
    if header_errors.is_empty() {
        println!("Headers: OK");
    } else {
        println!("Header errors:");
        for e in &header_errors { println!("  - {}", e); }
    }

    // Track actual last scanned page id for reporting (will be updated if we read beyond header)
    let mut last_scanned_pid: u64 = total_pages.saturating_sub(1);

    // Stage 2 intention
    println!("Stage 2: Checking page deserialisation...");

    // Main scan: process allocated pages starting from 2 to avoid double-processing headers
    if total_pages > 2 {
        for pid in 2..total_pages {
            let page_id = PageID(pid);
            match mvcc.read_page(page_id) {
                Ok(page) => {
                    let t = page.node.type_name();
                    *counts.entry(t).or_insert(0) += 1;
                    // Track EventLeaf page IDs for later probing if events are missing
                    if let Node::EventLeaf(_) = &page.node {
                        event_leaf_page_ids.push(page_id);
                    }
                    if args.verbose {
                        println!("ok page_id={} type={}", pid, t);
                    }
                }
                Err(err) => {
                    // Try to peek the page header to guess the node type
                    let mapped_opt = mvcc.pager.read_page_mmap_slice(page_id).ok();
                    let type_hint = mapped_opt.as_ref().and_then(|m| type_name_from_byte(m.as_slice()[0]));
                    let type_str = type_hint.unwrap_or("unknown");
                    // Include failed pages in counts under their hinted type (or "unknown")
                    *counts.entry(type_str).or_insert(0) += 1;
                    // If this looks like a TagsLeaf, retain the raw page bytes for Stage 4 forensic analysis
                    if type_str == "TagsLeaf" {
                        if let Some(mapped) = mapped_opt {
                            forensic_tags_pages.push(ForensicPage { page_id: pid, bytes: mapped.as_slice().to_vec(), source: "within-header" });
                        }
                    }
                    let msg = match &err {
                        DCBError::DeserializationError(_) => {
                            // Show the variant and message using Debug formatting
                            format!("Page {}: {} {:?}", pid, type_str, err)
                        }
                        DCBError::DatabaseCorrupted(s) => {
                            // Keep only the human-readable message for corruption
                            format!("Page {}: {} {}", pid, type_str, s)
                        }
                        other => format!("Page {}: {} {:?}", pid, type_str, other),
                    };
                    if args.verbose {
                        eprintln!("{}", msg);
                    }
                    deser_errors.push(msg);
                }
            }
        }
    }

    // Extended scan: continue reading pages beyond header.next_page_id up to actual file length
    // Stop when a full page of zeros is encountered or when the file ends.
    let reader_file = mvcc.pager.reader.clone();
    let file_len = reader_file.metadata().map_err(|e| DCBError::InternalError(format!("Failed to read file metadata: {}", e)))?.len();
    let page_size_u64 = page_size as u64;
    let mut pid = total_pages; // start at next_page_id
    let mut buf = vec![0u8; page_size];

    // Track next-page status and number of contiguous data pages beyond header before first zero page
    let mut next_page_status: Option<String> = None;
    let mut data_pages_beyond: u64 = 0;

    loop {
        let offset = pid.saturating_mul(page_size_u64);
        let end = offset + page_size_u64;
        if end > file_len {
            if pid == total_pages {
                next_page_status = Some("not present (EOF)".to_string());
            }
            break;
        }
        // Read page content safely without expanding mappings or the file
        match reader_file.read_at(&mut buf[..], offset as u64) {
            Ok(n) => {
                if n < page_size {
                    if pid == total_pages {
                        next_page_status = Some("not present (short read at EOF)".to_string());
                    }
                    break; // Short read at end-of-file; stop scanning
                }
            }
            Err(e) => {
                if pid == total_pages {
                    next_page_status = Some(format!("I/O error: {}", e));
                }
                deser_errors.push(format!("Page {}: unknown I/O error while reading beyond header: {}", pid, e));
                break;
            }
        }
        // If the page is all zeros, it hasn't been written; stop here
        if buf.iter().all(|&b| b == 0) {
            if pid == total_pages {
                next_page_status = Some("zero-filled (unused)".to_string());
            }
            break;
        }
        // Non-zero page: mark next-page status if this is the first beyond-header page
        if pid == total_pages && next_page_status.is_none() {
            next_page_status = Some("data present".to_string());
        }
        // Try to deserialize this page
        let page_id = PageID(pid);
        match Page::deserialize(page_id, &buf[..]) {
            Ok(page) => {
                let t = page.node.type_name();
                *counts.entry(t).or_insert(0) += 1;
                if let Node::EventLeaf(_) = &page.node {
                    event_leaf_page_ids.push(page_id);
                }
                if args.verbose { println!("ok page_id={} (beyond header) type={}", pid, t); }
            }
            Err(err) => {
                let type_hint = type_name_from_byte(buf[0]);
                let type_str = type_hint.unwrap_or("unknown");
                *counts.entry(type_str).or_insert(0) += 1;
                if type_str == "TagsLeaf" {
                    forensic_tags_pages.push(ForensicPage { page_id: pid, bytes: buf.clone(), source: "beyond-header" });
                }
                let msg = match &err {
                    DCBError::DeserializationError(_) => format!("Page {}: {} {:?}", pid, type_str, err),
                    DCBError::DatabaseCorrupted(s) => format!("Page {}: {} {}", pid, type_str, s),
                    other => format!("Page {}: {} {:?}", pid, type_str, other),
                };
                if args.verbose { eprintln!("{}", msg); }
                deser_errors.push(msg);
            }
        }
        last_scanned_pid = pid;
        data_pages_beyond += 1;
        pid += 1;
    }

    // Deserialisation section (Stage 2 findings)
    println!("Checking page deserialisation across pages 0..{}...", last_scanned_pid);
    // Report next-page status and how many contiguous data pages exist before first zero page
    let next_page_id_val = total_pages;
    let next_status = next_page_status.unwrap_or_else(|| "unknown".to_string());
    println!("Next page (page {}): {}", next_page_id_val, next_status);
    println!("Data pages beyond header.next_page_id before first zero page: {}", data_pages_beyond);
    println!("Counts by node type:");
    for (k, v) in counts.iter() {
        println!("  {:>22}: {}", k, v);
    }
    if deser_errors.is_empty() {
        println!("Deserialisation: OK");
    } else {
        println!("Deserialisation errors:");
        for e in &deser_errors {
            println!("  - {}", e);
        }
    }

    // Stage 3 intention
    println!("Stage 3: Checking events...");
    if args.print_events {
        println!("Printing events found (position-ordered):");
    }

    // After page scan, attempt higher-level integrity: event sequence vs next_position
    let mut last_event_pos: u64 = 0;
    let mut start = Some(Position(1));
    let mut total_events_read: u64 = 0;
    // Cache of events by position for Stage 4 analysis
    let mut events_by_position: HashMap<u64, DCBEvent> = HashMap::new();
    let empty_dirty: HashMap<PageID, Page> = HashMap::new();
    loop {
        // Read a batch of events sequentially using empty query (all events)
        match read_conditional(
            &mvcc,
            &empty_dirty,
            header.events_tree_root_id,
            header.tags_tree_root_id,
            DCBQuery { items: vec![] },
            start,
            false,
            Some(1024),
            false,
        ) {
            Ok(batch) => {
                if batch.is_empty() {
                    break;
                }
                if args.print_events {
                    for ev in &batch {
                        // Build tags list string
                        let tags_str = if ev.event.tags.is_empty() {
                            String::from("[]")
                        } else {
                            let mut s = String::from("[");
                            for (i, t) in ev.event.tags.iter().enumerate() {
                                if i > 0 { s.push_str(", "); }
                                s.push_str(t);
                            }
                            s.push(']');
                            s
                        };
                        // Build UUID string (if any)
                        let uuid_str = ev.event.uuid.as_ref().map(|u| u.to_string()).unwrap_or_else(|| "none".to_string());
                        // Hex-encode data
                        let mut hex = String::with_capacity(ev.event.data.len() * 2);
                        for b in &ev.event.data {
                            hex.push_str(&format!("{:02x}", b));
                        }
                        println!("Event {}:", ev.position);
                        println!("  type: {}", ev.event.event_type);
                        println!("  tags: {}", tags_str);
                        println!("  uuid: {}", uuid_str);
                        println!("  data_len: {}", ev.event.data.len());
                        println!("  data: {}", hex);
                    }
                }
                total_events_read += batch.len() as u64;
                for ev in &batch {
                    // Cache event by position for Stage 4
                    events_by_position.insert(ev.position, DCBEvent { event_type: ev.event.event_type.clone(), data: ev.event.data.clone(), tags: ev.event.tags.clone(), uuid: ev.event.uuid });
                    last_event_pos = ev.position;
                }
                // next start is last + 1
                start = Some(Position(last_event_pos + 1));
            }
            Err(e) => {
                event_errors.push(format!(
                    "Event sequence traversal failed: {:?}",
                    e
                ));
                break;
            }
        }
    }

    // Compare last event position to header.next_position - 1
    let header_last = header.next_position.0.saturating_sub(1);

    // New: validate we can get the last event directly using the events tree (backwards read)
    let mut last_by_tree: Option<u64> = None;
    match read_conditional(
        &mvcc,
        &empty_dirty,
        header.events_tree_root_id,
        header.tags_tree_root_id,
        DCBQuery { items: vec![] },
        None,              // no start -> let iterator pick end when backwards
        true,              // backwards: start from the end
        Some(1),           // only need the last event
        false,
    ) {
        Ok(mut v) => {
            if let Some(ev) = v.pop() {
                last_by_tree = Some(ev.position);
            }
        }
        Err(e) => {
            event_errors.push(format!(
                "Direct last-event lookup via events tree failed: {:?}",
                e
            ));
        }
    }

    // events_ok1: sequential scan vs header
    let events_ok1 = last_event_pos == header_last;
    if !events_ok1 {
        event_errors.push(format!(
            "Event sequence mismatch: header.next_position={} implies last={}, but events end at {} (events read: {})",
            header.next_position.0,
            header_last,
            last_event_pos,
            total_events_read
        ));
    } else if args.verbose {
        eprintln!(
            "Event sequence check OK: last_event_pos={} matches header.next_position-1",
            last_event_pos
        );
    }

    // events_ok2: direct-tree last vs header
    let events_ok2 = match last_by_tree {
        Some(p) => p == header_last,
        None => false,
    };
    if !events_ok2 {
        match last_by_tree {
            Some(p) => event_errors.push(format!(
                "Events tree last-event mismatch: header.next_position={} implies last={}, but direct-tree read returned {}",
                header.next_position.0,
                header_last,
                p
            )),
            None => event_errors.push("Events tree last-event lookup returned no events while header indicates there should be some".to_string()),
        }
    } else if args.verbose {
        eprintln!(
            "Direct events-tree last-event check OK: {}",
            header_last
        );
    }

    // Also compare sequential scan vs direct-tree result if both present
    if let Some(p) = last_by_tree {
        if p != last_event_pos {
            event_errors.push(format!(
                "Mismatch between sequential scan last ({}) and direct-tree last ({})",
                last_event_pos, p
            ));
        }
    }

    let events_ok = events_ok1 && events_ok2 && last_by_tree.map(|p| p == last_event_pos).unwrap_or(false);

    // If there are missing events, probe EventLeaf pages to see if the missing positions are present there
    let mut probe_findings: Vec<String> = Vec::new();
    if !events_ok {
        let missing_start = last_event_pos.saturating_add(1);
        let missing_end = header_last; // inclusive
        if missing_start <= missing_end {
            if args.verbose {
                eprintln!(
                    "Probing EventLeaf pages for missing positions in range {}..={} ({} positions) ...",
                    missing_start,
                    missing_end,
                    missing_end - missing_start + 1
                );
            }
            // Iterate EventLeaf page IDs that successfully deserialized earlier
            for pid in &event_leaf_page_ids {
                match mvcc.read_page(*pid) {
                    Ok(page) => {
                        if let Node::EventLeaf(node) = page.node {
                            let mut matches: Vec<u64> = Vec::new();
                            for k in node.keys.iter() {
                                let pos = k.0;
                                if pos >= missing_start && pos <= missing_end {
                                    matches.push(pos);
                                }
                            }
                            if !matches.is_empty() {
                                matches.sort_unstable();
                                let total = matches.len();
                                let show = usize::min(total, 10);
                                let sample: Vec<String> = matches.iter().take(show).map(|p| p.to_string()).collect();
                                let suffix = if total > show { format!(" (showing first {} of {})", show, total) } else { String::new() };
                                probe_findings.push(format!(
                                    "Page {}: contains {} missing positions{}: {}",
                                    pid.0,
                                    total,
                                    if suffix.is_empty() { "" } else { &suffix },
                                    sample.join(", ")
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        if args.verbose {
                            eprintln!("Skipping EventLeaf probe for page {} due to error: {:?}", pid.0, e);
                        }
                    }
                }
            }
            if probe_findings.is_empty() {
                probe_findings.push(format!(
                    "No EventLeaf pages were found containing positions in the missing range {}..={}.",
                    missing_start, missing_end
                ));
            }
        }
    }


    // Event sequence section
    println!("Checking event sequence vs header.next_position...");
    if events_ok && event_errors.is_empty() {
        println!(
            "Events: {} (last position {}) checks out against header.next_position={}",
            total_events_read,
            last_event_pos,
            header.next_position.0
        );
    } else {
        println!("Event sequence errors:");
        for e in &event_errors {
            println!("  - {}", e);
        }
        // If we probed EventLeaf pages, report findings
        if !probe_findings.is_empty() {
            println!("Looking for missing events inside EventLeaf pages:");
            for f in &probe_findings {
                println!("  - {}", f);
            }
        }
    }

    // Stage 4 intention and findings: Forensic analysis of failed TagsLeaf pages
    println!("Stage 4: Forensic analysis of failed TagsLeaf pages...");
    if forensic_tags_pages.is_empty() {
        println!("No failed TagsLeaf pages to analyze.");
    } else {
        // Helper to parse a TagsLeaf body tolerantly with a given key width
        fn forensic_parse_tags_leaf(body: &[u8], keyw: usize) -> (usize, Vec<([u8; TAG_HASH_LEN], Vec<u64>)>, String) {
            let mut notes = String::new();
            if body.len() < 2 { notes.push_str("too short (<2 bytes) for keys_len; "); return (0, vec![], notes); }
            let keys_len = u16::from_le_bytes([body[0], body[1]]) as usize;
            let mut offset = 2usize;
            let keys_bytes = keys_len.saturating_mul(keyw);
            if body.len() < offset + keys_bytes { notes.push_str("not enough bytes for keys; "); return (0, vec![], notes); }
            let mut keys: Vec<[u8; TAG_HASH_LEN]> = Vec::with_capacity(keys_len);
            for i in 0..keys_len {
                let start = offset + i * keyw;
                let mut k = [0u8; TAG_HASH_LEN];
                let end = start + keyw;
                if end <= body.len() {
                    k[..keyw].copy_from_slice(&body[start..end]);
                } else {
                    notes.push_str(&format!("key {} truncated; ", i));
                    let avail = body.len().saturating_sub(start);
                    k[..avail].copy_from_slice(&body[start..start+avail]);
                }
                keys.push(k);
            }
            offset += keys_bytes;
            let mut out: Vec<([u8; TAG_HASH_LEN], Vec<u64>)> = Vec::new();
            for (i, kh) in keys.into_iter().enumerate() {
                if offset + 10 > body.len() {
                    notes.push_str(&format!("value header for key {} truncated; ", i));
                    break;
                }
                let _root = u64::from_le_bytes(body[offset..offset+8].try_into().unwrap());
                offset += 8;
                let plen = u16::from_le_bytes(body[offset..offset+2].try_into().unwrap()) as usize;
                offset += 2;
                let need = plen.saturating_mul(8);
                if offset + need > body.len() {
                    notes.push_str(&format!("positions for key {} truncated; ", i));
                    // read as many complete u64s as we have
                    let avail = (body.len().saturating_sub(offset)) / 8;
                    let mut pos: Vec<u64> = Vec::with_capacity(avail);
                    for j in 0..avail { let p = offset + j*8; pos.push(u64::from_le_bytes(body[p..p+8].try_into().unwrap())); }
                    out.push((kh, pos));
                    break;
                } else {
                    let mut pos: Vec<u64> = Vec::with_capacity(usize::min(plen, 4096));
                    for j in 0..plen { let p = offset + j*8; pos.push(u64::from_le_bytes(body[p..p+8].try_into().unwrap())); }
                    offset += need;
                    out.push((kh, pos));
                }
            }
            (out.len(), out, notes)
        }

        // Helper to analyze recovered entries against cached events using a specific hash width
        fn analyze_entries(
            entries: &Vec<([u8; TAG_HASH_LEN], Vec<u64>)>,
            used_w: usize,
            events_by_position: &HashMap<u64, DCBEvent>,
        ) -> (usize, usize, usize, Vec<String>) {
            let mut total_positions = 0usize;
            let mut pos_with_event = 0usize;
            let mut hash_matches = 0usize;
            let mut mismatches: Vec<String> = Vec::new();

            for (kh, positions) in entries.iter() {
                total_positions += positions.len();
                for pos in positions {
                    if let Some(ev) = events_by_position.get(pos) {
                        pos_with_event += 1;
                        let mut matched = false;
                        for t in &ev.tags {
                            if used_w == 8 {
                                let h = umadb_core::db::tag_to_hash_crc64(t);
                                if &h[..8] == &kh[..8] { matched = true; break; }
                            } else {
                                let h = normalize_tag_hash_for_current_width(tag_to_hash(t));
                                if &h == kh { matched = true; break; }
                            }
                        }
                        if matched { hash_matches += 1; } else if mismatches.len() < 5 {
                            mismatches.push(format!(
                                "pos {} has event with tags {:?} but none hash to key {:02x?}",
                                pos,
                                ev.tags,
                                &kh[..used_w]
                            ));
                        }
                    }
                }
            }
            (total_positions, pos_with_event, hash_matches, mismatches)
        }

        // Analyze each forensic page
        for fp in &forensic_tags_pages {
            // Extract page body from full page bytes (strip 9-byte page header)
            let bytes = &fp.bytes;
            let body = if bytes.len() >= 9 { &bytes[9..] } else { bytes.as_slice() };

            // Parse with both widths independently
            let cur_w = get_tag_key_width();
            let (_n_cur, cur_entries, cur_notes) = forensic_parse_tags_leaf(body, cur_w);
            let (_n_legacy, legacy_entries, legacy_notes) = forensic_parse_tags_leaf(body, 8);

            println!("Forensic TagsLeaf page {} ({}):", fp.page_id, fp.source);

            // Report current width analysis
            let (tpos_cur, pe_cur, hm_cur, mm_cur) = analyze_entries(&cur_entries, cur_w, &events_by_position);
            println!("  parse width={}: recovered keys={} total_positions={}", cur_w, cur_entries.len(), tpos_cur);
            if !cur_notes.is_empty() { println!("  notes[width={}]: {}", cur_w, cur_notes); }
            println!("  positions with readable events: {}", pe_cur);
            println!("  events where some tag hashes matched recovered key: {}", hm_cur);
            if !mm_cur.is_empty() {
                println!("  sample mismatches (up to 5) [width={}]:", cur_w);
                for m in mm_cur { println!("    - {}", m); }
            }

            // Report legacy width=8 analysis
            let (tpos_legacy, pe_legacy, hm_legacy, mm_legacy) = analyze_entries(&legacy_entries, 8, &events_by_position);
            println!("  parse width=8: recovered keys={} total_positions={}", legacy_entries.len(), tpos_legacy);
            if !legacy_notes.is_empty() { println!("  notes[width=8]: {}", legacy_notes); }
            println!("  positions with readable events: {}", pe_legacy);
            println!("  events where some tag hashes matched recovered key: {}", hm_legacy);
            if !mm_legacy.is_empty() {
                println!("  sample mismatches (up to 5) [width=8]:");
                for m in mm_legacy { println!("    - {}", m); }
            }

            // Additionally, attempt core deserialization using schema 0 key width (8 bytes)
            let prev_w = get_tag_key_width();
            set_tag_key_width(8);
            let core_res = TagsLeafNode::from_slice(body);
            // Restore width immediately
            set_tag_key_width(prev_w);
            match core_res {
                Ok(node) => {
                    let keys = node.keys.len();
                    let total_positions: usize = node.values.iter().map(|v| v.positions.len()).sum();
                    println!("  core deserialise with schema0 key width=8: OK (keys={} total_positions={})", keys, total_positions);
                }
                Err(e) => {
                    println!("  core deserialise with schema0 key width=8: failed: {:?}", e);
                }
            }
        }
    }

    // Final outcome
    if deser_errors.is_empty() && event_errors.is_empty() {
        println!("Integrity: OK");
        Ok(())
    } else {
        Err(DCBError::InternalError("Integrity check failed".to_string()))
    }
}
