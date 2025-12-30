use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;

use tempfile::tempdir;
use umadb_core::db::UmaDB;
use umadb_core::tags_tree_nodes::get_tag_key_width;
use umadb_dcb::{DCBEvent, DCBEventStoreSync, DCBQuery, DCBQueryItem};

// This test verifies that a legacy schema version 0 (with 64-bit tag hashes)
// can be opened, queried, and appended to by the current code.
// Steps:
// 1) Decompress fixtures/uma.db.schema0.zst into a temp file.
// 2) Open UmaDB on that file path.
// 3) Read all events; collect all encountered tags.
// 4) For each existing tag, read events by that tag and ensure non-empty.
// 5) Append new events with new tags.
// 6) Re-read all events and verify new ones are present; also verify tag queries work for new tags.
// 7) Assert that the on-disk tag key width detected for this DB is 8 bytes (legacy 64-bit hashes).
#[test]
fn schema_version_0_roundtrip_read_write_and_tags() -> Result<(), Box<dyn std::error::Error>> {
    // Locate the compressed fixture
    let mut fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    fixture.push("fixtures/uma.db.schema_version_0.zst");

    // Decompress to a temp file path (the DB path we will open)
    let tmpdir = tempdir()?;
    let db_path = tmpdir.path().join("uma.db.schema0");

    let input = File::open(&fixture)?;
    let reader = BufReader::new(input);
    let mut decoder = zstd::stream::read::Decoder::new(reader)?;

    let output = File::create(&db_path)?;
    let mut writer = BufWriter::new(output);
    std::io::copy(&mut decoder, &mut writer)?;
    writer.flush()?;

    // Open the DB at the decompressed path
    let db = UmaDB::new(&db_path)?;

    // The MVCC should have detected legacy schema and set key width to 4
    assert_eq!(8, get_tag_key_width(), "Legacy DB should use 64-bit (8-byte) tag keys on disk");

    // Read all events; collect tags and remember head
    let (all_events, head) = db.read_with_head(None, None, false, None)?;
    assert!(head.is_some(), "Expected head for non-empty legacy DB");

    // Collect all tags present in the DB
    use std::collections::{BTreeSet};
    let mut existing_tags: BTreeSet<String> = BTreeSet::new();
    let original_count = all_events.len();
    for se in &all_events {
        for t in &se.event.tags {
            existing_tags.insert(t.clone());
        }
    }

    // For each tag, query events filtered by that tag and ensure we get >=1 result
    // First: pure index path (all items have tags) to validate index-based search on legacy DB
    for tag in &existing_tags {
        let query = DCBQuery::new().item(DCBQueryItem::new().tags([tag.clone()]));
        let (tag_events, _head2) = db.read_with_head(Some(query), None, false, None)?;
        assert!(
            !tag_events.is_empty(),
            "Expected events via index path for existing tag '{}' in legacy DB",
            tag
        );
    }

    // Second: force sequential fallback path while still filtering by the tag, should also work
    let build_tag_query_fallback = |t: &str| {
        DCBQuery::new()
            .item(DCBQueryItem::new().tags([t]))
            .item(DCBQueryItem::new().types(["__no_such_type__"]))
    };

    for tag in &existing_tags {
        let query = build_tag_query_fallback(tag);
        let (tag_events, _head2) = db.read_with_head(Some(query), None, false, None)?;
        assert!(
            !tag_events.is_empty(),
            "Expected events via fallback path for existing tag '{}' in legacy DB",
            tag
        );
    }

    // Append a few new events with brand-new tags
    let new_tags = vec!["compat-new-A", "compat-new-B", "compat-new-C"];
    for (i, nt) in new_tags.iter().enumerate() {
        let ev = DCBEvent {
            event_type: format!("CompatEvent{}", i + 1),
            data: format!("payload-{}", i + 1).into_bytes(),
            tags: vec![nt.to_string()],
            uuid: None,
        };
        let _pos = db.append(vec![ev], None)?;
    }

    // Re-read all events; ensure count increased by number of new events
    let (all_after, head_after) = db.read_with_head(None, None, false, None)?;
    assert!(head_after.is_some());
    assert_eq!(original_count + new_tags.len(), all_after.len());

    // Verify per-tag queries for the new tags return exactly one event
    for (i, nt) in new_tags.iter().enumerate() {
        let query = DCBQuery::new().item(DCBQueryItem::new().tags([*nt]));
        let (events_for_tag, _h) = db.read_with_head(Some(query), None, false, None)?;
        assert_eq!(
            1,
            events_for_tag.len(),
            "Expected exactly one event for new tag '{}' (CompatEvent{})",
            nt,
            i + 1
        );
    }

    // Finally, verify the tag key width we operate under remains 8 for this opened DB
    assert_eq!(8, get_tag_key_width());

    Ok(())
}
