use std::env;
use std::hint::black_box;
use std::time::Instant;
use tempfile::tempdir;
use umadb_core::db::UmaDB;
use umadb_dcb::{DCBAppendCondition, DCBEvent, DCBEventStoreSync, DCBQuery, DCBQueryItem};

// This test is intended for profiling hotspots when appending events.
// It can append one event per call or many events per call, to profile both per-call overhead
// and per-event work inside the unconditional append path.
//
// How to run (ignored by default):
//   cargo test --test unconditional_append_hotspot -- --ignored --nocapture
//
// To control how many append calls run (total events = CALLS_PER_RUN * EVENTS_PER_CALL):
//   CALLS_PER_RUN=10000 cargo test --test unconditional_append_hotspot -- --ignored --nocapture
//
// To control the number of events per append call (default 100):
//   EVENTS_PER_CALL=100 cargo test --test unconditional_append_hotspot -- --ignored --nocapture
//
// You can use a profiler around the single test process, e.g.:
//   Linux perf:   perf record -- cargo test --test unconditional_append_hotspot -- --ignored --nocapture
//   macOS (DTrace/instruments): instruments -t Time\ Profiler target/debug/deps/unconditional_append_hotspot-*
//   flamegraph:  cargo flamegraph --test unconditional_append_hotspot -- --ignored --nocapture
#[test]
#[ignore]
fn profile_event_store_append() {
    // Control the number of append calls; total events = CALLS_PER_RUN * EVENTS_PER_CALL
    let calls_per_run: usize = env::var("CALLS_PER_RUN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10000);

    // Number of events per append call (default 100)
    let events_per_call: usize = env::var("EVENTS_PER_CALL")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(1);

    // Number of tags per event (1 or 2 is realistic). Using 2 exercises more indexing work.
    let tags_per_event: usize = env::var("TAGS_PER_EVENT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let total_events: usize = calls_per_run.saturating_mul(events_per_call);

    let tmp = tempdir().expect("create temp dir");
    let store = UmaDB::new(tmp.path()).expect("open event store");

    // Time the entire loop of batched appends.
    let start = Instant::now();
    let mut last: u64 = 0;

    let mut appended_calls: usize = 0;

    let data_size: usize = env::var("DATA_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);

    let payload = vec![0u8; data_size];
    for i in 0..calls_per_run {
        let mut events: Vec<DCBEvent> = Vec::with_capacity(events_per_call);
        for j in 0..events_per_call {
            let idx = i + j;

            // Generate tags. Tag content is varied to avoid overly uniform hashing.
            let mut tags = Vec::with_capacity(tags_per_event);
            for k in 0..tags_per_event {
                tags.push(format!("tag-{j}-{k}"));
            }

            let event = DCBEvent {
                event_type: format!("Type{}", idx % 16),
                data: payload.clone(),
                tags,
                uuid: None,
            };
            events.push(event);
        }
        let condition = DCBAppendCondition {
            fail_if_events_match: DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec!["foo".to_string()],
                }],
            },
            after: Some(0),
        };

        last = black_box(
            store
                .append(black_box(events), black_box(Some(condition)), None)
                .expect("append ok"),
        );
        appended_calls += 1;
    }

    let elapsed = start.elapsed();

    let avg_per_call_us = (elapsed.as_secs_f64() * 1e6) / (appended_calls as f64);
    let avg_per_event_us = (elapsed.as_secs_f64() * 1e6) / (total_events as f64);
    let events_per_second = total_events as f64 / elapsed.as_secs_f64();

    // Print some stats so --nocapture shows useful info during profiling sessions.
    eprintln!(
        "profile_event_store_append: appended {} events in {} calls ({} events/call); last position {}; elapsed = {:.3?} (avg {:.3} µs/call, {:.3} µs/event, {:.1} events/sec)",
        total_events,
        appended_calls,
        events_per_call,
        last,
        elapsed,
        avg_per_call_us,
        avg_per_event_us,
        events_per_second,
    );

    // Keep tempdir until end of test to ensure DB flush completes before directory removal.
    drop(store);
    drop(tmp);
}
