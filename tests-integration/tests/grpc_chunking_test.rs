/// Integration tests verifying that the server correctly chunks large gRPC responses.
///
/// Each test appends ~4 000 events with ~10 kB payloads (totalling ~40 MB of raw event
/// data), which is well above the 4 MiB gRPC message-size limit. The server must split
/// the events across multiple `ReadResponse` / `SubscribeResponse` messages; the client's
/// `next_batch()` API surfaces each message as one batch. The tests assert that:
///
/// 1. More than one batch is returned (chunking actually happened).
/// 2. No individual batch contains more events than the requested batch size (the
///    server's own DB-read batching limit was respected within every chunk).
/// 3. All appended events are eventually received (no data is lost).
use std::time::Duration;
use serial_test::serial;

use umadb_dcb::{DcbEvent, DcbEventStoreAsync};
use uuid::Uuid;
use tests_integration::TempServer;

/// Build a `DcbEvent` whose `data` payload is approximately `payload_bytes` bytes.
fn large_event(payload_bytes: usize) -> DcbEvent {
    DcbEvent::default()
        .event_type("LargeEvent")
        .tags(["chunk-test"])
        .data(vec![0xAB; payload_bytes])
        .uuid(Uuid::new_v4())
}

// ---------------------------------------------------------------------------
// Test: Read chunking
// ---------------------------------------------------------------------------

/// Appends `event_count` events each with `payload_bytes` bytes of data, then
/// reads them back. Asserts that more than one `next_batch()` call is needed,
/// that no individual batch exceeds `batch_size` events, and that the total
/// number of received events equals `event_count`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn read_response_is_chunked_when_payload_exceeds_grpc_limit() {

    const EVENT_COUNT: usize = 4_000;
    const PAYLOAD_BYTES: usize = 10_240; // ~10 kB per event → ~40 MB total
    const BATCH_SIZE: u32 = 1_000; // ~10MB larger than grpc 4MB message limit, so force chunking

    let server = TempServer::start_insecure().await;
    let client = server.client()
        .batch_size(BATCH_SIZE)
        .connect_async()
        .await
        .expect("connect async client");

    // Append all events (in smaller batches to avoid a single huge append RPC).
    for chunk in (0..EVENT_COUNT)
        .map(|_| large_event(PAYLOAD_BYTES))
        .collect::<Vec<_>>()
        .chunks(100)
    {
        client
            .append(chunk.to_vec(), None, None)
            .await
            .expect("append chunk");
    }

    // Read all events back.
    let mut resp = client
        .read(None, None, false, None)
        .await
        .expect("read all");

    let mut total_received = 0usize;
    let mut batch_count = 0usize;

    loop {
        let batch = resp.next_batch().await.expect("next_batch");
        if batch.is_empty() {
            break;
        }
        assert!(
            batch.len() < BATCH_SIZE as usize,
            "batch of {} events not less than batch_size {}",
            batch.len(),
            BATCH_SIZE
        );
        total_received += batch.len();
        batch_count += 1;
    }

    assert_eq!(
        total_received, EVENT_COUNT,
        "expected {EVENT_COUNT} events total, got {total_received}"
    );
    assert!(
        batch_count > 1,
        "expected multiple batches (chunking), but got only {batch_count}"
    );

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Test: Subscribe chunking
// ---------------------------------------------------------------------------

/// Same scenario but via the Subscribe RPC. All events are appended before the
/// subscription is opened (so they are served immediately as historical events),
/// and we use `next_batch_timeout` to drain the stream without waiting forever
/// once we have received all expected events.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn subscribe_response_is_chunked_when_payload_exceeds_grpc_limit() {
    const EVENT_COUNT: usize = 4_000;
    const PAYLOAD_BYTES: usize = 10_240; // ~10 kB per event → ~40 MB total
    const BATCH_SIZE: u32 = 1_000; // ~10MB larger than grpc 4MB message limit, so force chunking

    let server = TempServer::start_insecure().await;
    let client = server.client()
        .batch_size(BATCH_SIZE)
        .connect_async()
        .await
        .expect("connect async client");

    // Append all events before opening the subscription.
    for chunk in (0..EVENT_COUNT)
        .map(|_| large_event(PAYLOAD_BYTES))
        .collect::<Vec<_>>()
        .chunks(100)
    {
        client
            .append(chunk.to_vec(), None, None)
            .await
            .expect("append chunk");
    }

    // Open subscription from the beginning (after = None means from position 0).
    let mut sub = client
        .subscribe(None, None)
        .await
        .expect("subscribe");

    let mut total_received = 0usize;
    let mut batch_count = 0usize;
    let timeout = Duration::from_secs(5);

    // Drain the subscription until we have all events or we time out waiting for more.
    loop {
        match sub.next_batch_timeout(timeout).await {
            Ok(batch) if batch.is_empty() => break,
            Ok(batch) => {
                assert!(
                    batch.len() < BATCH_SIZE as usize,
                    "batch of {} events not less than batch_size {}",
                    batch.len(),
                    BATCH_SIZE
                );
                total_received += batch.len();
                batch_count += 1;
                if total_received >= EVENT_COUNT {
                    break;
                }
            }
            Err(umadb_dcb::DcbError::Timeout()) => break,
            Err(e) => panic!("unexpected subscription error: {e:?}"),
        }
    }

    assert_eq!(
        total_received, EVENT_COUNT,
        "expected {EVENT_COUNT} events total, got {total_received}"
    );
    assert!(
        batch_count > 1,
        "expected multiple batches (chunking), but got only {batch_count}"
    );

    server.shutdown().await;
}
