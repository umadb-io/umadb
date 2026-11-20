use futures::StreamExt;
use serial_test::serial;
use tests_integration::TempServer;
use umadb_client::UmaDbClient;
use umadb_dcb::{DcbEvent, DcbEventStoreAsync, DcbEventStoreSync};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn grpc_async_stream_interface_test() {
    let server = TempServer::start_insecure().await;
    let client = server.client()
        .connect_async()
        .await
        .expect("connect async client");

    // Append initial events
    let initial_count = 5usize;
    let initial_events: Vec<DcbEvent> = (0..initial_count as u64)
        .map(|i| DcbEvent {
            event_type: "AsyncStreamEvent".to_string(),
            data: format!("init-{i}").into_bytes(),
            tags: vec!["grpc-async-stream".to_string()],
            uuid: None,
            metadata: Vec::new(),
        })
        .collect();
    let _ = client
        .append(initial_events, None, None)
        .await
        .expect("append initial events");

    // Act: use subscribe() and treat it as a Stream (exercises poll_next)
    let mut resp = client
        .subscribe(None, None)
        .await
        .expect("subscribe stream");

    let mut received = 0usize;
    while received < initial_count {
        // This call exercises AsyncClientSubscribeResponse::poll_next
        let event = resp
            .next()
            .await
            .expect("stream ended unexpectedly")
            .expect("error in stream");

        assert_eq!(event.position, 1 + received as u64);
        received += 1;
    }

    // Test buffering: poll_next should handle multiple events in one SubscribeResponse
    // The server currently sends events as they come, but let's append a batch
    let batch_count = 3usize;
    let batch_events: Vec<DcbEvent> = (0..batch_count as u64)
        .map(|i| DcbEvent {
            event_type: "AsyncStreamEventBatch".to_string(),
            data: format!("batch-{i}").into_bytes(),
            tags: vec!["grpc-async-stream-batch".to_string()],
            uuid: None,
            metadata: Vec::new(),
        })
        .collect();
    let _ = client
        .append(batch_events, None, None)
        .await
        .expect("append batch events");

    for i in 0..batch_count {
        let event = resp
            .next()
            .await
            .expect("stream ended unexpectedly during batch")
            .expect("error in stream during batch");
        assert_eq!(event.position, (initial_count + 1 + i) as u64);
    }

    // Cleanup
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn grpc_sync_iterator_interface_test() {
    let server = TempServer::start_insecure().await;

    let http_url = server.url.clone();

    tokio::task::spawn_blocking(move || {
        // Build client with small batch_size so we can test next_batch
        let client = UmaDbClient::new(http_url)
            .connect()
            .expect("connect sync client");

        // Append some events
        let initial_count = 5usize;
        let initial_events: Vec<DcbEvent> = (0..initial_count as u64)
            .map(|i| DcbEvent {
                event_type: "SyncIterEvent".to_string(),
                data: format!("init-{i}").into_bytes(),
                tags: vec!["grpc-sync-iter".to_string()],
                uuid: None,
                metadata: Vec::new(),
            })
            .collect();
        let _ = client.append(initial_events, None, None).unwrap();

        // Subscribe and use as Iterator
        let resp = client.subscribe(None, None).expect("sync subscribe stream");

        // SyncClientSubscription implements Iterator
        let mut received = 0usize;
        for event_res in resp.take(initial_count) {
            let event = event_res.expect("error in iterator");
            assert_eq!(event.position, 1 + received as u64);
            received += 1;
        }
        assert_eq!(received, initial_count);
    }).await.unwrap();

    // Shutdown server
    server.shutdown().await;
}
