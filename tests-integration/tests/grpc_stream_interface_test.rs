use futures::StreamExt;
use std::net::TcpListener;
use tempfile::tempdir;
use tokio::time::{Duration as TokioDuration, sleep};
use umadb_client::UmaDbClient;
use umadb_dcb::{DcbEvent, DcbEventStoreAsync, DcbEventStoreSync};
use umadb_server::start_server; // For .next()

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_async_stream_interface_test() {
    // Arrange: start a gRPC server
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let addr_str = format!("{}", addr);
    let addr_http = format!("http://{}", addr);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        let _ = start_server(db_path, &addr_str, shutdown_rx).await;
    });

    sleep(TokioDuration::from_millis(200)).await;

    // Connect async client
    let client = UmaDbClient::new(addr_http.clone())
        .connect_async()
        .await
        .expect("client connect");

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
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}

#[test]
fn grpc_sync_iterator_interface_test() {
    // Pick a free port
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let addr_noscheme = format!("{}", addr);
    let addr_with_scheme = format!("http://{}", addr);

    // Build a multi-threaded runtime for server
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let temp_dir = tempdir().unwrap();
    let data_dir = temp_dir.path().to_path_buf();
    rt.spawn(async move {
        let _ = start_server(data_dir, &addr_noscheme, shutdown_rx).await;
    });

    // Connect the sync client
    let client = {
        use std::{thread, time::Duration};
        let mut attempts = 0;
        loop {
            match UmaDbClient::new(addr_with_scheme.clone()).connect() {
                Ok(c) => break c,
                Err(_e) => {
                    attempts += 1;
                    if attempts >= 50 {
                        panic!("failed to connect to grpc server after retries");
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }
    };

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

    // Shutdown server
    let _ = shutdown_tx.send(());
}
