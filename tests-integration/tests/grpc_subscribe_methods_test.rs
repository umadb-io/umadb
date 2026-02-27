use std::net::TcpListener;
use tempfile::tempdir;
use tokio::time::{Duration as TokioDuration, sleep};
use umadb_client::UmaDBClient;
use umadb_dcb::{DCBEvent, DCBEventStoreAsync, DCBEventStoreSync};
use umadb_server::start_server;

// ----------------------
// Async subscribe() tests
// ----------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_async_subscribe_catch_up_and_continue() {
    // Arrange: start a gRPC server backed by a temporary directory
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let addr = "127.0.0.1:50076"; // dedicated port for this test
    let addr_http = format!("http://{}", addr);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        let _ = start_server(db_path, &addr, shutdown_rx).await;
    });

    sleep(TokioDuration::from_millis(200)).await;

    // Connect async client
    let client = UmaDBClient::new(addr_http.clone())
        .connect_async()
        .await
        .expect("client connect");

    // Append initial events
    let initial_count = 30usize;
    let initial_events: Vec<DCBEvent> = (0..initial_count as u64)
        .map(|i| DCBEvent {
            event_type: "AsyncSubEvent".to_string(),
            data: format!("init-{i}").into_bytes(),
            tags: vec!["grpc-async-sub".to_string()],
            uuid: None,
        })
        .collect();
    let _ = client
        .append(initial_events, None, None)
        .await
        .expect("append initial events");

    // Act: use explicit subscribe() helper (catch-up phase)
    let mut resp = client
        .subscribe(None, None)
        .await
        .expect("subscribe stream");

    let mut received = 0usize;
    while received < initial_count {
        let batch = resp.next_batch().await.expect("next_batch catching up");
        if batch.is_empty() {
            panic!("subscription ended unexpectedly while catching up");
        }
        for se in batch.into_iter() {
            assert_eq!(se.position, 1 + received as u64);
            received += 1;
            if received == initial_count {
                break;
            }
        }
    }

    // Append more events and ensure they arrive on the same subscription
    let new_count = 12usize;
    let new_events: Vec<DCBEvent> = (0..new_count as u64)
        .map(|i| DCBEvent {
            event_type: "AsyncSubEvent2".to_string(),
            data: format!("new-{i}").into_bytes(),
            tags: vec!["grpc-async-sub".to_string()],
            uuid: None,
        })
        .collect();
    let _ = client
        .append(new_events, None, None)
        .await
        .expect("append new events");

    let mut received_new = 0usize;
    while received_new < new_count {
        let batch = resp.next_batch().await.expect("next_batch for new events");
        if batch.is_empty() {
            panic!("subscription ended unexpectedly before receiving new events");
        }
        for se in batch.into_iter() {
            received_new += 1;
            assert_eq!(se.position, (received + received_new) as u64);
            if received_new == new_count {
                break;
            }
        }
    }

    // Cleanup
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_async_subscribe_with_after_position() {
    // Arrange
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let addr = "127.0.0.1:50077";
    let addr_http = format!("http://{}", addr);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        let _ = start_server(db_path, &addr, shutdown_rx).await;
    });
    sleep(TokioDuration::from_millis(200)).await;

    let client = UmaDBClient::new(addr_http.clone())
        .connect_async()
        .await
        .expect("client connect");

    // Append A initial events
    let a = 20usize;
    let initial_events: Vec<DCBEvent> = (0..a as u64)
        .map(|i| DCBEvent {
            event_type: "AsyncSubStartEvent".to_string(),
            data: format!("init-{i}").into_bytes(),
            tags: vec!["grpc-async-sub-start".to_string()],
            uuid: None,
        })
        .collect();
    let _ = client
        .append(initial_events, None, None)
        .await
        .expect("append initial events");

    // Subscribe starting from A - K (catch up last K events)
    let k = 10usize;
    let after_position = Some((a - k) as u64); // positions start at 1; start after A-K to get exactly K
    let mut resp = client
        .subscribe(None, after_position)
        .await
        .expect("subscribe after start");

    let mut caught_up = 0usize;
    while caught_up < k {
        let batch = resp
            .next_batch()
            .await
            .expect("next_batch catching up from start");
        if batch.is_empty() {
            panic!("subscription ended unexpectedly during catch-up from start");
        }
        for se in batch.into_iter() {
            caught_up += 1;
            assert_eq!(se.position, (a - k + caught_up) as u64);
            if caught_up == k {
                break;
            }
        }
    }

    // Append M new events and ensure they arrive
    let m = 9usize;
    let new_events: Vec<DCBEvent> = (0..m as u64)
        .map(|i| DCBEvent {
            event_type: "AsyncSubStartEvent2".to_string(),
            data: format!("new-{i}").into_bytes(),
            tags: vec!["grpc-async-sub-start".to_string()],
            uuid: None,
        })
        .collect();
    let _ = client
        .append(new_events, None, None)
        .await
        .expect("append new events");

    let mut received_new = 0usize;
    while received_new < m {
        let batch = resp
            .next_batch()
            .await
            .expect("next_batch for new events after start");
        if batch.is_empty() {
            panic!("subscription ended unexpectedly before receiving new events after start");
        }
        for se in batch.into_iter() {
            received_new += 1;
            assert_eq!(se.position, (a - k + caught_up + received_new) as u64);

            if received_new == m {
                break;
            }
        }
    }

    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}

// ----------------------
// Sync subscribe() test
// ----------------------

#[test]
fn grpc_sync_subscribe_catch_up_and_continue() {
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

    // Prepare shutdown channel for the server
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    // Temporary directory for the database server
    let temp_dir = tempdir().unwrap();

    // Start the server
    let data_dir = temp_dir.path().to_path_buf();
    rt.spawn(async move {
        let _ = start_server(data_dir, &addr_noscheme, shutdown_rx).await;
    });

    // Connect the sync client (retry until server is ready)
    let client = {
        use std::{thread, time::Duration};
        let mut attempts = 0;
        loop {
            match UmaDBClient::new(addr_with_scheme.clone()).connect() {
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

    // Append initial events via sync API
    let initial_count = 20usize;
    let initial_events: Vec<DCBEvent> = (0..initial_count as u64)
        .map(|i| DCBEvent {
            event_type: "SyncSubEvent".to_string(),
            data: format!("init-{i}").into_bytes(),
            tags: vec!["grpc-sync-sub".to_string()],
            uuid: None,
        })
        .collect();
    let _ = client.append(initial_events, None, None).unwrap();

    // Subscribe explicitly
    let mut resp = client.subscribe(None, None).expect("sync subscribe stream");

    // Catch up
    let mut received = 0usize;
    while received < initial_count {
        let batch = resp.next_batch().expect("next_batch catching up");
        // Since it's a live subscription, batches should be non-empty here until we reach the target
        assert!(!batch.is_empty(), "empty batch while catching up");
        received += batch.len();
    }

    // Append more and ensure they arrive
    let new_count = 11usize;
    let new_events: Vec<DCBEvent> = (0..new_count as u64)
        .map(|i| DCBEvent {
            event_type: "SyncSubEvent2".to_string(),
            data: format!("new-{i}").into_bytes(),
            tags: vec!["grpc-sync-sub".to_string()],
            uuid: None,
        })
        .collect();
    let _ = client.append(new_events, None, None).unwrap();

    let mut received_new = 0usize;
    while received_new < new_count {
        let batch = resp.next_batch().expect("next_batch for new events");
        assert!(
            !batch.is_empty(),
            "empty batch before receiving all new events"
        );
        received_new += batch.len();
    }

    // Shutdown server
    let _ = shutdown_tx.send(());
}
