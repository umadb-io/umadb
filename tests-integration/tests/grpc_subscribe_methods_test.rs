use serial_test::serial;
use tests_integration::TempServer;
use umadb_client::UmaDbClient;
use umadb_dcb::{DcbEvent, DcbEventStoreAsync, DcbEventStoreSync};

// ----------------------
// Async subscribe() tests
// ----------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn grpc_async_subscribe_catch_up_and_continue() {
    let server = TempServer::start_insecure().await;
    let client = server.client()
        .connect_async()
        .await
        .expect("connect async client");

    // Append initial events
    let initial_count = 30usize;
    let initial_events: Vec<DcbEvent> = (0..initial_count as u64)
        .map(|i| DcbEvent {
            event_type: "AsyncSubEvent".to_string(),
            data: format!("init-{i}").into_bytes(),
            tags: vec!["grpc-async-sub".to_string()],
            uuid: None,
            metadata: Vec::new(),
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
    let new_events: Vec<DcbEvent> = (0..new_count as u64)
        .map(|i| DcbEvent {
            event_type: "AsyncSubEvent2".to_string(),
            data: format!("new-{i}").into_bytes(),
            tags: vec!["grpc-async-sub".to_string()],
            uuid: None,
            metadata: Vec::new(),
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
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn grpc_async_subscribe_with_after_position() {
    let server = TempServer::start_insecure().await;
    let client = server.client()
        .connect_async()
        .await
        .expect("connect async client");

    // Append A initial events
    let a = 20usize;
    let initial_events: Vec<DcbEvent> = (0..a as u64)
        .map(|i| DcbEvent {
            event_type: "AsyncSubStartEvent".to_string(),
            data: format!("init-{i}").into_bytes(),
            tags: vec!["grpc-async-sub-start".to_string()],
            uuid: None,
            metadata: Vec::new(),
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
    let new_events: Vec<DcbEvent> = (0..m as u64)
        .map(|i| DcbEvent {
            event_type: "AsyncSubStartEvent2".to_string(),
            data: format!("new-{i}").into_bytes(),
            tags: vec!["grpc-async-sub-start".to_string()],
            uuid: None,
            metadata: Vec::new(),
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

    server.shutdown().await;
}

// ----------------------
// Sync subscribe() test
// ----------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn grpc_sync_subscribe_catch_up_and_continue() {
    let server = TempServer::start_insecure().await;
    let http_url = server.url.clone();

    tokio::task::spawn_blocking(move || {
        // Build client with small batch_size so we can test next_batch
        let client = UmaDbClient::new(http_url)
            .connect()
            .expect("connect sync client");


        // Append initial events via sync API
        let initial_count = 20usize;
        let initial_events: Vec<DcbEvent> = (0..initial_count as u64)
            .map(|i| DcbEvent {
                event_type: "SyncSubEvent".to_string(),
                data: format!("init-{i}").into_bytes(),
                tags: vec!["grpc-sync-sub".to_string()],
                uuid: None,
                metadata: Vec::new(),
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
        let new_events: Vec<DcbEvent> = (0..new_count as u64)
            .map(|i| DcbEvent {
                event_type: "SyncSubEvent2".to_string(),
                data: format!("new-{i}").into_bytes(),
                tags: vec!["grpc-sync-sub".to_string()],
                uuid: None,
                metadata: Vec::new(),
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
    }).await.unwrap();

    // Shutdown server
    server.shutdown().await;
}
