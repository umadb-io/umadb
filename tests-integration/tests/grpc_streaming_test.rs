use umadb_client::UmaDBClient;
use umadb_dcb::{DCBEvent, DCBEventStoreAsync};
use umadb_server::start_server;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_async_streams_large_reads_total_count() {
    // Arrange: start a gRPC server backed by a temporary directory
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let addr = "127.0.0.1:50071";
    let addr_http = format!("http://{}", addr);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        let _ = start_server(db_path, &addr, shutdown_rx).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Connect client
    let client = UmaDBClient::new(addr_http.clone())
        .connect_async()
        .await
        .expect("client connect");

    // Append 1000 events
    let events: Vec<DCBEvent> = (0..1000)
        .map(|i| DCBEvent {
            event_type: "TestEvent".to_string(),
            data: format!("data-{i}").into_bytes(),
            tags: vec!["grpc-test".to_string()],
            uuid: None,
        })
        .collect();
    let last_pos = client
        .append(events, None, None)
        .await
        .expect("append 1000 events");
    assert!(last_pos >= 1000);

    // Act: stream all events and count them
    let mut resp = client
        .read(None, None, false, None, false)
        .await
        .expect("read_stream");
    let mut total = 0usize;
    loop {
        let batch = resp.next_batch().await.expect("next_batch");
        if batch.is_empty() {
            break;
        }
        total += batch.len();
    }

    assert_eq!(1000, total, "should read exactly 1000 events");

    // Cleanup
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_async_does_not_stream_past_starting_head() {
    // Arrange
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let addr = "127.0.0.1:50072";
    let addr_http = format!("http://{}", addr);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        let _ = start_server(db_path, &addr, shutdown_rx).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = UmaDBClient::new(addr_http.clone())
        .connect_async()
        .await
        .expect("client connect");

    // Append initial 300 events
    let initial_events: Vec<DCBEvent> = (0..300)
        .map(|i| DCBEvent {
            event_type: "TestEvent".to_string(),
            data: format!("data-{i}").into_bytes(),
            tags: vec!["grpc-boundary".to_string()],
            uuid: None,
        })
        .collect();
    let _ = client
        .append(initial_events, None, None)
        .await
        .expect("append initial events");

    // Start streaming read with no limit to capture starting head semantics
    let mut resp = client
        .read(None, None, false, None, false)
        .await
        .expect("read_stream");

    // Append 50 more events AFTER the read has started
    let new_events: Vec<DCBEvent> = (0..50)
        .map(|i| DCBEvent {
            event_type: "TestEvent2".to_string(),
            data: format!("new-{i}").into_bytes(),
            tags: vec!["grpc-boundary".to_string()],
            uuid: None,
        })
        .collect();
    let _ = client
        .append(new_events, None, None)
        .await
        .expect("append new events during read");

    // Consume response; it should end at the starting head (300)
    let mut total = 0usize;
    loop {
        let batch = resp.next_batch().await.expect("next_batch");
        if batch.is_empty() {
            break;
        }
        total += batch.len();
    }

    assert_eq!(300, total, "should read exactly initial 300 events");

    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_async_subscription_catch_up_and_continue() {
    // Arrange
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let addr = "127.0.0.1:50073";
    let addr_http = format!("http://{}", addr);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        let _ = start_server(db_path, &addr, shutdown_rx).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = UmaDBClient::new(addr_http.clone())
        .connect_async()
        .await
        .expect("client connect");

    // Append initial events
    let initial_count = 40usize;
    let initial_events: Vec<DCBEvent> = (0..initial_count as u64)
        .map(|i| DCBEvent {
            event_type: "SubTestEvent".to_string(),
            data: format!("init-{i}").into_bytes(),
            tags: vec!["grpc-sub".to_string()],
            uuid: None,
        })
        .collect();
    let _ = client
        .append(initial_events, None, None)
        .await
        .expect("append initial events");

    // Start a subscription stream that should catch up existing events and then continue
    let mut resp = client
        .read(None, None, false, None, true)
        .await
        .expect("subscription stream");

    // Collect exactly initial_count events
    let mut collected_initial = 0usize;
    while collected_initial < initial_count {
        let batch = resp
            .next_batch()
            .await
            .expect("next_batch while catching up");
        if batch.is_empty() {
            panic!("subscription ended unexpectedly while catching up");
        }
        for ev in batch.into_iter() {
            assert!(ev.event.tags.iter().any(|t| t == "grpc-sub"));
            collected_initial += 1;
            if collected_initial == initial_count {
                break;
            }
        }
    }

    // Append more events and ensure they also arrive
    let new_count = 25usize;
    let new_events: Vec<DCBEvent> = (0..new_count as u64)
        .map(|i| DCBEvent {
            event_type: "SubTestEvent2".to_string(),
            data: format!("new-{i}").into_bytes(),
            tags: vec!["grpc-sub".to_string()],
            uuid: None,
        })
        .collect();
    let _ = client
        .append(new_events, None, None)
        .await
        .expect("append new events during subscription");

    let mut collected_new = 0usize;
    while collected_new < new_count {
        let batch = resp.next_batch().await.expect("next_batch for new events");
        if batch.is_empty() {
            panic!("subscription ended unexpectedly before receiving new events");
        }
        for ev in batch.into_iter() {
            assert!(ev.event.tags.iter().any(|t| t == "grpc-sub"));
            collected_new += 1;
            if collected_new == new_count {
                break;
            }
        }
    }

    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_async_stream_catch_up_and_continue() {
    // This is the existing async test retained from the previous setup to validate catch-up streaming
    use tokio::time::{Duration as TokioDuration, sleep};

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let addr = "127.0.0.1:50075"; // async API test port
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

    // Append initial events via async API
    let initial_count = 15usize;
    let initial_events: Vec<DCBEvent> = (0..initial_count as u64)
        .map(|i| DCBEvent {
            event_type: "AsyncEvent".to_string(),
            data: format!("init-{i}").into_bytes(),
            tags: vec!["grpc-async".to_string()],
            uuid: None,
        })
        .collect();
    let _ = client
        .append(initial_events, None, None)
        .await
        .expect("append initial events");

    let mut resp = client
        .read(None, None, false, None, true)
        .await
        .expect("read_stream");

    let mut received = 0usize;
    while received < initial_count {
        let batch = resp.next_batch().await.expect("next_batch catching up");
        if batch.is_empty() {
            panic!("unexpected response end while catching up");
        }
        for ev in batch.into_iter() {
            assert!(ev.event.tags.iter().any(|t| t == "grpc-async"));
            received += 1;
            if received == initial_count {
                break;
            }
        }
    }

    // Append more events via async API
    let new_count = 7usize;
    let new_events: Vec<DCBEvent> = (0..new_count as u64)
        .map(|i| DCBEvent {
            event_type: "AsyncEvent2".to_string(),
            data: format!("new-{i}").into_bytes(),
            tags: vec!["grpc-async".to_string()],
            uuid: None,
        })
        .collect();
    let _ = client
        .append(new_events, None, None)
        .await
        .expect("append new events");

    let mut received_new = 0usize;
    while received_new < new_count {
        let batch = resp.next_batch().await.expect("next_batch new events");
        if batch.is_empty() {
            panic!("unexpected response end before receiving new events");
        }
        for ev in batch.into_iter() {
            assert!(ev.event.tags.iter().any(|t| t == "grpc-async"));
            received_new += 1;
            if received_new == new_count {
                break;
            }
        }
    }

    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}
