use umadb_client::UmaDBClient;
use umadb_dcb::{DCBError, DCBEvent, DCBEventStoreAsync, TrackingInfo};
use umadb_server::start_server;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_append_with_tracking_enforces_monotonicity() {
    // Arrange: start a gRPC server backed by a temporary directory
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let addr = "127.0.0.1:50111"; // test-specific port
    let addr_http = format!("http://{}", addr);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        let _ = start_server(db_path, &addr, shutdown_rx).await;
    });

    // Give server a brief moment to start
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Connect async client
    let client = UmaDBClient::new(addr_http.clone())
        .connect_async()
        .await
        .expect("client connect");

    // Build a simple event
    let ev = DCBEvent {
        event_type: "TrackEvt".to_string(),
        data: b"d".to_vec(),
        tags: vec!["t".to_string()],
        uuid: None,
    };

    assert_eq!(None, client.get_tracking_info("srcA").await.unwrap());
    assert_eq!(None, client.get_tracking_info("srcB").await.unwrap());

    // 1) First append with tracking at position 5 should succeed
    let last1 = client
        .append(
            vec![ev.clone()],
            None,
            Some(TrackingInfo {
                source: "srcA".into(),
                position: 5,
            }),
        )
        .await
        .expect("append with tracking should succeed");
    assert_eq!(1, last1);

    assert_eq!(Some(5u64), client.get_tracking_info("srcA").await.unwrap());
    assert_eq!(None, client.get_tracking_info("srcB").await.unwrap());

    // 2) Same position 5 should fail with IntegrityError
    let err = client
        .append(
            vec![ev.clone()],
            None,
            Some(TrackingInfo {
                source: "srcA".into(),
                position: 5,
            }),
        )
        .await
        .err()
        .expect("expected integrity error on non-increasing tracking");
    match err {
        DCBError::IntegrityError(msg) => {
            assert!(msg.contains("non-increasing tracking position"))
        }
        other => panic!("unexpected error: {:?}", other),
    }

    assert_eq!(Some(5u64), client.get_tracking_info("srcA").await.unwrap());
    assert_eq!(None, client.get_tracking_info("srcB").await.unwrap());

    // 3) Higher position 6 should succeed
    let last2 = client
        .append(
            vec![ev.clone()],
            None,
            Some(TrackingInfo {
                source: "srcA".into(),
                position: 6,
            }),
        )
        .await
        .expect("append with higher tracking should succeed");
    assert_eq!(2, last2);

    assert_eq!(Some(6u64), client.get_tracking_info("srcA").await.unwrap());
    assert_eq!(None, client.get_tracking_info("srcB").await.unwrap());

    // Cleanup
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}
