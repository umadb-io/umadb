use futures::StreamExt;
use serial_test::serial;
use tests_integration::TempServer;
use umadb_dcb::{DcbError, DcbEvent, DcbEventStoreAsync, DcbQuery, DcbQueryItem, TrackingInfo};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn grpc_append_with_tracking_enforces_monotonicity() {
    let server = TempServer::start_insecure().await;
    let client = server.client().connect_async().await.expect("Failed to connect");

    // Build a simple event
    let ev = DcbEvent {
        event_type: "TrackEvt".to_string(),
        data: b"d".to_vec(),
        tags: vec!["t".to_string()],
        uuid: None,
        metadata: Vec::new(),
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
        DcbError::IntegrityError(msg) => {
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

    // Check the sequenced event has the tracking info.
    let query = DcbQuery {
        items: vec![DcbQueryItem {
            types: vec![],
            tags: vec!["x".to_string()],
        }],
    };
    let mut response = client
        .read(Some(query), None, false, None)
        .await
        .expect("appended events");
    let mut count = 0;
    let mut expected_position = 5;
    while let Some(result) = response.next().await {
        let event = result.expect("sequenced event");
        count = count + 1;
        assert_eq!(
            Some(TrackingInfo {
                source: "src1".into(),
                position: expected_position
            }),
            event.tracking_info
        );
        if expected_position == 5 {
            expected_position = 6;
        }
    }

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn grpc_append_with_tracking_only_no_events() {
    let server = TempServer::start_insecure().await;
    let client = server.client().connect_async().await.expect("Failed to connect");

    assert_eq!(None, client.get_tracking_info("srcC").await.unwrap());
    assert_eq!(None, client.get_tracking_info("srcD").await.unwrap());

    // 1) First append with tracking at position 5 should succeed
    let last1 = client
        .append(
            vec![],
            None,
            Some(TrackingInfo {
                source: "srcC".into(),
                position: 5,
            }),
        )
        .await
        .expect("append with tracking should succeed");
    assert_eq!(0, last1);

    assert_eq!(Some(5u64), client.get_tracking_info("srcC").await.unwrap());
    assert_eq!(None, client.get_tracking_info("srcD").await.unwrap());

    // 2) Same position 5 should fail with IntegrityError
    let err = client
        .append(
            vec![],
            None,
            Some(TrackingInfo {
                source: "srcC".into(),
                position: 5,
            }),
        )
        .await
        .err()
        .expect("expected integrity error on non-increasing tracking");
    match err {
        DcbError::IntegrityError(msg) => {
            assert!(msg.contains("non-increasing tracking position"))
        }
        other => panic!("unexpected error: {:?}", other),
    }

    assert_eq!(Some(5u64), client.get_tracking_info("srcC").await.unwrap());
    assert_eq!(None, client.get_tracking_info("srcD").await.unwrap());

    // 3) Higher position 6 should succeed
    let last2 = client
        .append(
            vec![],
            None,
            Some(TrackingInfo {
                source: "srcC".into(),
                position: 6,
            }),
        )
        .await
        .expect("append with higher tracking should succeed");
    assert_eq!(0, last2);

    assert_eq!(Some(6u64), client.get_tracking_info("srcC").await.unwrap());
    assert_eq!(None, client.get_tracking_info("srcD").await.unwrap());

    // Check the sequenced event has the tracking info.
    let query = DcbQuery {
        items: vec![DcbQueryItem {
            types: vec![],
            tags: vec!["x".to_string()],
        }],
    };
    let mut response = client
        .read(Some(query), None, false, None)
        .await
        .expect("read events");
    let mut count = 0;
    let mut expected_position = 5;
    while let Some(result) = response.next().await {
        let event = result.expect("sequenced event");
        count = count + 1;
        assert_eq!(
            Some(TrackingInfo {
                source: "src1".into(),
                position: expected_position
            }),
            event.tracking_info
        );
        if expected_position == 5 {
            expected_position = 6;
        }
    }

    // Cleanup
    server.shutdown().await;
}
