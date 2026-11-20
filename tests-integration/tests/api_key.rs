use serial_test::serial;
use tests_integration::TempServer;
use umadb_dcb::{DcbError, DcbEvent, DcbEventStoreAsync};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_key_success_over_tls() {
    let server = TempServer::start_secure().await;
    let client = server.client().tls_domain("localhost").connect_async().await.expect("Failed to connect");

    // Append and read should succeed
    let events: Vec<DcbEvent> = (0..3)
        .map(|i| DcbEvent {
            event_type: "AuthTest".to_string(),
            data: format!("data-{}", i).into_bytes(),
            tags: vec!["t".to_string()],
            uuid: None,
            metadata: Vec::new(),
        })
        .collect();
    let last_pos = client.append(events, None, None).await.expect("append");
    assert!(last_pos >= 3);

    let head = client.head().await.expect("head");
    assert!(head.is_some());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn api_key_wrong_fails_over_tls() {
    let server = TempServer::start_secure().await;

    // This should work.
    let client = server.client().tls_domain("localhost").connect_async().await.expect("Failed to connect");
    client.head().await.expect("Failed to get head");

    // This should fail.
    let client = server.client().tls_domain("localhost").api_key("wrong").connect_async().await.expect("Failed to connect");

    // Calls should fail with Unauthenticated mapped to TransportError/Io (depending on mapping)
    let err = client.head().await.err().expect("head should fail");
    match err {
        DcbError::AuthenticationError(_) => {}
        other => panic!("unexpected error type: {:?}", other),
    }

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_key_rejected_over_insecure_http() {
    let server = TempServer::start_insecure().await;

    // This call to hear() should work.
    let client = server.client().connect_async().await.expect("Failed to connect");
    client.head().await.expect("Failed to get head");

    let client = server.client().api_key("something").connect_async().await.expect("Failed to connect");
    // This call to head() should fail.
    let err = client.head().await.err().expect("Expected error");

    match err {
        DcbError::TransportError(msg) => assert_eq!(msg, "API key configured but TLS is not enabled; refusing to send credentials over insecure channel"),
        other => panic!("unexpected error type: {:?}", other),
    }
}
