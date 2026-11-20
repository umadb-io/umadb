use serial_test::serial;
use tests_integration::TempServer;
use umadb_dcb::{DcbError, DcbEvent, DcbEventStoreAsync};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn secure_grpc_end_to_end_append_and_read() {
    let server = TempServer::start_secure().await;
    let client = server.client().tls_domain("localhost").connect_async().await.expect("Failed to connect");

    // Append a handful of events
    let events: Vec<DcbEvent> = (0..25)
        .map(|i| DcbEvent {
            event_type: "SecureTest".to_string(),
            data: format!("tls-data-{}", i).into_bytes(),
            tags: vec!["secure".to_string()],
            uuid: None,
            metadata: Vec::new(),
        })
        .collect();
    let last_pos = client.append(events, None, None).await.expect("append");
    assert!(last_pos >= 25);

    // Read them back (no query means everything)
    let mut resp = client
        .read(None, None, false, None)
        .await
        .expect("read start");

    let mut total = 0usize;
    loop {
        let batch = resp.next_batch().await.expect("next_batch");
        if batch.is_empty() {
            break;
        }
        total += batch.len();
        // Ensure events have expected type/tag
        for ev in batch {
            assert_eq!("SecureTest", ev.event.event_type);
            assert!(ev.event.tags.contains(&"secure".to_string()));
        }
    }

    assert!(total >= 25, "should read back at least appended events");

    // Cleanup
    server.shutdown().await;
}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_tls_domain() {
    let server = TempServer::start_secure().await;

    // This should all work.
    let client = server.client().tls_domain("localhost").connect_async().await.expect("Failed to connect");
    client.head().await.expect("Failed to get head");

    // This connection should fail.
    let err = server.client().tls_domain("wrong").connect_async().await.err().expect("Should have failed to connect");

    match err {
        DcbError::TransportError(msg) => assert_eq!(msg, "transport error: invalid peer certificate: certificate not valid for name \"wrong\"; certificate is only valid for DnsName(\"localhost\")"),
        other => panic!("unexpected error type: {:?}", other),
    }

    // This connection should fail (127.0.0.1 isn't included in the certificate).
    let err = server.client().connect_async().await.err().expect("Should have failed to connect");

    match err {
        DcbError::TransportError(msg) => assert_eq!(msg, "transport error: invalid peer certificate: certificate not valid for name \"127.0.0.1\"; certificate is only valid for DnsName(\"localhost\")"),
        other => panic!("unexpected error type: {:?}", other),
    }
}
