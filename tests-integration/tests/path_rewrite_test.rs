use prost::Message;
use std::net::TcpListener;
use tempfile::tempdir;
use tokio::runtime::Builder as RtBuilder;
use umadb_client::UmaDBClient;
use umadb_dcb::DCBEvent;
use umadb_proto::v1::HeadRequest;
use umadb_server::start_server;

/// Test that the path rewriting from "/umadb.UmaDBService/" to "/umadb.v1.DCB/" is working
///
/// The server's PathRewriterService in umadb-server/src/lib.rs (lines 38-86) implements
/// a Tower middleware layer that intercepts incoming HTTP/2 gRPC requests and rewrites
/// paths that start with "/umadb.UmaDBService/" to "/umadb.v1.DCB/".
///
/// This path rewriting provides backward compatibility for legacy clients that may still
/// use the old service name "UmaDBService" instead of the new "DCB" service name.
///
/// The rewriting logic (lines 56-57) specifically checks:
///   if path.starts_with("/umadb.UmaDBService/") {
///       let new_path_str = path.replace("/umadb.UmaDBService/", "/umadb.v1.DCB/");
///
/// This test verifies the path rewriting functionality by:
/// 1. Starting a server with the PathRewriterLayer enabled (configured in start_server_internal at line 221)
/// 2. Using the normal UmaDBClient to append some events and get the head position
/// 3. Sending a raw HTTP/2 gRPC request with the OLD path "/umadb.UmaDBService/Head"
/// 4. Verifying that the request succeeds and returns the same head position as the client,
///    confirming the server rewrote the path to "/umadb.v1.DCB/Head" correctly
#[test]
fn test_path_rewrite_umadbservice_to_dcb() {
    // Pick a free port
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let addr_noscheme = format!("{}", addr);
    let addr_with_scheme = format!("http://{}", addr);

    // Build a multi-threaded runtime for server and client
    let rt = RtBuilder::new_multi_thread().enable_all().build().unwrap();

    // Prepare shutdown channel for the server
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    // Temporary directory for the database server
    let temp_dir = tempdir().unwrap();

    // Start the server which has the PathRewriterLayer enabled
    // The layer is applied in umadb-server/src/lib.rs:221 within start_server_internal():
    //   .layer(PathRewriterLayer)
    let data_dir = temp_dir.path().to_path_buf();
    rt.spawn(async move {
        let _ = start_server(data_dir, &addr_noscheme, shutdown_rx).await;
    });

    // Connect the normal UmaDBClient (retry until server is ready)
    let client = {
        use std::{thread, time::Duration};
        let mut attempts = 0;
        loop {
            match UmaDBClient::new(addr_with_scheme.clone()).connect() {
                Ok(c) => break c,
                Err(e) => {
                    attempts += 1;
                    if attempts >= 50 {
                        panic!("Failed to connect to gRPC server after retries: {:?}", e);
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }
    };

    // Append some events using the normal client
    use umadb_dcb::DCBEventStoreSync;
    let events = vec![
        DCBEvent {
            event_type: "TestEvent".to_string(),
            data: b"test data 1".to_vec(),
            tags: vec!["tag1".to_string()],
            uuid: None,
        },
        DCBEvent {
            event_type: "TestEvent".to_string(),
            data: b"test data 2".to_vec(),
            tags: vec!["tag2".to_string()],
            uuid: None,
        },
        DCBEvent {
            event_type: "TestEvent".to_string(),
            data: b"test data 3".to_vec(),
            tags: vec!["tag3".to_string()],
            uuid: None,
        },
    ];
    let append_position = client
        .append(events, None, None)
        .expect("Failed to append events");

    // Get the head position using the normal client
    let client_head_position = client
        .head()
        .expect("Failed to get head")
        .expect("Head should not be None");

    // Verify append returned the correct position (should be 3 after appending 3 events)
    assert_eq!(append_position, 3, "Append should return position 3");
    assert_eq!(client_head_position, 3, "Head should be at position 3");

    // Test with the OLD path format "/umadb.UmaDBService/Head" using raw HTTP/2 request
    // This will verify path rewriting works and returns the same head position
    let old_path_head_position = rt.block_on(async {
        use http_body_util::Full;
        use hyper::body::Bytes;
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use tokio::net::TcpStream;

        // Parse the address
        let addr_parsed = addr_with_scheme.strip_prefix("http://").unwrap();

        // Wait for server to be ready by retrying connection
        let stream = {
            let mut attempts = 0;
            loop {
                match TcpStream::connect(addr_parsed).await {
                    Ok(s) => break s,
                    Err(e) => {
                        attempts += 1;
                        if attempts >= 50 {
                            panic!(
                                "Failed to connect to server after {} attempts: {:?}",
                                attempts, e
                            );
                        }
                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    }
                }
            }
        };

        // Give the server a moment to finish initialization after port binding
        // The server binds the port early but may not be ready to serve requests immediately
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let io = TokioIo::new(stream);

        // Create an HTTP/2 connection with TokioExecutor
        let (mut sender, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
            .await
            .expect("Failed HTTP/2 handshake");

        // Drive the connection in the background
        let _conn_task = tokio::spawn(async move {
            if let Err(err) = conn.await {
                eprintln!("Connection error: {:?}", err);
            }
        });

        // Encode the HeadRequest protobuf message
        let head_request = HeadRequest {};
        let mut proto_buf = Vec::new();
        head_request
            .encode(&mut proto_buf)
            .expect("Failed to encode HeadRequest");

        // Add gRPC framing: 5-byte prefix (1 byte compression flag + 4 bytes message length)
        let mut grpc_body = Vec::new();
        grpc_body.push(0u8); // compression flag (0 = not compressed)
        grpc_body.extend_from_slice(&(proto_buf.len() as u32).to_be_bytes()); // message length (big-endian)
        grpc_body.extend_from_slice(&proto_buf); // the actual protobuf message

        // Build the HTTP/2 request with the OLD path "/umadb.UmaDBService/Head"
        let req = hyper::Request::builder()
            .method("POST")
            .uri("/umadb.UmaDBService/Head") // OLD PATH - should be rewritten by server
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(Full::new(Bytes::from(grpc_body)))
            .expect("Failed to build request");

        // Send the request
        let response = sender.send_request(req).await;

        // Check if the request succeeded and decode the response
        match response {
            Ok(mut resp) => {
                use http_body_util::BodyExt;

                let status = resp.status();
                if !status.is_success() {
                    return None;
                }

                // Collect the response body by reading frames
                let mut body_bytes = Vec::new();
                while let Some(frame_result) = resp.frame().await {
                    match frame_result {
                        Ok(frame) => {
                            if let Some(data) = frame.data_ref() {
                                body_bytes.extend_from_slice(data);
                            }
                        }
                        Err(e) => {
                            eprintln!("Error reading response frame: {:?}", e);
                            return None;
                        }
                    }
                }

                // gRPC response has 5-byte framing: 1 byte compression flag + 4 bytes message length
                if body_bytes.len() < 5 {
                    eprintln!("Response body too short for gRPC frame");
                    return None;
                }

                let message_length = u32::from_be_bytes([
                    body_bytes[1],
                    body_bytes[2],
                    body_bytes[3],
                    body_bytes[4],
                ]) as usize;

                if body_bytes.len() < 5 + message_length {
                    eprintln!("Response body too short for declared message length");
                    return None;
                }

                // Extract and decode the protobuf message
                let proto_bytes = &body_bytes[5..5 + message_length];

                use umadb_proto::v1::HeadResponse;
                match HeadResponse::decode(proto_bytes) {
                    Ok(head_response) => {
                        // Return the position from the response
                        head_response.position
                    }
                    Err(e) => {
                        eprintln!("Failed to decode HeadResponse: {:?}", e);
                        None
                    }
                }
            }
            Err(e) => {
                eprintln!("Request failed: {:?}", e);
                None
            }
        }
    });

    // Verify the position returned via old path matches the client's head position
    println!(
        "Got {:?} as head position from old path",
        old_path_head_position
    );
    assert_eq!(
        old_path_head_position,
        Some(client_head_position),
        "Head position from old path '/umadb.UmaDBService/Head' should match client head position"
    );
    // Summary: This test verifies that:
    // 1. The server is running with PathRewriterLayer enabled (applied at server level in lib.rs:255)
    // 2. Normal UmaDBClient successfully appends 3 events and gets head position (3)
    // 3. A raw HTTP/2 request with the OLD path "/umadb.UmaDBService/Head" is sent
    // 4. The server's PathRewriterService rewrites it to "/umadb.v1.DCB/Head"
    // 5. The request succeeds with HTTP 200 OK
    // 6. The response body is decoded to verify it contains a valid HeadResponse message
    // 7. The head position from the old path matches the position from the normal client (both = 3)

    // Shutdown server
    let _ = shutdown_tx.send(());
}
