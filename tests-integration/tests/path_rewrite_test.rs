use prost::Message;
use tests_integration::TempServer;
use umadb_dcb::{DcbEvent, DcbEventStoreAsync};
use umadb_proto::v1::{HeadRequest, HeadResponse};

/// Test that the path rewriting from "/umadb.UmaDBService/" to "/umadb.v1.DCB/" is working
///
/// The server's PathRewriterService implements a Tower middleware layer that intercepts
/// incoming HTTP/2 gRPC requests and rewrites paths that start with "/umadb.UmaDBService/"
/// to "/umadb.v1.DCB/". This provides backward compatibility for legacy clients.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_path_rewrite_umadbservice_to_dcb() {
    // 1. Start the server (TempServer handles tempdir, port binding, and health checks!)
    let server = TempServer::start_insecure().await;

    // 2. Offload the synchronous UmaDbClient setup and assertions to a blocking thread
    let client = server.client().connect_async().await.expect("Failed to connect sync client");

    let events = vec![
        DcbEvent {
            event_type: "TestEvent".to_string(),
            data: b"test data 1".to_vec(),
            tags: vec!["tag1".to_string()],
            uuid: None,
            metadata: Vec::new(),
        },
        DcbEvent {
            event_type: "TestEvent".to_string(),
            data: b"test data 2".to_vec(),
            tags: vec!["tag2".to_string()],
            uuid: None,
            metadata: Vec::new(),
        },
        DcbEvent {
            event_type: "TestEvent".to_string(),
            data: b"test data 3".to_vec(),
            tags: vec!["tag3".to_string()],
            uuid: None,
            metadata: Vec::new(),
        },
    ];

    let append_position = client
        .append(events, None, None)
        .await
        .expect("Failed to append events");
    assert_eq!(append_position, 3, "Append should return position 3");

    let client_head_position = client
        .head()
        .await
        .expect("Failed to get head")
        .expect("Head should not be None");
    assert_eq!(client_head_position, 3, "Head should be at position 3");


    // 3. Test with the OLD path format using a raw HTTP/2 request in the main async thread
    use http_body_util::Full;
    use hyper::body::Bytes;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use tokio::net::TcpStream;

    // Because TempServer ensures the server is ready, we don't need a retry loop!
    let stream = TcpStream::connect(server.bound_addr.clone())
        .await
        .expect("Failed to connect to server");

    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .expect("Failed HTTP/2 handshake");

    tokio::spawn(async move {
        if let Err(err) = conn.await {
            eprintln!("Connection error: {:?}", err);
        }
    });

    // Encode the HeadRequest protobuf message
    let mut proto_buf = Vec::new();
    HeadRequest {}.encode(&mut proto_buf).expect("Failed to encode HeadRequest");

    // Add gRPC framing: 5-byte prefix (1 byte compression flag + 4 bytes message length)
    let mut grpc_body = Vec::new();
    grpc_body.push(0u8);
    grpc_body.extend_from_slice(&(proto_buf.len() as u32).to_be_bytes());
    grpc_body.extend_from_slice(&proto_buf);

    // Build the HTTP/2 request with the OLD path "/umadb.UmaDBService/Head"
    let req = hyper::Request::builder()
        .method("POST")
        .uri("/umadb.UmaDBService/Head") // OLD PATH - should be rewritten by server
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(Full::new(Bytes::from(grpc_body)))
        .expect("Failed to build request");

    // Send the request
    let mut resp = sender.send_request(req).await.expect("Request failed");
    assert!(resp.status().is_success(), "HTTP request should succeed");

    // Collect the response body
    use http_body_util::BodyExt;
    let mut body_bytes = Vec::new();
    while let Some(frame_result) = resp.frame().await {
        let frame = frame_result.expect("Error reading response frame");
        if let Some(data) = frame.data_ref() {
            body_bytes.extend_from_slice(data);
        }
    }

    // Verify gRPC framing
    assert!(body_bytes.len() >= 5, "Response body too short for gRPC frame");
    let message_length = u32::from_be_bytes([
        body_bytes[1], body_bytes[2], body_bytes[3], body_bytes[4]
    ]) as usize;
    assert!(body_bytes.len() >= 5 + message_length, "Response body too short for declared length");

    // Extract and decode the protobuf message
    let proto_bytes = &body_bytes[5..5 + message_length];
    let head_response = HeadResponse::decode(proto_bytes).expect("Failed to decode HeadResponse");

    let old_path_head_position = head_response.position;

    // 4. Verify the position returned via old path matches the client's head position
    println!("Got {:?} as head position from old path", old_path_head_position);
    assert_eq!(
        old_path_head_position,
        Some(client_head_position),
        "Head position from old path should match client head position"
    );

    // 5. Clean shutdown
    server.shutdown().await;
}