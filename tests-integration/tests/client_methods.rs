use serial_test::serial;
use tonic::Request;

use tests_integration::TempServer;
use umadb_client::UmaDbClient;
use umadb_dcb::{
    DcbAppendCondition, DcbEvent, DcbEventStoreAsync, DcbEventStoreSync, DcbQuery, DcbQueryItem,
};
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn client_sync_covers_all_methods() {
    // 1. Start the server asynchronously
    let server = TempServer::start_insecure().await;

    // We need to clone the url to move it into the blocking thread
    let http_url = server.url.clone();

    // 2. Offload ALL synchronous client logic to a dedicated blocking thread
    tokio::task::spawn_blocking(move || {
        // Build client with small batch_size so we can test next_batch
        let client = UmaDbClient::new(http_url)
            .batch_size(2)
            .connect()
            .expect("connect sync client");

        // Head on empty store should be None
        let empty_head = client.head().expect("head on empty");
        assert!(empty_head.is_none());

        // Prepare some events
        let mk_event = |i: u8| {
            DcbEvent::default()
                .event_type("ExampleEvent")
                .tags(["tagA", "tagB"])
                .data(vec![i])
                .uuid(Uuid::new_v4())
        };
        let e1 = mk_event(1);
        let e2 = mk_event(2);
        let e3 = mk_event(3);

        // Append events unconditionally
        let p1 = client.append(vec![e1.clone()], None, None).unwrap();
        let p2 = client.append(vec![e2.clone()], None, None).unwrap();
        let p3 = client.append(vec![e3.clone()], None, None).unwrap();
        assert!(p1 < p2 && p2 < p3);

        // Exercise: iterate over all events, then retrieve head()
        let mut resp = client.read(None, None, false, None).expect("read all");
        let mut positions = Vec::new();
        while let Some(item) = resp.next() {
            let ev = item.expect("stream item");
            positions.push(ev.position);
        }
        assert_eq!(positions, vec![p1, p2, p3]);
        let head_after_iter = resp.head().unwrap();
        assert_eq!(head_after_iter, Some(p3));

        // Also check client.head()
        let client_head = client.head().unwrap();
        assert_eq!(client_head, Some(p3));

        // Exercise: collect_with_head via response
        let mut resp2 = client
            .read(None, None, false, None)
            .expect("read for collect");
        let (events, head2) = resp2.collect_with_head().unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(head2, Some(p3));

        // Exercise: next_batch with a limit
        let mut resp3 = client.read(None, None, false, None).expect("read batched");
        let b1 = resp3.next_batch().unwrap();
        assert_eq!(b1.len(), 2);
        let b2 = resp3.next_batch().unwrap();
        assert_eq!(b2.len(), 1);

        // Exercise: read_with_head helper with a boundary and after parameter
        let boundary = DcbQuery::new().item(DcbQueryItem::new().types(["ExampleEvent"]).tags(["tagA"]));
        let (vec_events, head3) = client
            .read_with_head(Some(boundary.clone()), None, false, None)
            .unwrap();
        assert_eq!(vec_events.len(), 3);
        assert_eq!(head3, Some(p3));

        // Now try conditional append using last-known head
        let cond = DcbAppendCondition::new(boundary).after(head3);
        let e4 = mk_event(4);
        let p4 = client.append(vec![e4.clone()], Some(cond), None).unwrap();
        assert!(p4 > p3);
    })
        .await // Wait for the blocking thread to finish
        .unwrap(); // Bubble up any panics from assertions inside the blocking thread

    // 3. Shutdown server gracefully
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn read_with_subscribe_true_returns_invalid_argument_error() {
    let server = TempServer::start_insecure().await;

    // Build basic gRPC client directly from generated proto client.
    let mut client = umadb_proto::v1::dcb_client::DcbClient::connect(server.url.clone())
        .await
        .expect("connect grpc client");

    // Check we get an invalid argument error from effectively calling `read(subscribe=True)`.
    #[allow(deprecated)]
    let req_body = umadb_proto::v1::ReadRequest {
        query: None,
        start: None,
        backwards: None,
        limit: None,
        subscribe: Some(true),
        batch_size: None,
    };
    let err = client
        .read(Request::new(req_body))
        .await
        .expect_err("read(subscribe=true) should fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("subscribe"));

    // Shutdown server
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn client_async_covers_all_methods() {
    use futures::StreamExt; // for next() on Stream

    let server = TempServer::start_insecure().await;

    // Build async client via UmaDBClient builder
    let client = server.client()
        .batch_size(2)
        .connect_async()
        .await
        .expect("connect async client");

    // Head on empty store should be None
    let empty_head = client.head().await.expect("head on empty");
    assert!(empty_head.is_none());

    // Prepare events
    let mk_event = |i: u8| {
        DcbEvent::default()
            .event_type("ExampleEvent")
            .tags(["tagA", "tagB"])
            .data(vec![i])
            .uuid(Uuid::new_v4())
    };
    let e1 = mk_event(1);
    let e2 = mk_event(2);
    let e3 = mk_event(3);

    // Append a few events
    let p1 = client.append(vec![e1.clone()], None, None).await.unwrap();
    let p2 = client.append(vec![e2.clone()], None, None).await.unwrap();
    let p3 = client.append(vec![e3.clone()], None, None).await.unwrap();
    assert!(p1 < p2 && p2 < p3);

    // Exercise: iterate over stream, then get head()
    let mut resp = client
        .read(None, None, false, None)
        .await
        .expect("read all");
    let mut positions = Vec::new();
    while let Some(item) = resp.next().await {
        let ev = item.expect("stream item");
        positions.push(ev.position);
    }
    assert_eq!(positions, vec![p1, p2, p3]);
    let head_after_iter = resp.head().await.unwrap();
    assert_eq!(head_after_iter, Some(p3));

    // Exercise: next_batch with limit
    let mut resp2 = client
        .read(None, None, false, None)
        .await
        .expect("read batched");
    let b1 = resp2.next_batch().await.unwrap();
    assert_eq!(b1.len(), 2);
    let b2 = resp2.next_batch().await.unwrap();
    assert_eq!(b2.len(), 1);

    // Exercise: collect_with_head via helper method read_with_head
    let (events, head2) = client
        .read_with_head(None, None, false, None)
        .await
        .unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(head2, Some(p3));

    // Use a boundary and conditional append based on head
    let boundary = DcbQuery::new().item(DcbQueryItem::new().types(["ExampleEvent"]).tags(["tagA"]));
    let (evs_in_boundary, head3) = client
        .read_with_head(Some(boundary.clone()), None, false, None)
        .await
        .unwrap();
    assert_eq!(evs_in_boundary.len(), 3);
    assert_eq!(head3, Some(p3));

    let cond = DcbAppendCondition::new(boundary).after(head3);
    let p4 = client
        .append(vec![mk_event(4)], Some(cond), None)
        .await
        .unwrap();
    assert!(p4 > p3);

    // Cleanup
    server.shutdown().await;
}
