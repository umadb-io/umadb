use serial_test::serial;
use umadb_dcb::{DcbEvent, DcbEventStoreAsync};
use uuid::Uuid;
use tests_integration::TempServer;


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn next_batch_returns_all_then_empty_when_unlimited_and_large_batch_size() {
    use futures::StreamExt;

    let server = TempServer::start_insecure().await;

    // Build client with large batch_size (greater than total events we'll append)
    let client = server.client()
        .batch_size(10)
        .connect_async()
        .await
        .expect("connect async client");

    // Append 3 events
    let mk_event = |i: u8| {
        DcbEvent::default()
            .event_type("ExampleEvent")
            .tags(["tagA", "tagB"]) // any tags
            .data(vec![i])
            .uuid(Uuid::new_v4())
    };
    let _p1 = client.append(vec![mk_event(1)], None, None).await.unwrap();
    let _p2 = client.append(vec![mk_event(2)], None, None).await.unwrap();
    let p3 = client.append(vec![mk_event(3)], None, None).await.unwrap();

    // Read with no limit
    let mut resp = client
        .read(None, None, false, None)
        .await
        .expect("read all");

    // First next_batch should return all 3 events (since server batch_size >= total)
    let b1 = resp.next_batch().await.unwrap();
    assert_eq!(b1.len(), 3, "first batch should contain all events");

    // Second next_batch should be empty
    let b2 = resp.next_batch().await.unwrap();
    assert!(
        b2.is_empty(),
        "second batch should be empty once stream is drained"
    );

    // Head should equal last position
    let head = resp.head().await.unwrap();
    assert_eq!(head, Some(p3));

    // Also verify that iterating the stream yields nothing now (already drained)
    assert!(resp.next().await.is_none());

    // Cleanup
    server.shutdown().await;
}
