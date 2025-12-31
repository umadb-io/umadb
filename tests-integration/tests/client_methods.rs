use std::net::{Ipv4Addr, TcpListener};
use std::time::Duration;

use tempfile::tempdir;
use tokio::runtime::Builder as RtBuilder;
use tokio::time::sleep;

use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus as PbServingStatus;
use tonic_health::pb::health_client::HealthClient;

use umadb_client::UmaDBClient;
use umadb_dcb::{
    DCBAppendCondition, DCBEvent, DCBEventStoreAsync, DCBEventStoreSync, DCBQuery, DCBQueryItem,
};
use umadb_server::start_server;
use uuid::Uuid;

// Helper to pick a free localhost port
fn get_free_port() -> u16 {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind :0");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

// Waits for the gRPC health service to report SERVING for the overall service and UmaDB service
async fn wait_for_health(url: &str) {
    // Retry connect to channel
    let channel = loop {
        match tonic::transport::Channel::from_shared(url.to_string())
            .unwrap()
            .connect()
            .await
        {
            Ok(ch) => break ch,
            Err(_) => {
                sleep(Duration::from_millis(50)).await;
            }
        }
    };

    let mut client = HealthClient::new(channel);

    // overall
    for _ in 0..60u32 {
        if let Ok(resp) = client
            .check(HealthCheckRequest {
                service: "".to_string(),
            })
            .await
        {
            if resp.into_inner().status == PbServingStatus::Serving as i32 {
                break;
            }
        }
        sleep(Duration::from_millis(50)).await;
    }

    // UmaDB named service
    for _ in 0..60u32 {
        if let Ok(resp) = client
            .check(HealthCheckRequest {
                service: "umadb.v1.DCB".to_string(),
            })
            .await
        {
            if resp.into_inner().status == PbServingStatus::Serving as i32 {
                return;
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("server did not become SERVING in time");
}

#[test]
fn client_sync_covers_all_methods() {
    // Arrange: start server on a free port
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let port = get_free_port();
    let addr = format!("127.0.0.1:{}", port);
    let url = format!("http://{}", addr);

    let rt = RtBuilder::new_multi_thread().enable_all().build().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Start server in background
    let db_clone = db_path.clone();
    let addr_clone = addr.clone();
    rt.spawn(async move {
        let _ = start_server(db_clone, &addr_clone, shutdown_rx).await;
    });

    // Wait until health is serving on same port
    rt.block_on(wait_for_health(&url));

    // Build client using UmaDBClient builder (sync) and force small batch_size so we can test next_batch
    let client = UmaDBClient::new(url.clone())
        .batch_size(2)
        .connect()
        .expect("connect sync client");

    // Head on empty store should be None
    let empty_head = client.head().expect("head on empty");
    assert!(empty_head.is_none());

    // Prepare some events
    let mk_event = |i: u8| {
        DCBEvent::default()
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
    let mut resp = client
        .read(None, None, false, None, false)
        .expect("read all");
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
        .read(None, None, false, None, false)
        .expect("read for collect");
    let (events, head2) = resp2.collect_with_head().unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(head2, Some(p3));

    // Exercise: next_batch with a limit
    let mut resp3 = client
        .read(None, None, false, None, false)
        .expect("read batched");
    let b1 = resp3.next_batch().unwrap();
    assert_eq!(b1.len(), 2);
    let b2 = resp3.next_batch().unwrap();
    assert_eq!(b2.len(), 1);

    // Exercise: read_with_head helper with a boundary and after parameter
    let boundary = DCBQuery::new().item(DCBQueryItem::new().types(["ExampleEvent"]).tags(["tagA"]));
    let (vec_events, head3) = client
        .read_with_head(Some(boundary.clone()), None, false, None)
        .unwrap();
    assert_eq!(vec_events.len(), 3);
    assert_eq!(head3, Some(p3));

    // Now try conditional append using last-known head
    let cond = DCBAppendCondition::new(boundary).after(head3);
    let e4 = mk_event(4);
    let p4 = client.append(vec![e4.clone()], Some(cond), None).unwrap();
    assert!(p4 > p3);

    // Shutdown server
    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_async_covers_all_methods() {
    use futures::StreamExt; // for next() on Stream

    // Arrange: start server on a free port
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let port = get_free_port();
    let addr = format!("127.0.0.1:{}", port);
    let url = format!("http://{}", addr);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Start server task
    let db_clone = db_path.clone();
    let addr_clone = addr.clone();
    let server_task = tokio::spawn(async move {
        let _ = start_server(db_clone, &addr_clone, shutdown_rx).await;
    });

    // Wait for health
    wait_for_health(&url).await;

    // Build async client via UmaDBClient builder
    let client = UmaDBClient::new(url.clone())
        .batch_size(2)
        .connect_async()
        .await
        .expect("connect async client");

    // Head on empty store should be None
    let empty_head = client.head().await.expect("head on empty");
    assert!(empty_head.is_none());

    // Prepare events
    let mk_event = |i: u8| {
        DCBEvent::default()
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
        .read(None, None, false, None, false)
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
        .read(None, None, false, None, false)
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
    let boundary = DCBQuery::new().item(DCBQueryItem::new().types(["ExampleEvent"]).tags(["tagA"]));
    let (evs_in_boundary, head3) = client
        .read_with_head(Some(boundary.clone()), None, false, None)
        .await
        .unwrap();
    assert_eq!(evs_in_boundary.len(), 3);
    assert_eq!(head3, Some(p3));

    let cond = DCBAppendCondition::new(boundary).after(head3);
    let p4 = client
        .append(vec![mk_event(4)], Some(cond), None)
        .await
        .unwrap();
    assert!(p4 > p3);

    // Cleanup
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}
