use std::net::{Ipv4Addr, TcpListener};
use std::time::Duration;

use tempfile::tempdir;
use tokio::time::sleep;

use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus as PbServingStatus;
use tonic_health::pb::health_client::HealthClient;

use umadb_client::UmaDBClient;
use umadb_dcb::{DCBEvent, DCBEventStoreAsync};
use umadb_server::start_server;
use uuid::Uuid;

// Helper to pick a free localhost port
fn get_free_port() -> u16 {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind :0");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn next_batch_returns_all_then_empty_when_unlimited_and_large_batch_size() {
    use futures::StreamExt;

    // Start server
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let port = get_free_port();
    let addr = format!("127.0.0.1:{}", port);
    let url = format!("http://{}", addr);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server_task = tokio::spawn({
        let db_clone = db_path.clone();
        let addr_clone = addr.clone();
        async move {
            let _ = start_server(db_clone, &addr_clone, shutdown_rx).await;
        }
    });

    wait_for_health(&url).await;

    // Build client with large batch_size (greater than total events we'll append)
    let client = UmaDBClient::new(url.clone())
        .batch_size(10)
        .connect_async()
        .await
        .expect("connect async client");

    // Append 3 events
    let mk_event = |i: u8| {
        DCBEvent::default()
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
        .read(None, None, false, None, false)
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
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}
