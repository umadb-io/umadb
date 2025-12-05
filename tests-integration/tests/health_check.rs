use std::net::{Ipv4Addr, TcpListener};
use std::time::Duration;

use tempfile::tempdir;
use tokio::time::sleep;

// tonic-health client API
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus as PbServingStatus;
use tonic_health::pb::health_client::HealthClient;
use umadb_server::start_server;

// Helper to pick a free localhost port
fn get_free_port() -> u16 {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind :0");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_health_check_serving() {
    // Arrange: temp DB and free port
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let port = get_free_port();
    let addr = format!("127.0.0.1:{}", port);
    let url = format!("http://{}", addr);

    // Start server
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let db_path_clone = db_path.clone();
    let addr_clone = addr.clone();
    let server_task = tokio::spawn(async move {
        let _ = start_server(db_path_clone, &addr_clone, shutdown_rx).await;
    });

    // Retry connect loop to avoid race with server startup
    let mut client: Option<HealthClient<tonic::transport::Channel>> = None;
    let mut last_err: Option<tonic::transport::Error> = None;
    for _ in 0..40 {
        // up to ~2s
        match tonic::transport::Channel::from_shared(url.clone())
            .unwrap()
            .connect()
            .await
        {
            Ok(channel) => {
                client = Some(HealthClient::new(channel));
                break;
            }
            Err(e) => {
                last_err = Some(e);
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
    let mut client = client.unwrap_or_else(|| {
        panic!(
            "failed to connect health client after retries: {:?}",
            last_err
        )
    });

    // Act + Assert: overall service (empty service name) should be SERVING
    let resp = client
        .check(HealthCheckRequest {
            service: "".to_string(),
        })
        .await
        .expect("health check overall");
    assert_eq!(resp.into_inner().status, PbServingStatus::Serving as i32);

    // Act + Assert: named UmaDB service should be SERVING
    let resp = client
        .check(HealthCheckRequest {
            service: "umadb.v1.DCB".to_string(),
        })
        .await
        .expect("health check UmaDB");
    assert_eq!(resp.into_inner().status, PbServingStatus::Serving as i32);

    // Cleanup
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}
