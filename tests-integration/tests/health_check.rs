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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_health_check_via_umadb_client() {
    use umadb_client::{ServingStatus, UmaDbClient};

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

    // Connect via UmaDbClient (retry loop handled internally by connect)
    let client = {
        let mut last_err = None;
        let mut c = None;
        for _ in 0..40 {
            match UmaDbClient::new(url.clone()).connect_async().await {
                Ok(cl) => {
                    c = Some(cl);
                    break;
                }
                Err(e) => {
                    last_err = Some(e);
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
        c.unwrap_or_else(|| panic!("failed to connect: {:?}", last_err))
    };

    // Act + Assert: check_health for overall service
    let status = client.check_health("").await.unwrap();
    assert_eq!(status, ServingStatus::Serving);

    // Act + Assert: check_health for named service
    let status = client.check_health("umadb.v1.DCB").await.unwrap();
    assert_eq!(status, ServingStatus::Serving);

    // Act + Assert: watch_health returns a stream; first status should be SERVING
    {
        let mut watch = client.watch_health("").await.unwrap();
        let first = watch
            .next()
            .await
            .unwrap()
            .expect("expected at least one watch response");
        assert_eq!(first, ServingStatus::Serving);
    } // watch dropped here so the server can shut down cleanly

    // Cleanup: drop client before signalling shutdown so all streams are closed
    drop(client);
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_async_check_health_server_not_started() {
    use umadb_client::UmaDbClient;

    // Use a port where no server is listening
    let port = get_free_port();
    let url = format!("http://127.0.0.1:{}", port);

    // connect will fail because no server is running
    let result = UmaDbClient::new(url).connect_async().await;
    assert!(
        result.is_err(),
        "expected connection to fail when server is not started"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_async_watch_health_server_not_started() {
    use umadb_client::UmaDbClient;

    let port = get_free_port();
    let url = format!("http://127.0.0.1:{}", port);

    let result = UmaDbClient::new(url).connect_async().await;
    assert!(
        result.is_err(),
        "expected connection to fail when server is not started"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_async_check_health_server_shutdown_before_client() {
    use umadb_client::{ServingStatus, UmaDbClient};

    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let port = get_free_port();
    let addr = format!("127.0.0.1:{}", port);
    let url = format!("http://{}", addr);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let db_path_clone = db_path.clone();
    let addr_clone = addr.clone();
    let server_task = tokio::spawn(async move {
        let _ = start_server(db_path_clone, &addr_clone, shutdown_rx).await;
    });

    let client = {
        let mut c = None;
        for _ in 0..40 {
            match UmaDbClient::new(url.clone()).connect_async().await {
                Ok(cl) => {
                    c = Some(cl);
                    break;
                }
                Err(_) => {
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
        c.expect("failed to connect")
    };

    // Verify health is serving
    let status = client.check_health("").await.unwrap();
    assert_eq!(status, ServingStatus::Serving);

    // Shutdown server
    let _ = shutdown_tx.send(());
    let _ = server_task.await;

    // After shutdown, check_health should fail
    let result = client.check_health("").await;
    assert!(
        result.is_err(),
        "expected check_health to fail after server shutdown"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_async_watch_health_server_shutdown_before_client() {
    use umadb_client::{ServingStatus, UmaDbClient};

    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let port = get_free_port();
    let addr = format!("127.0.0.1:{}", port);
    let url = format!("http://{}", addr);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let db_path_clone = db_path.clone();
    let addr_clone = addr.clone();
    let server_task = tokio::spawn(async move {
        let _ = start_server(db_path_clone, &addr_clone, shutdown_rx).await;
    });

    let client = {
        let mut c = None;
        for _ in 0..40 {
            match UmaDbClient::new(url.clone()).connect_async().await {
                Ok(cl) => {
                    c = Some(cl);
                    break;
                }
                Err(_) => {
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
        c.expect("failed to connect")
    };

    // Open a health watch stream
    let mut watch = client.watch_health("").await.unwrap();
    let first = watch.next().await.unwrap().expect("expected first status");
    assert_eq!(first, ServingStatus::Serving);

    // Shutdown server (signal first, then wait)
    let _ = shutdown_tx.send(());

    // After shutdown signal, the watch stream should eventually end or report
    // a non-SERVING status. The server may send NOT_SERVING before closing.
    let mut got_end_or_non_serving = false;
    for _ in 0..10 {
        let result = watch.next_timeout(Duration::from_secs(2)).await;
        match result {
            Ok(None) => {
                got_end_or_non_serving = true;
                break;
            }
            Err(_) => {
                got_end_or_non_serving = true;
                break;
            }
            Ok(Some(status)) if status != ServingStatus::Serving => {
                got_end_or_non_serving = true;
                break;
            }
            Ok(Some(_)) => continue, // might get one more SERVING before shutdown propagates
        }
    }
    assert!(
        got_end_or_non_serving,
        "expected watch to end, error, or report non-SERVING after server shutdown"
    );

    // Clean up: drop watch and client before awaiting server task
    drop(watch);
    drop(client);
    let _ = server_task.await;
}

#[test]
fn grpc_sync_check_health_server_not_started() {
    use umadb_client::UmaDbClient;

    let port = get_free_port();
    let url = format!("http://127.0.0.1:{}", port);

    let result = UmaDbClient::new(url).connect();
    assert!(
        result.is_err(),
        "expected connection to fail when server is not started"
    );
}

#[test]
fn grpc_sync_watch_health_server_not_started() {
    use umadb_client::UmaDbClient;

    let port = get_free_port();
    let url = format!("http://127.0.0.1:{}", port);

    let result = UmaDbClient::new(url).connect();
    assert!(
        result.is_err(),
        "expected connection to fail when server is not started"
    );
}

#[test]
fn grpc_sync_check_health_server_shutdown_before_client() {
    use umadb_client::{ServingStatus, UmaDbClient};

    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let port = get_free_port();
    let addr = format!("127.0.0.1:{}", port);
    let url = format!("http://{}", addr);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let db_path_clone = db_path.clone();
    let addr_clone = addr.clone();
    let server_thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let _ = start_server(db_path_clone, &addr_clone, shutdown_rx).await;
        });
    });

    // Wait for server to start
    for _ in 0..40 {
        if std::net::TcpStream::connect(&addr).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    let client = UmaDbClient::new(url).connect().expect("failed to connect");

    let status = client.check_health("").unwrap();
    assert_eq!(status, ServingStatus::Serving);

    // Shutdown server
    let _ = shutdown_tx.send(());
    let _ = server_thread.join();

    // After shutdown, check_health should fail
    let result = client.check_health("");
    assert!(
        result.is_err(),
        "expected check_health to fail after server shutdown"
    );
}

#[test]
fn grpc_sync_watch_health_server_shutdown_before_client() {
    use umadb_client::{ServingStatus, UmaDbClient};

    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let port = get_free_port();
    let addr = format!("127.0.0.1:{}", port);
    let url = format!("http://{}", addr);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let db_path_clone = db_path.clone();
    let addr_clone = addr.clone();
    let server_thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let _ = start_server(db_path_clone, &addr_clone, shutdown_rx).await;
        });
    });

    // Wait for server to start
    for _ in 0..40 {
        if std::net::TcpStream::connect(&addr).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    let client = UmaDbClient::new(url).connect().expect("failed to connect");

    let mut watch = client.watch_health("").unwrap();
    let first = watch.next().unwrap().expect("expected first status");
    assert_eq!(first, ServingStatus::Serving);

    // Shutdown server (signal first)
    let _ = shutdown_tx.send(());

    // After shutdown signal, the watch stream should eventually end or report
    // a non-SERVING status. The server may send NOT_SERVING before closing.
    let mut got_end_or_non_serving = false;
    for _ in 0..10 {
        let result = watch.next_timeout(Duration::from_secs(2));
        match result {
            Ok(Some(ServingStatus::Serving)) => continue,
            _ => {
                got_end_or_non_serving = true;
                break;
            }
        }
    }
    assert!(
        got_end_or_non_serving,
        "expected watch to end, error, or report non-SERVING after server shutdown"
    );

    // Clean up
    drop(watch);
    drop(client);
    let _ = server_thread.join();
}
