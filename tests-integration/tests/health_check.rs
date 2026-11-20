use std::time::Duration;
use serial_test::serial;

// tonic-health client API
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus as PbServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tests_integration::{allocate_grpc_addr, TempServer};
use umadb_client::UmaDbClient;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn grpc_health_check_serving() {
    let server = TempServer::start_insecure().await;

    // Retry connect loop to avoid race with server startup
    let mut client: Option<HealthClient<tonic::transport::Channel>> = None;
    let mut last_err: Option<tonic::transport::Error> = None;
    for _ in 0..40 {
        // up to ~2s
        match tonic::transport::Channel::from_shared(server.url.clone())
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
                tokio::time::sleep(Duration::from_millis(50)).await;
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
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn grpc_health_check_via_umadb_client() {
    use umadb_client::ServingStatus;

    let server = TempServer::start_insecure().await;
    let client = server.client()
        .connect_async()
        .await
        .expect("connect async client");

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

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn grpc_async_check_health_server_not_started() {
    use umadb_client::UmaDbClient;

    // Use a port where no server is listening
    let (_, url) = allocate_grpc_addr();

    // connect will fail because no server is running
    let result = UmaDbClient::new(url).connect_async().await;
    assert!(
        result.is_err(),
        "expected connection to fail when server is not started"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn grpc_async_watch_health_server_not_started() {
    use umadb_client::UmaDbClient;

    let (_, url) = allocate_grpc_addr();

    let result = UmaDbClient::new(url).connect_async().await;
    assert!(
        result.is_err(),
        "expected connection to fail when server is not started"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn grpc_async_check_health_server_shutdown_before_client() {
    use umadb_client::ServingStatus;

    let server = TempServer::start_insecure().await;
    let client = server.client()
        .connect_async()
        .await
        .expect("connect async client");

    // Verify health is serving
    let status = client.check_health("").await.unwrap();
    assert_eq!(status, ServingStatus::Serving);

    // Shutdown server
    server.shutdown().await;

    // After shutdown, check_health should fail
    let result = client.check_health("").await;
    assert!(
        result.is_err(),
        "expected check_health to fail after server shutdown"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn grpc_async_watch_health_server_shutdown_before_client() {
    use umadb_client::ServingStatus;

    let server = TempServer::start_insecure().await;
    let client = server.client()
        .connect_async()
        .await
        .expect("connect async client");

    // Open a health watch stream
    let mut watch = client.watch_health("").await.unwrap();
    let first = watch.next().await.unwrap().expect("expected first status");
    assert_eq!(first, ServingStatus::Serving);

    // Shutdown server (signal first, then wait)
    let shutdownn_future = server.shutdown();

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
    shutdownn_future.await;
}

#[test]
#[serial]
fn grpc_sync_check_health_server_not_started() {
    use umadb_client::UmaDbClient;

    let (_, url) = allocate_grpc_addr();

    let result = UmaDbClient::new(url).connect();
    assert!(
        result.is_err(),
        "expected connection to fail when server is not started"
    );
}

#[test]
#[serial]
fn grpc_sync_watch_health_server_not_started() {
    use umadb_client::UmaDbClient;

    let (_, url) = allocate_grpc_addr();

    let result = UmaDbClient::new(url).connect();
    assert!(
        result.is_err(),
        "expected connection to fail when server is not started"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn grpc_sync_check_health_server_shutdown_before_client() {
    use umadb_client::ServingStatus;

    let server = TempServer::start_insecure().await;
    let http_url = server.url.clone();

    // PHASE 1: Connect and verify health while server is UP
    let client = tokio::task::spawn_blocking(move || {
        let client = UmaDbClient::new(http_url)
            .connect()
            .expect("connect sync client");

        let status = client.check_health("").unwrap();
        assert_eq!(status, ServingStatus::Serving);

        client // Return the connected client to the main async thread
    })
        .await
        .unwrap();

    // PHASE 2: Shut down the server
    // We .await this directly in the main async thread so we are 100%
    // guaranteed the server has completely exited and dropped the TCP connection.
    server.shutdown().await;

    // PHASE 3: Try to use the client while server is DOWN
    tokio::task::spawn_blocking(move || {
        // We moved the existing `client` into this new blocking task
        let result = client.check_health("");

        assert!(
            result.is_err(),
            "expected check_health to fail after server shutdown"
        );
    })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn grpc_sync_watch_health_server_shutdown_before_client() {
    use umadb_client::ServingStatus;

    let server = TempServer::start_insecure().await;
    let http_url = server.url.clone();

    let shutdown_future = tokio::task::spawn_blocking(move || {
        // Build client with small batch_size so we can test next_batch
        let client = UmaDbClient::new(http_url)
            .connect()
            .expect("connect sync client");

        let mut watch = client.watch_health("").unwrap();
        let first = watch.next().unwrap().expect("expected first status");
        assert_eq!(first, ServingStatus::Serving);

        // Shutdown server (signal first)
        let shutdown_future = server.shutdown();

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

        shutdown_future

    }).await.unwrap();

    shutdown_future.await;
}
