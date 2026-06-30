use std::net::{Ipv4Addr, TcpListener};
use std::path::PathBuf;
use std::time::Duration;

use rcgen::generate_simple_self_signed;
use tempfile::tempdir;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use umadb_client::{AsyncUmaDbClient, ClientTlsOptions, UmaDbClient};
use umadb_core::mvcc::StorageOptions;
use umadb_dcb::{DcbError, DcbEvent, DcbEventStoreAsync};
use umadb_server::{ServerOptions, ServerTlsOptions, start_server_with_options};

// Helper to pick a free localhost port
fn get_free_port() -> u16 {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind :0");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn generate_self_signed_cert() -> (Vec<u8>, Vec<u8>) {
    let certified_key =
        generate_simple_self_signed(["localhost".to_string()]).expect("generate cert");
    let cert_pem = certified_key.cert.pem();
    let key_pem = certified_key.signing_key.serialize_pem();
    (cert_pem.into_bytes(), key_pem.into_bytes())
}

fn spawn_server_with_tls_and_api_key(
    shutdown_rx: Receiver<()>,
    db_path: PathBuf,
    listen_addr: String,
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    api_key: String,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let options = ServerOptions {
            listen_addr,
            tls: Some(ServerTlsOptions { cert_pem, key_pem }),
            api_key: Some(api_key),
            storage: StorageOptions::default().db_path(db_path),
        };
        let _ = start_server_with_options(options, shutdown_rx).await;
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_key_success_over_tls() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let port = get_free_port();
    let addr = format!("127.0.0.1:{}", port);
    let url = format!("grpcs://localhost:{}", port);

    let (cert_pem, key_pem) = generate_self_signed_cert();
    let api_key = "k123".to_string();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let _server_task = spawn_server_with_tls_and_api_key(
        shutdown_rx,
        db_path.clone(),
        addr.clone(),
        cert_pem.clone(),
        key_pem.clone(),
        api_key.clone(),
    );

    // Build TLS opts for client (trust the self-signed cert and use SNI localhost)
    let tls = ClientTlsOptions {
        domain: Some("localhost".to_string()),
        ca_pem: Some(cert_pem),
    };

    // Connect with retries
    let client = {
        let mut last_err = None;
        let mut client: Option<AsyncUmaDbClient> = None;
        for _ in 0..40 {
            match AsyncUmaDbClient::connect_with_tls_options(
                url.clone(),
                Some(tls.clone()),
                None,
                Some(api_key.clone()),
            )
            .await
            {
                Ok(c) => {
                    client = Some(c);
                    break;
                }
                Err(e) => {
                    last_err = Some(e);
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
        client.unwrap_or_else(|| panic!("client connect failed: {:?}", last_err))
    };

    // Append and read should succeed
    let events: Vec<DcbEvent> = (0..3)
        .map(|i| DcbEvent {
            event_type: "AuthTest".to_string(),
            data: format!("data-{}", i).into_bytes(),
            tags: vec!["t".to_string()],
            uuid: None,
            metadata: Vec::new(),
        })
        .collect();
    let last_pos = client.append(events, None, None).await.expect("append");
    assert!(last_pos >= 3);

    let head = client.head().await.expect("head");
    assert!(head.is_some());

    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_key_wrong_fails_over_tls() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let port = get_free_port();
    let addr = format!("127.0.0.1:{}", port);
    let url = format!("grpcs://localhost:{}", port);

    let (cert_pem, key_pem) = generate_self_signed_cert();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let _server_task = spawn_server_with_tls_and_api_key(
        shutdown_rx,
        db_path.clone(),
        addr.clone(),
        cert_pem.clone(),
        key_pem.clone(),
        "expected".to_string(),
    );

    // Build TLS opts for client
    let tls = ClientTlsOptions {
        domain: Some("localhost".to_string()),
        ca_pem: Some(cert_pem.clone()),
    };

    // Connect
    let client = {
        let mut last_err = None;
        let mut client: Option<AsyncUmaDbClient> = None;
        for _ in 0..40 {
            match AsyncUmaDbClient::connect_with_tls_options(
                url.clone(),
                Some(tls.clone()),
                None,
                Some("wrong".to_string()),
            )
            .await
            {
                Ok(c) => {
                    client = Some(c);
                    break;
                }
                Err(e) => {
                    last_err = Some(e);
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
        client.unwrap_or_else(|| panic!("client connect failed: {:?}", last_err))
    };

    // Calls should fail with Unauthenticated mapped to TransportError/Io (depending on mapping)
    let err = client.head().await.err().expect("head should fail");
    match err {
        DcbError::AuthenticationError(_) => {}
        other => panic!("unexpected error type: {:?}", other),
    }

    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_key_rejected_over_insecure_http() {
    // Client configured with API key and http URL should error before sending
    let url = "http://localhost:9".to_string();
    let builder = UmaDbClient::new(url.clone()).api_key("k".to_string());
    let client = builder.connect_async().await;
    // connect may succeed because channel creation may not connect immediately; perform a call
    if let Ok(c) = client {
        let err = c
            .head()
            .await
            .err()
            .expect("head must fail due to insecure");
        match err {
            DcbError::TransportError(msg) => assert!(msg.contains("TLS")),
            other => panic!("unexpected error type: {:?}", other),
        }
    }
}
