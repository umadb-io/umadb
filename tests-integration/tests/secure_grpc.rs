use std::net::{Ipv4Addr, TcpListener};
use std::time::Duration;

use rcgen::generate_simple_self_signed;
use tempfile::tempdir;
use tokio::time::sleep;
use umadb_client::{AsyncUmaDBClient, ClientTlsOptions};
use umadb_dcb::{DCBEvent, DCBEventStoreAsync};
use umadb_server::start_server_secure;

// Helper to pick a free localhost port
fn get_free_port() -> u16 {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind :0");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn generate_self_signed_cert() -> (Vec<u8>, Vec<u8>) {
    let cert = generate_simple_self_signed(["localhost".to_string()]).expect("generate cert");
    let cert_pem = cert.serialize_pem().expect("serialize cert pem");
    let key_pem = cert.serialize_private_key_pem();
    (cert_pem.into_bytes(), key_pem.into_bytes())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secure_grpc_end_to_end_append_and_read() {
    // Arrange: temp DB and TLS materials
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let port = get_free_port();
    let addr = format!("127.0.0.1:{}", port);
    let url = format!("grpcs://localhost:{}", port);

    let (cert_pem, key_pem) = generate_self_signed_cert();

    // Start secure server
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let db_path_clone = db_path.clone();
    let addr_clone = addr.clone();
    let cert_clone = cert_pem.clone();
    let key_clone = key_pem.clone();
    let server_task = tokio::spawn(async move {
        let _ = start_server_secure(
            db_path_clone,
            &addr_clone,
            shutdown_rx,
            cert_clone,
            key_clone,
        )
        .await;
    });

    // Build TLS opts for client (trust the self-signed cert and use SNI localhost)
    let tls = ClientTlsOptions {
        domain: Some("localhost".to_string()),
        ca_pem: Some(cert_pem.clone()),
    };

    // Retry connect loop to avoid race with server startup
    let client = {
        let mut last_err = None;
        let mut client: Option<AsyncUmaDBClient> = None;
        for _ in 0..40 {
            // up to ~2s
            match AsyncUmaDBClient::connect_with_tls_options(
                url.clone(),
                Some(tls.clone()),
                None,
                None,
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
        client.unwrap_or_else(|| {
            panic!(
                "failed to connect to secure server after retries: {:?}",
                last_err
            )
        })
    };

    // Append a handful of events
    let events: Vec<DCBEvent> = (0..25)
        .map(|i| DCBEvent {
            event_type: "SecureTest".to_string(),
            data: format!("tls-data-{}", i).into_bytes(),
            tags: vec!["secure".to_string()],
            uuid: None,
        })
        .collect();
    let last_pos = client.append(events, None, None).await.expect("append");
    assert!(last_pos >= 25);

    // Read them back (no query means everything)
    let mut resp = client
        .read(None, None, false, None, false)
        .await
        .expect("read start");

    let mut total = 0usize;
    loop {
        let batch = resp.next_batch().await.expect("next_batch");
        if batch.is_empty() {
            break;
        }
        total += batch.len();
        // Ensure events have expected type/tag
        for ev in batch {
            assert_eq!("SecureTest", ev.event.event_type);
            assert!(ev.event.tags.contains(&"secure".to_string()));
        }
    }

    assert!(total >= 25, "should read back at least appended events");

    // Cleanup
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}
