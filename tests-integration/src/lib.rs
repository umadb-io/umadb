use std::fs::File;
use std::io::Write;
use rcgen::generate_simple_self_signed;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::time::Duration;
use tempfile::{TempDir, tempdir};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus as PbServingStatus;
use tonic_health::pb::health_client::HealthClient;
use umadb_client::UmaDbClient;
use umadb_core::mvcc::StorageOptions;
use umadb_server::{ServerOptions, ServerTlsOptions, start_server, start_server_with_options};

pub fn allocate_grpc_addr() -> (String, String) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("resolve local addr");
    drop(listener);
    (format!("{addr}"), format!("http://{addr}"))
}

pub struct TempServer {
    _temp_dir: TempDir,
    shutdown_tx: Sender<()>,
    server_task: JoinHandle<()>,
    pub bound_addr: SocketAddr,
    pub url: String,
    pub is_secure: bool,
    pub cert_path: Option<PathBuf>,
    pub api_key: Option<String>,
}

impl TempServer {
    pub async fn start_insecure() -> TempServer{
        Self::_start(false).await
    }

    pub async fn start_secure() -> TempServer{
        Self::_start(true).await
    }

    async fn _start(is_secure: bool) -> TempServer{
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().to_path_buf();

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<std::net::SocketAddr>();

        let (server_task, bound_addr, url, cert_path, api_key ) = if is_secure {
            // Start server in background
            let (cert_pem, key_pem) = generate_self_signed_cert();
            let cert_path = temp_dir.path().join("ca_cert.txt");
            let mut cert_file = File::create(cert_path.clone()).expect("Failed to create file");
            cert_file.write(&cert_pem).expect("Failed to write PEM to temp cert file");
            let api_key = "k123";
            let server_task = tokio::spawn(async move {
                let options = ServerOptions {
                    listen_addr: "127.0.0.1:0".to_string(),
                    tls: Some(ServerTlsOptions { cert_pem, key_pem }),
                    api_key: Some(api_key.to_string()),
                    storage: StorageOptions::default().db_path(db_path),
                };
                let _ = start_server_with_options(options, shutdown_rx, Some(ready_tx)).await;
            });

            let bound_addr = ready_rx.await.expect("Server panicked or failed to bind before sending address");
            let url = format!("https://{}", bound_addr);
            (server_task, bound_addr, url, Some(cert_path), Some(api_key.to_string()))
        } else {
            // Start server in background
            let server_task = tokio::spawn(async move {
                let _ = start_server(db_path, "127.0.0.1:0", shutdown_rx, Some(ready_tx)).await;
            });

            let bound_addr = ready_rx.await.expect("Server panicked or failed to bind before sending address");
            let url = format!("http://{}", bound_addr);
            Self::wait_for_health(&url).await;
            (server_task, bound_addr, url, None, None)
        };

        Self {
            _temp_dir: temp_dir,
            shutdown_tx,
            server_task,
            bound_addr,
            url,
            is_secure,
            cert_path,
            api_key,
        }
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

    pub fn client(&self) -> UmaDbClient {
        let mut client = UmaDbClient::new(self.url.clone());
        if self.is_secure {
            if self.api_key.is_some() {
                client = client.api_key(self.api_key.clone().unwrap().to_string());
            }
            if self.cert_path.is_some() {
                client = client.ca_path(self.cert_path.clone().unwrap().into_string().unwrap());
            }
        }
        client
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        let _ = self.server_task.await;
    }
}

pub fn generate_self_signed_cert() -> (Vec<u8>, Vec<u8>) {
    let certified_key =
        generate_simple_self_signed(["localhost".to_string()]).expect("generate cert");
    let cert_pem = certified_key.cert.pem();
    let key_pem = certified_key.signing_key.serialize_pem();
    (cert_pem.into_bytes(), key_pem.into_bytes())
}
