use futures::Stream;
use std::fs;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::thread;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Identity, ServerTlsConfig};
use tonic::{Request, Response, Status, transport::Server};
use umadb_core::db::{
    DEFAULT_DB_FILENAME, DEFAULT_PAGE_SIZE, UmaDB, is_request_idempotent, read_conditional,
};
use umadb_core::mvcc::Mvcc;
use umadb_dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBQuery, DCBResult, DCBSequencedEvent, TrackingInfo,
};

use tokio::runtime::Runtime;
use tonic::codegen::http;
use tonic::transport::server::TcpIncoming;
use umadb_core::common::Position;

use std::convert::Infallible;
use std::future::Future;
use std::task::{Context, Poll};
use tonic::server::NamedService;
use umadb_proto::status_from_dcb_error;

// This is just to maintain compatibility for the very early unversioned API (pre-v1).
#[derive(Clone, Debug)]
pub struct PathRewriterService<S> {
    inner: S,
}

impl<S> tower::Service<http::Request<tonic::body::Body>> for PathRewriterService<S>
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<tonic::body::Body>) -> Self::Future {
        let uri = req.uri().clone();
        let path = uri.path();

        // Check and rewrite the path string first
        if path.starts_with("/umadb.UmaDBService/") {
            let new_path_str = path.replace("/umadb.UmaDBService/", "/umadb.v1.DCB/");

            // Use the existing authority and scheme if present, otherwise default to a simple path-only URI structure
            // which is often safer than hardcoded hostnames in internal systems.
            let new_uri = if let (Some(scheme), Some(authority)) = (uri.scheme(), uri.authority()) {
                // If we have all components, try to build the full URI
                http::Uri::builder()
                    .scheme(scheme.clone())
                    .authority(authority.clone())
                    .path_and_query(new_path_str.as_str())
                    .build()
                    .ok() // Convert the final build Result into an Option
            } else {
                // Fallback for malformed requests (missing scheme/authority)
                // Just try to build a path-only URI
                new_path_str.parse::<http::Uri>().ok()
            };

            if let Some(final_uri) = new_uri {
                *req.uri_mut() = final_uri;
            } else {
                eprintln!("Failed to construct valid URI for path: {}", path);
            }
        }

        let fut = self.inner.call(req);
        Box::pin(fut)
    }
}

// Add this implementation to satisfy the compiler error
impl<S: NamedService> NamedService for PathRewriterService<S> {
    const NAME: &'static str = S::NAME;
}

#[derive(Clone, Debug)]
pub struct PathRewriterLayer;

impl<S> tower::Layer<S> for PathRewriterLayer
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Service = PathRewriterService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        PathRewriterService { inner }
    }
}

static START_TIME: LazyLock<Instant> = LazyLock::new(Instant::now);

const APPEND_BATCH_MAX_EVENTS: usize = 2000;
const READ_RESPONSE_BATCH_SIZE_DEFAULT: u32 = 100;
const READ_RESPONSE_BATCH_SIZE_MAX: u32 = 5000;

// Optional TLS configuration helpers
#[derive(Clone, Debug)]
pub struct ServerTlsOptions {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

pub fn uptime() -> std::time::Duration {
    START_TIME.elapsed()
}

fn build_server_builder_with_options(tls: Option<ServerTlsOptions>) -> Server {
    use std::time::Duration;
    let mut server_builder = Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(5)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)))
        .initial_stream_window_size(Some(4 * 1024 * 1024))
        .initial_connection_window_size(Some(8 * 1024 * 1024))
        .tcp_nodelay(true)
        .concurrency_limit_per_connection(1024);

    if let Some(opts) = tls {
        let identity = Identity::from_pem(opts.cert_pem, opts.key_pem);
        server_builder = server_builder
            .tls_config(ServerTlsConfig::new().identity(identity))
            .expect("failed to apply TLS config");
    }

    server_builder
}

// Function to start the gRPC server with a shutdown signal
pub async fn start_server<P: AsRef<Path> + Send + 'static>(
    path: P,
    addr: &str,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    start_server_internal(path, addr, shutdown_rx, None, None).await
}

/// Start server with TLS using PEM-encoded cert and key.
pub async fn start_server_secure<P: AsRef<Path> + Send + 'static>(
    path: P,
    addr: &str,
    shutdown_rx: oneshot::Receiver<()>,
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let tls = ServerTlsOptions { cert_pem, key_pem };
    start_server_internal(path, addr, shutdown_rx, Some(tls), None).await
}

/// Convenience: load cert and key from filesystem paths
pub async fn start_server_secure_from_files<
    P: AsRef<Path> + Send + 'static,
    CP: AsRef<Path>,
    KP: AsRef<Path>,
>(
    path: P,
    addr: &str,
    shutdown_rx: oneshot::Receiver<()>,
    cert_path: CP,
    key_path: KP,
) -> Result<(), Box<dyn std::error::Error>> {
    let cert_path_ref = cert_path.as_ref();
    let cert_pem = fs::read(cert_path_ref).map_err(|e| -> Box<dyn std::error::Error> {
        format!(
            "Failed to open TLS certificate file '{}': {}",
            cert_path_ref.display(),
            e
        )
        .into()
    })?;

    let key_path_ref = key_path.as_ref();
    let key_pem = fs::read(key_path_ref).map_err(|e| -> Box<dyn std::error::Error> {
        format!(
            "Failed to open TLS key file '{}': {}",
            key_path_ref.display(),
            e
        )
        .into()
    })?;
    start_server_secure(path, addr, shutdown_rx, cert_pem, key_pem).await
}

/// Start server (insecure) requiring an API key for all RPCs.
pub async fn start_server_with_api_key<P: AsRef<Path> + Send + 'static>(
    path: P,
    addr: &str,
    shutdown_rx: oneshot::Receiver<()>,
    api_key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    start_server_internal(path, addr, shutdown_rx, None, Some(api_key)).await
}

/// Start TLS server requiring an API key for all RPCs.
pub async fn start_server_secure_with_api_key<P: AsRef<Path> + Send + 'static>(
    path: P,
    addr: &str,
    shutdown_rx: oneshot::Receiver<()>,
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    api_key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let tls = ServerTlsOptions { cert_pem, key_pem };
    start_server_internal(path, addr, shutdown_rx, Some(tls), Some(api_key)).await
}

/// TLS server from files requiring an API key
pub async fn start_server_secure_from_files_with_api_key<
    P: AsRef<Path> + Send + 'static,
    CP: AsRef<Path>,
    KP: AsRef<Path>,
>(
    path: P,
    addr: &str,
    shutdown_rx: oneshot::Receiver<()>,
    cert_path: CP,
    key_path: KP,
    api_key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let cert_path_ref = cert_path.as_ref();
    let cert_pem = fs::read(cert_path_ref).map_err(|e| -> Box<dyn std::error::Error> {
        format!(
            "Failed to open TLS certificate file '{}': {}",
            cert_path_ref.display(),
            e
        )
        .into()
    })?;

    let key_path_ref = key_path.as_ref();
    let key_pem = fs::read(key_path_ref).map_err(|e| -> Box<dyn std::error::Error> {
        format!(
            "Failed to open TLS key file '{}': {}",
            key_path_ref.display(),
            e
        )
        .into()
    })?;
    start_server_secure_with_api_key(path, addr, shutdown_rx, cert_pem, key_pem, api_key).await
}

async fn start_server_internal<P: AsRef<Path> + Send + 'static>(
    path: P,
    addr: &str,
    shutdown_rx: oneshot::Receiver<()>,
    tls: Option<ServerTlsOptions>,
    api_key: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.parse()?;
    // ---- Bind incoming manually like tonic ----
    let incoming = match TcpIncoming::bind(addr) {
        Ok(incoming) => incoming,
        Err(err) => {
            return Err(Box::new(DCBError::InitializationError(format!(
                "Failed to bind to address {}: {}",
                addr, err
            ))));
        }
    }
    .with_nodelay(Some(true))
    .with_keepalive(Some(std::time::Duration::from_secs(60)));

    // Create a shutdown broadcast channel for terminating ongoing subscriptions
    let (srv_shutdown_tx, srv_shutdown_rx) = watch::channel(false);
    let dcb_server =
        match DCBServer::new(path.as_ref().to_owned(), srv_shutdown_rx, api_key.clone()) {
            Ok(server) => server,
            Err(err) => {
                return Err(Box::new(err));
            }
        };

    println!(
        "UmaDB has {:?} events",
        dcb_server.request_handler.head().await?.unwrap_or(0)
    );
    let tls_mode_display_str = if tls.is_some() {
        "with TLS"
    } else {
        "without TLS"
    };

    let api_key_display_str = if api_key.is_some() {
        "with API key"
    } else {
        "without API key"
    };

    // gRPC Health service setup
    use tonic_health::ServingStatus; // server API expects this enum
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    // Set overall and service-specific health to SERVING
    health_reporter
        .set_service_status("", ServingStatus::Serving)
        .await;
    health_reporter
        .set_service_status("umadb.v1.DCB", ServingStatus::Serving)
        .await;
    let health_reporter_for_shutdown = health_reporter.clone();

    // Apply PathRewriterLayer at the server level to intercept all requests before routing
    let mut builder = build_server_builder_with_options(tls)
        .layer(PathRewriterLayer)
        .add_service(health_service);

    // Add DCB service (auth enforced inside RPC handlers if configured)
    builder = builder.add_service(dcb_server.into_service());
    let router = builder;

    println!("UmaDB is listening on {addr} ({tls_mode_display_str}, {api_key_display_str})");
    println!("UmaDB started in {:?}", uptime());
    // let incoming = router.server.bind_incoming();
    router
        .serve_with_incoming_shutdown(incoming, async move {
            // Wait for an external shutdown trigger
            let _ = shutdown_rx.await;
            // Mark health as NOT_SERVING before shutdown
            let _ = health_reporter_for_shutdown
                .set_service_status("", ServingStatus::NotServing)
                .await;
            let _ = health_reporter_for_shutdown
                .set_service_status("umadb.v1.DCB", ServingStatus::NotServing)
                .await;
            // Broadcast shutdown to all subscription tasks
            let _ = srv_shutdown_tx.send(true);
            println!("UmaDB server shutdown complete");
        })
        .await?;

    Ok(())
}

// gRPC server implementation
pub struct DCBServer {
    pub(crate) request_handler: RequestHandler,
    shutdown_watch_rx: watch::Receiver<bool>,
    api_key: Option<String>,
}

impl DCBServer {
    pub fn new<P: AsRef<Path> + Send + 'static>(
        path: P,
        shutdown_rx: watch::Receiver<bool>,
        api_key: Option<String>,
    ) -> DCBResult<Self> {
        let command_handler = RequestHandler::new(path)?;
        Ok(Self {
            request_handler: command_handler,
            shutdown_watch_rx: shutdown_rx,
            api_key,
        })
    }

    pub fn into_service(self) -> umadb_proto::v1::dcb_server::DcbServer<Self> {
        umadb_proto::v1::dcb_server::DcbServer::new(self)
    }
}

#[tonic::async_trait]
impl umadb_proto::v1::dcb_server::Dcb for DCBServer {
    type ReadStream =
        Pin<Box<dyn Stream<Item = Result<umadb_proto::v1::ReadResponse, Status>> + Send + 'static>>;

    async fn read(
        &self,
        request: Request<umadb_proto::v1::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        // Enforce API key if configured
        if let Some(expected) = &self.api_key {
            let auth = request.metadata().get("authorization");
            let expected_val = format!("Bearer {}", expected);
            let ok = auth
                .and_then(|m| m.to_str().ok())
                .map(|s| s == expected_val)
                .unwrap_or(false);
            if !ok {
                return Err(status_from_dcb_error(DCBError::AuthenticationError(
                    "missing or invalid API key".to_string(),
                )));
            }
        }
        let read_request = request.into_inner();

        // Convert protobuf query to DCB types
        let mut query: Option<DCBQuery> = read_request.query.map(|q| q.into());
        let start = read_request.start;
        let backwards = read_request.backwards.unwrap_or(false);
        let limit = read_request.limit;
        // Cap requested batch size.
        let batch_size = read_request
            .batch_size
            .unwrap_or(READ_RESPONSE_BATCH_SIZE_DEFAULT)
            .clamp(1, READ_RESPONSE_BATCH_SIZE_MAX);
        let subscribe = read_request.subscribe.unwrap_or(false);

        // Create a channel for streaming responses (deeper buffer to reduce backpressure under concurrency)
        let (tx, rx) = mpsc::channel(2048);
        // Clone the request handler.
        let request_handler = self.request_handler.clone();
        // Clone the shutdown watch receiver.
        let mut shutdown_watch_rx = self.shutdown_watch_rx.clone();

        // Spawn a task to handle the read operation and stream multiple batches
        tokio::spawn(async move {
            // Ensure we can reuse the same query across batches
            let query_clone = query.take();
            let mut next_start = start;
            let mut sent_any = false;
            let mut remaining_limit = limit.unwrap_or(u32::MAX);
            // Create a watch receiver for head updates (for subscriptions)
            // TODO: Make this an Option and only do this for subscriptions?
            let mut head_rx = request_handler.watch_head();
            // If non-subscription read, capture head to preserve point-in-time semantics
            let captured_head = if !subscribe {
                request_handler.head().await.unwrap_or(None)
            } else {
                None
            };
            loop {
                // If this is a subscription, exit if the client
                // has gone away or the server is shutting down.
                if subscribe {
                    if tx.is_closed() {
                        break;
                    }
                    if *shutdown_watch_rx.borrow() {
                        break;
                    }
                }
                // Determine per-iteration limit.
                let read_limit = remaining_limit.min(batch_size);
                // If subscription and remaining exhausted (limit reached), terminate
                if subscribe && limit.is_some() && remaining_limit == 0 {
                    break;
                }
                match request_handler
                    .read(query_clone.clone(), next_start, backwards, Some(read_limit))
                    .await
                {
                    Ok((dcb_sequenced_events, head)) => {
                        // Capture the original length before consuming events
                        let original_len = dcb_sequenced_events.len();

                        // Filter and map events, discarding those with position > captured_head
                        let sequenced_event_protos: Vec<umadb_proto::v1::SequencedEvent> =
                            dcb_sequenced_events
                                .into_iter()
                                .filter(|e| {
                                    if let Some(h) = captured_head {
                                        e.position <= h
                                    } else {
                                        true
                                    }
                                })
                                .map(umadb_proto::v1::SequencedEvent::from)
                                .collect();

                        let reached_captured_head = if captured_head.is_some() {
                            // Check if we filtered out any events
                            sequenced_event_protos.len() < original_len
                        } else {
                            false
                        };

                        // Calculate head to send based on context
                        // For subscriptions: use current head
                        // For unlimited non-subscription reads: use captured_head
                        // For limited reads: use last event position (or current head if empty)
                        let last_event_position = sequenced_event_protos.last().map(|e| e.position);
                        let head_to_send = if subscribe {
                            head
                        } else if limit.is_none() {
                            captured_head
                        } else {
                            last_event_position.or(head)
                        };

                        if sequenced_event_protos.is_empty() {
                            // Only send an empty response to communicate head if this is the first
                            if !sent_any {
                                let response = umadb_proto::v1::ReadResponse {
                                    events: vec![],
                                    head: head_to_send,
                                };
                                let _ = tx.send(Ok(response)).await;
                            }
                            // For subscriptions, wait for new events instead of terminating
                            if subscribe {
                                // Wait for either a new head or a server shutdown signal
                                tokio::select! {
                                    _ = head_rx.changed() => {},
                                    _ = shutdown_watch_rx.changed() => {},
                                    _ = tx.closed() => {},
                                }
                                continue;
                            }
                            break;
                        }

                        // Capture values needed after sequenced_event_protos is moved
                        let sent_count = sequenced_event_protos.len() as u32;

                        let response = umadb_proto::v1::ReadResponse {
                            events: sequenced_event_protos,
                            head: head_to_send,
                        };

                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                        sent_any = true;

                        // Advance the cursor (use a new reader on the next loop iteration)
                        next_start =
                            last_event_position.map(|p| if !backwards { p + 1 } else { p - 1 });

                        // Stop streaming further if we reached the
                        // captured head boundary (non-subscriber only).
                        if reached_captured_head && !subscribe {
                            break;
                        }

                        // Decrease the remaining overall limit if any, and stop if reached
                        if limit.is_some() {
                            if remaining_limit <= sent_count {
                                remaining_limit = 0;
                            } else {
                                remaining_limit -= sent_count;
                            }
                            if remaining_limit == 0 {
                                break;
                            }
                        }

                        // Yield to let other tasks progress under high concurrency
                        tokio::task::yield_now().await;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(status_from_dcb_error(e))).await;
                        break;
                    }
                }
            }
        });

        // Return the receiver as a stream
        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::ReadStream
        ))
    }

    async fn append(
        &self,
        request: Request<umadb_proto::v1::AppendRequest>,
    ) -> Result<Response<umadb_proto::v1::AppendResponse>, Status> {
        // Enforce API key if configured
        if let Some(expected) = &self.api_key {
            let auth = request.metadata().get("authorization");
            let expected_val = format!("Bearer {}", expected);
            let ok = auth
                .and_then(|m| m.to_str().ok())
                .map(|s| s == expected_val)
                .unwrap_or(false);
            if !ok {
                return Err(status_from_dcb_error(DCBError::AuthenticationError(
                    "missing or invalid API key".to_string(),
                )));
            }
        }
        let req = request.into_inner();

        // Convert protobuf types to API types
        let events: Vec<DCBEvent> = match req.events.into_iter().map(|e| e.try_into()).collect() {
            Ok(events) => events,
            Err(e) => {
                return Err(status_from_dcb_error(e));
            }
        };
        let condition = req.condition.map(|c| c.into());

        // Call the event store append method
        match self
            .request_handler
            .append(
                events,
                condition,
                req.tracking_info.map(|t| TrackingInfo {
                    source: t.source,
                    position: t.position,
                }),
            )
            .await
        {
            Ok(position) => Ok(Response::new(umadb_proto::v1::AppendResponse { position })),
            Err(e) => Err(status_from_dcb_error(e)),
        }
    }

    async fn head(
        &self,
        request: Request<umadb_proto::v1::HeadRequest>,
    ) -> Result<Response<umadb_proto::v1::HeadResponse>, Status> {
        // Enforce API key if configured
        if let Some(expected) = &self.api_key {
            let auth = request.metadata().get("authorization");
            let expected_val = format!("Bearer {}", expected);
            let ok = auth
                .and_then(|m| m.to_str().ok())
                .map(|s| s == expected_val)
                .unwrap_or(false);
            if !ok {
                return Err(status_from_dcb_error(DCBError::AuthenticationError(
                    "missing or invalid API key".to_string(),
                )));
            }
        }
        // Call the event store head method
        match self.request_handler.head().await {
            Ok(position) => {
                // Return the position as a response
                Ok(Response::new(umadb_proto::v1::HeadResponse { position }))
            }
            Err(e) => Err(status_from_dcb_error(e)),
        }
    }

    async fn get_tracking_info(
        &self,
        request: Request<umadb_proto::v1::TrackingRequest>,
    ) -> Result<Response<umadb_proto::v1::TrackingResponse>, Status> {
        // Enforce API key if configured
        if let Some(expected) = &self.api_key {
            let auth = request.metadata().get("authorization");
            let expected_val = format!("Bearer {}", expected);
            let ok = auth
                .and_then(|m| m.to_str().ok())
                .map(|s| s == expected_val)
                .unwrap_or(false);
            if !ok {
                return Err(status_from_dcb_error(DCBError::AuthenticationError(
                    "missing or invalid API key".to_string(),
                )));
            }
        }
        let req = request.into_inner();
        match self.request_handler.get_tracking_info(req.source).await {
            Ok(position) => Ok(Response::new(umadb_proto::v1::TrackingResponse {
                position,
            })),
            Err(e) => Err(status_from_dcb_error(e)),
        }
    }
}

// Message types for communication between the gRPC server and the request handler's writer thread
enum WriterRequest {
    Append {
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
        tracking_info: Option<TrackingInfo>,
        response_tx: oneshot::Sender<DCBResult<u64>>,
    },
    Shutdown,
}

// Thread-safe request handler
struct RequestHandler {
    mvcc: Arc<Mvcc>,
    head_watch_tx: watch::Sender<Option<u64>>,
    writer_request_tx: mpsc::Sender<WriterRequest>,
}

impl RequestHandler {
    fn new<P: AsRef<Path> + Send + 'static>(path: P) -> DCBResult<Self> {
        // Create a channel for sending requests to the writer thread
        let (request_tx, mut request_rx) = mpsc::channel::<WriterRequest>(1024);

        // Build a shared Mvcc instance (Arc) upfront so reads can proceed concurrently
        let p = path.as_ref();
        let file_path = if p.is_dir() {
            p.join(DEFAULT_DB_FILENAME)
        } else {
            p.to_path_buf()
        };
        let mvcc = Arc::new(Mvcc::new(&file_path, DEFAULT_PAGE_SIZE, false)?);

        // Initialize the head watch channel with the current head.
        let init_head = {
            let (_, header) = mvcc.get_latest_header()?;
            let last = header.next_position.0.saturating_sub(1);
            if last == 0 { None } else { Some(last) }
        };
        let (head_tx, _head_rx) = watch::channel::<Option<u64>>(init_head);

        // Spawn a thread for processing writer requests.
        let mvcc_for_writer = mvcc.clone();
        let head_tx_writer = head_tx.clone();
        thread::spawn(move || {
            let db = UmaDB::from_arc(mvcc_for_writer);

            // Create a runtime for processing writer requests.
            let rt = Runtime::new().unwrap();

            // Process writer requests.
            rt.block_on(async {
                while let Some(request) = request_rx.recv().await {
                    match request {
                        WriterRequest::Append {
                            events,
                            condition,
                            tracking_info,
                            response_tx,
                        } => {
                            // Batch processing: drain any immediately available requests
                            // let mut items: Vec<(Vec<DCBEvent>, Option<DCBAppendCondition>)> =
                            //     Vec::new();

                            let mut total_events = 0;
                            total_events += events.len();
                            // items.push((events, condition));

                            let mvcc = &db.mvcc;
                            let mut writer = match mvcc.writer() {
                                Ok(writer) => writer,
                                Err(err) => {
                                    let _ = response_tx.send(Err(err));
                                    continue;
                                }
                            };

                            let mut responders: Vec<oneshot::Sender<DCBResult<u64>>> = Vec::new();
                            let mut results: Vec<DCBResult<u64>> = Vec::new();

                            responders.push(response_tx);
                            let mut result = UmaDB::process_append_request(
                                events,
                                condition,
                                tracking_info,
                                mvcc,
                                &mut writer,
                            );
                            results.push(result);

                            // Drain the channel for more pending writer requests without awaiting.
                            // Important: do not drop a popped request when hitting the batch limit.
                            // We stop draining BEFORE attempting to recv if we've reached the limit.
                            loop {
                                if total_events >= APPEND_BATCH_MAX_EVENTS {
                                    break;
                                }
                                match request_rx.try_recv() {
                                    Ok(WriterRequest::Append {
                                        events,
                                        condition,
                                        tracking_info,
                                        response_tx,
                                    }) => {
                                        let ev_len = events.len();
                                        responders.push(response_tx);
                                        result = UmaDB::process_append_request(
                                            events,
                                            condition,
                                            tracking_info,
                                            mvcc,
                                            &mut writer,
                                        );
                                        results.push(result);
                                        total_events += ev_len;
                                    }
                                    Ok(WriterRequest::Shutdown) => {
                                        // Push back the shutdown signal by breaking and letting
                                        // outer loop handle after batch. We'll process the
                                        // current batch first, then break the outer loop on
                                        // the next iteration when the channel is empty.
                                        break;
                                    }
                                    Err(mpsc::error::TryRecvError::Empty) => {
                                        break;
                                    }
                                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                                }
                            }
                            // println!("Total events: {total_events}");

                            // Single commit at the end of the batch
                            let batch_result = match mvcc.commit(&mut writer) {
                                Ok(_) => Ok(results),
                                Err(err) => Err(err),
                            };

                            match batch_result {
                                Ok(results) => {
                                    // Send individual results back to requesters
                                    // Also compute the new head as the maximum successful last position in this batch
                                    let mut max_ok: Option<u64> = None;
                                    for (res, tx) in results.into_iter().zip(responders.into_iter())
                                    {
                                        if let Ok(v) = &res {
                                            max_ok = Some(max_ok.map_or(*v, |m| m.max(*v)));
                                        }
                                        let _ = tx.send(res);
                                    }
                                    // After a successful batch commit, publish the updated head.
                                    if let Some(h) = max_ok {
                                        let _ = head_tx_writer.send(Some(h));
                                    }
                                }
                                Err(e) => {
                                    // If the batch failed as a whole (e.g., commit failed), propagate the SAME error to all responders.
                                    // DCBError is not Clone (contains io::Error), so reconstruct a best-effort copy by using its Display text
                                    // for Io and cloning data for other variants.
                                    let total = responders.len();
                                    let mut iter = responders.into_iter();
                                    for _ in 0..total {
                                        if let Some(tx) = iter.next() {
                                            let _ = tx.send(Err(clone_dcb_error(&e)));
                                        }
                                    }
                                }
                            }
                        }
                        WriterRequest::Shutdown => {
                            break;
                        }
                    }
                }
            });
        });

        Ok(Self {
            mvcc,
            head_watch_tx: head_tx,
            writer_request_tx: request_tx,
        })
    }

    async fn read(
        &self,
        query: Option<DCBQuery>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
    ) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        let reader = self.mvcc.reader()?;
        let last_committed_position = reader.next_position.0.saturating_sub(1);

        let q = query.unwrap_or(DCBQuery { items: vec![] });
        let start_position = start.map(Position);

        let events = read_conditional(
            &self.mvcc,
            &std::collections::HashMap::new(),
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            q,
            start_position,
            backwards,
            limit,
            false,
        )
        .map_err(|e| DCBError::Corruption(format!("{e}")))?;

        let head = if limit.is_none() {
            if last_committed_position == 0 {
                None
            } else {
                Some(last_committed_position)
            }
        } else {
            events.last().map(|e| e.position)
        };

        Ok((events, head))
    }

    async fn head(&self) -> DCBResult<Option<u64>> {
        let (_, header) = self
            .mvcc
            .get_latest_header()
            .map_err(|e| DCBError::Corruption(format!("{e}")))?;
        let last = header.next_position.0.saturating_sub(1);
        if last == 0 { Ok(None) } else { Ok(Some(last)) }
    }

    async fn get_tracking_info(&self, source: String) -> DCBResult<Option<u64>> {
        let db = UmaDB::from_arc(self.mvcc.clone());
        db.get_tracking_info(&source)
    }

    pub async fn append(
        &self,
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
        tracking_info: Option<TrackingInfo>,
    ) -> DCBResult<u64> {
        // Concurrent pre-check of the given condition using a reader in a blocking thread.
        let pre_append_decision = if let Some(mut given_condition) = condition {
            let reader = self.mvcc.reader()?;
            let current_head = {
                let last = reader.next_position.0.saturating_sub(1);
                if last == 0 { None } else { Some(last) }
            };

            // Perform conditional read on the snapshot (limit 1) starting after the given position
            let from = given_condition.after.map(|after| Position(after + 1));
            let empty_dirty = std::collections::HashMap::new();
            let found = read_conditional(
                &self.mvcc,
                &empty_dirty,
                reader.events_tree_root_id,
                reader.tags_tree_root_id,
                given_condition.fail_if_events_match.clone(),
                from,
                false,
                Some(1),
                false,
            )?;

            if let Some(matched) = found.first() {
                // Found one event — consider if the request is idempotent...
                match is_request_idempotent(
                    &self.mvcc,
                    &empty_dirty,
                    reader.events_tree_root_id,
                    reader.tags_tree_root_id,
                    &events,
                    given_condition.fail_if_events_match.clone(),
                    from,
                ) {
                    Ok(Some(last_recorded_position)) => {
                        // Request is idempotent; skip actual append
                        PreAppendDecision::AlreadyAppended(last_recorded_position)
                    }
                    Ok(None) => {
                        // Integrity violation
                        let msg = format!(
                            "condition: {:?} matched: {:?}",
                            given_condition.clone(),
                            matched,
                        );
                        return Err(DCBError::IntegrityError(msg));
                    }
                    Err(err) => {
                        // Propagate underlying read error
                        return Err(err);
                    }
                }
            } else {
                // No match found: we can advance 'after' to the current head observed by this reader
                let new_after = std::cmp::max(
                    given_condition.after.unwrap_or(0),
                    current_head.unwrap_or(0),
                );
                given_condition.after = Some(new_after);

                PreAppendDecision::UseCondition(Some(given_condition))
            }
        } else {
            // No condition provided at all
            PreAppendDecision::UseCondition(None)
        };

        // Handle the pre-check decision
        match pre_append_decision {
            PreAppendDecision::AlreadyAppended(last_found_position) => {
                // ✅ Request was idempotent — just return the existing position.
                Ok(last_found_position)
            }
            PreAppendDecision::UseCondition(adjusted_condition) => {
                // ✅ Proceed with append operation on the writer thread.
                let (response_tx, response_rx) = oneshot::channel();

                self.writer_request_tx
                    .send(WriterRequest::Append {
                        events,
                        condition: adjusted_condition,
                        tracking_info: tracking_info,
                        response_tx,
                    })
                    .await
                    .map_err(|_| {
                        DCBError::Io(std::io::Error::other(
                            "Failed to send append request to EventStore thread",
                        ))
                    })?;

                response_rx.await.map_err(|_| {
                    DCBError::Io(std::io::Error::other(
                        "Failed to receive append response from EventStore thread",
                    ))
                })?
            }
        }
    }

    fn watch_head(&self) -> watch::Receiver<Option<u64>> {
        self.head_watch_tx.subscribe()
    }

    #[allow(dead_code)]
    async fn shutdown(&self) {
        let _ = self.writer_request_tx.send(WriterRequest::Shutdown).await;
    }
}

fn clone_dcb_error(src: &DCBError) -> DCBError {
    match src {
        DCBError::AuthenticationError(err) => DCBError::AuthenticationError(err.to_string()),
        DCBError::InitializationError(err) => DCBError::InitializationError(err.to_string()),
        DCBError::Io(err) => DCBError::Io(std::io::Error::other(err.to_string())),
        DCBError::IntegrityError(s) => DCBError::IntegrityError(s.clone()),
        DCBError::Corruption(s) => DCBError::Corruption(s.clone()),
        DCBError::PageNotFound(id) => DCBError::PageNotFound(*id),
        DCBError::DirtyPageNotFound(id) => DCBError::DirtyPageNotFound(*id),
        DCBError::RootIDMismatch(old_id, new_id) => DCBError::RootIDMismatch(*old_id, *new_id),
        DCBError::DatabaseCorrupted(s) => DCBError::DatabaseCorrupted(s.clone()),
        DCBError::InternalError(s) => DCBError::InternalError(s.clone()),
        DCBError::SerializationError(s) => DCBError::SerializationError(s.clone()),
        DCBError::DeserializationError(s) => DCBError::DeserializationError(s.clone()),
        DCBError::PageAlreadyFreed(id) => DCBError::PageAlreadyFreed(*id),
        DCBError::PageAlreadyDirty(id) => DCBError::PageAlreadyDirty(*id),
        DCBError::TransportError(err) => DCBError::TransportError(err.clone()),
        DCBError::CancelledByUser() => DCBError::CancelledByUser(),
    }
}

// Clone implementation for EventStoreHandle
impl Clone for RequestHandler {
    fn clone(&self) -> Self {
        Self {
            mvcc: self.mvcc.clone(),
            head_watch_tx: self.head_watch_tx.clone(),
            writer_request_tx: self.writer_request_tx.clone(),
        }
    }
}

#[derive(Debug)]
enum PreAppendDecision {
    /// Proceed with this (possibly adjusted) condition
    UseCondition(Option<DCBAppendCondition>),
    /// Skip append operation because the request was idempotent; return last recorded position
    AlreadyAppended(u64),
}
