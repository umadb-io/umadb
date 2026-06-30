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
    UmaDb, clone_dcb_error, is_integrity_error, is_invalid_argument_error, is_request_idempotent,
    read_conditional, shadow_for_batch_abort,
};
pub use umadb_core::mvcc::{
    DEFAULT_DB_FILENAME, DEFAULT_PAGE_SIZE, Mvcc, ReadMethod, StorageOptions,
};
use umadb_dcb::{
    DcbAppendCondition, DcbError, DcbEvent, DcbQuery, DcbResult, DcbSequencedEvent, TrackingInfo,
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

// Server options
#[derive(Clone, Debug)]
pub struct ServerOptions {
    pub listen_addr: String,
    pub tls: Option<ServerTlsOptions>,
    pub api_key: Option<String>,
    pub storage: StorageOptions,
}

// Server TLS configuration
#[derive(Clone, Debug)]
pub struct ServerTlsOptions {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

impl ServerTlsOptions {
    pub fn from_path_strings(
        cert_path: Option<String>,
        key_path: Option<String>,
    ) -> Result<Option<Self>, Box<dyn std::error::Error>> {
        match (cert_path, key_path) {
            (Some(cert_path), Some(key_path)) => {
                let cert_pem = read_file(cert_path.clone(), "TLS certificate")?;
                let key_pem = read_file(key_path.clone(), "TLS key")?;
                Ok(Some(ServerTlsOptions { cert_pem, key_pem }))
            }
            (None, None) => Ok(None),
            _ => Err("both cert_path and key_path must be provided for TLS".into()).into(),
        }
    }
}

fn read_file(path: String, purpose: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    Ok(
        fs::read(path.clone()).map_err(|e| -> Box<dyn std::error::Error> {
            format!("failed to open {purpose} file '{path}': {}", e).into()
        })?,
    )
}

/// A guard that sends a signal through a oneshot channel when dropped.
struct CancellationGuard(Option<oneshot::Sender<()>>);

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

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
                eprintln!("failed to construct valid URI for path: {}", path);
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
pub async fn start_server<P: AsRef<Path>>(
    db_path: P,
    addr: &str,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let options = ServerOptions {
        listen_addr: addr.to_string(),
        tls: None,
        api_key: None,
        storage: StorageOptions::default().db_path(db_path.as_ref()),
    };
    start_server_with_options(options, shutdown_rx).await
}

pub async fn start_server_with_options(
    options: ServerOptions,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = options.listen_addr.parse()?;
    // ---- Bind incoming manually like tonic ----
    let incoming = match TcpIncoming::bind(addr) {
        Ok(incoming) => incoming,
        Err(err) => {
            return Err(Box::new(DcbError::InitializationError(format!(
                "failed to bind to address {}: {}",
                addr, err
            ))));
        }
    }
    .with_nodelay(Some(true))
    .with_keepalive(Some(std::time::Duration::from_secs(60)));

    // Create a shutdown broadcast channel for terminating ongoing subscriptions
    let (srv_shutdown_tx, srv_shutdown_rx) = watch::channel(false);
    let dcb_server = match DcbServer::new(srv_shutdown_rx, options.api_key.clone(), options.storage)
    {
        Ok(server) => server,
        Err(err) => {
            return Err(Box::new(err));
        }
    };

    println!(
        "UmaDB has {:?} events",
        dcb_server
            .request_handler
            .head()
            .unwrap_or(Some(0))
            .unwrap_or(0)
    );
    let tls_mode_display_str = if options.tls.is_some() {
        "with TLS"
    } else {
        "without TLS"
    };

    let api_key_display_str = if options.api_key.is_some() {
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
    let mut builder = build_server_builder_with_options(options.tls)
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
pub struct DcbServer {
    pub(crate) request_handler: RequestHandler,
    shutdown_watch_rx: watch::Receiver<bool>,
    api_key: Option<String>,
}

impl DcbServer {
    pub fn new(
        shutdown_rx: watch::Receiver<bool>,
        api_key: Option<String>,
        storage_options: StorageOptions,
    ) -> DcbResult<Self> {
        let command_handler = RequestHandler::new(storage_options)?;
        Ok(Self {
            request_handler: command_handler,
            shutdown_watch_rx: shutdown_rx,
            api_key,
        })
    }

    pub fn into_service(self) -> umadb_proto::v1::dcb_server::DcbServer<Self> {
        umadb_proto::v1::dcb_server::DcbServer::new(self)
    }

    fn enforce_api_key(&self, metadata: &tonic::metadata::MetadataMap) -> Result<(), Status> {
        if let Some(expected) = &self.api_key {
            let auth = metadata.get("authorization");
            let expected_val = format!("Bearer {}", expected);
            let ok = auth
                .and_then(|m| m.to_str().ok())
                .map(|s| s == expected_val)
                .unwrap_or(false);
            if !ok {
                return Err(status_from_dcb_error(DcbError::AuthenticationError(
                    "missing or invalid API key".to_string(),
                )));
            }
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl umadb_proto::v1::dcb_server::Dcb for DcbServer {
    type ReadStream =
        Pin<Box<dyn Stream<Item = Result<umadb_proto::v1::ReadResponse, Status>> + Send + 'static>>;
    type SubscribeStream = Pin<
        Box<dyn Stream<Item = Result<umadb_proto::v1::SubscribeResponse, Status>> + Send + 'static>,
    >;

    async fn read(
        &self,
        request: Request<umadb_proto::v1::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        // Enforce API key if configured
        self.enforce_api_key(request.metadata())?;
        let read_request = request.into_inner();

        // Convert protobuf query to DCB types
        let query: Option<DcbQuery> = read_request.query.map(|q| q.into());
        let start = read_request.start;
        let backwards = read_request.backwards.unwrap_or(false);
        let limit = read_request.limit;
        // Cap requested batch size.
        let batch_size = read_request
            .batch_size
            .unwrap_or(READ_RESPONSE_BATCH_SIZE_DEFAULT)
            .clamp(1, READ_RESPONSE_BATCH_SIZE_MAX);

        // Create a channel for streaming responses (deeper buffer to reduce backpressure under concurrency)
        let (tx, rx) = mpsc::channel(2048);
        // Clone the request handler.
        let request_handler = self.request_handler.clone();
        // Clone the shutdown watch receiver.
        let mut shutdown_watch_rx = self.shutdown_watch_rx.clone();

        let cancel_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancel_signal_for_task = cancel_signal.clone();

        // Spawn a task to handle the read operation and stream multiple batches
        tokio::spawn(async move {
            // Ensure we can reuse the same query across batches
            let query_clone = query;
            let mut next_start = start;
            let mut sent_any = false;
            let mut remaining_limit = limit.unwrap_or(u32::MAX);
            let mut captured_db_head: Option<u64> = None;
            let mut have_captured_db_head: bool = false;
            loop {
                // TODO: Can remove this check when we sure that cancel_signal_for_task
                //  is fully respected by all paths in spawn_blocking(handler.read).
                // Exit if the client has gone away or the server is shutting down.
                if tx.is_closed() || *shutdown_watch_rx.borrow() {
                    cancel_signal_for_task.store(true, std::sync::atomic::Ordering::SeqCst);
                    break;
                }
                // Determine per-iteration limit.
                let read_limit = remaining_limit.min(batch_size);
                // If subscription and remaining exhausted (limit reached), terminate
                if limit.is_some() && remaining_limit == 0 {
                    break;
                }
                let handler = request_handler.clone();
                let query_val = query_clone.clone();
                let limit_val = Some(read_limit);
                let cancel_for_blocking = cancel_signal_for_task.clone();
                let mut blocking_handle = tokio::task::spawn_blocking(move || {
                    handler.read(
                        query_val,
                        next_start,
                        backwards,
                        limit_val,
                        Some(cancel_for_blocking),
                    )
                });

                let res = tokio::select! {
                    res = &mut blocking_handle => {
                        res.map_err(|e| DcbError::InternalError(e.to_string())).and_then(|res| res)
                    }
                    _ = tx.closed() => {
                        cancel_signal_for_task.store(true, std::sync::atomic::Ordering::SeqCst);
                        // Await the task to ensure it finishes and doesn't leak
                        let _ = blocking_handle.await;
                        break;
                    }
                    _ = shutdown_watch_rx.changed() => {
                        cancel_signal_for_task.store(true, std::sync::atomic::Ordering::SeqCst);
                        let _ = blocking_handle.await;
                        break;
                    }
                };

                match res {
                    Ok((dcb_sequenced_events, db_head)) => {
                        // Capture the db head from the first read.
                        if !have_captured_db_head {
                            captured_db_head = db_head;
                            have_captured_db_head = true;
                        }

                        // Capture the original length before consuming events
                        let original_len = dcb_sequenced_events.len();
                        let read_less_than_read_limit = (original_len as u32) < read_limit;

                        // Map events to protobuf messages, discarding if position too large.
                        let sequenced_event_protos: Vec<umadb_proto::v1::SequencedEvent> =
                            dcb_sequenced_events
                                .into_iter()
                                .filter(|e| {
                                    if let Some(h) = captured_db_head {
                                        e.position <= h
                                    } else {
                                        true
                                    }
                                })
                                .map(umadb_proto::v1::SequencedEvent::from)
                                .collect();

                        // Check if we filtered out any events
                        let reached_captured_head = captured_db_head.is_some()
                            && sequenced_event_protos.len() < original_len;

                        if sequenced_event_protos.is_empty() {
                            if !sent_any {
                                // At least send an empty response to communicate head.
                                let response = umadb_proto::v1::ReadResponse {
                                    events: vec![],
                                    head: if limit.is_some() {
                                        None
                                    } else {
                                        captured_db_head
                                    },
                                };
                                let _ = tx.send(Ok(response)).await;
                            }
                            // Stop looping, because there's nothing else to read.
                            break;
                        }

                        // Capture values needed after sequenced_event_protos is moved.
                        let sent_count = sequenced_event_protos.len() as u32;

                        let last_event_position = sequenced_event_protos.last().map(|e| e.position);

                        let response = umadb_proto::v1::ReadResponse {
                            events: sequenced_event_protos,
                            head: if limit.is_some() {
                                last_event_position
                            } else {
                                captured_db_head
                            },
                        };

                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                        sent_any = true;

                        // Advance the cursor (use a new reader on the next loop iteration)
                        next_start = last_event_position.map(|p| {
                            if backwards {
                                p.saturating_sub(1)
                            } else {
                                p.saturating_add(1)
                            }
                        });

                        // Stop streaming further if we read less than limit or
                        // reached the captured head boundary.
                        if read_less_than_read_limit || reached_captured_head {
                            break;
                        }

                        // Decrease the remaining overall limit if any, and stop if reached
                        if limit.is_some() {
                            remaining_limit = remaining_limit.saturating_sub(sent_count);
                            if remaining_limit == 0 {
                                break;
                            }
                        }

                        // Yield to let other tasks progress under high concurrency
                        tokio::task::yield_now().await;
                    }
                    Err(e) => {
                        if matches!(e, DcbError::CancelledByUser()) {
                            // Silently stop if cancelled by user
                        } else {
                            let _ = tx.send(Err(status_from_dcb_error(e))).await;
                        }
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

    async fn subscribe(
        &self,
        request: Request<umadb_proto::v1::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        // Enforce API key if configured
        self.enforce_api_key(request.metadata())?;
        let subscribe_request = request.into_inner();

        // Convert protobuf query to DCB types
        let query: Option<DcbQuery> = subscribe_request.query.map(|q| q.into());
        let after = subscribe_request.after;
        // Cap requested batch size.
        let batch_size = subscribe_request
            .batch_size
            .unwrap_or(READ_RESPONSE_BATCH_SIZE_DEFAULT)
            .clamp(1, READ_RESPONSE_BATCH_SIZE_MAX);

        // Create a channel for streaming responses
        let (tx, rx) = mpsc::channel(2048);
        // Clone the request handler.
        let request_handler = self.request_handler.clone();
        // Clone the shutdown watch receiver.
        let mut shutdown_watch_rx = self.shutdown_watch_rx.clone();

        let cancel_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancel_signal_for_task = cancel_signal.clone();

        // Spawn a task to handle the subscribe operation and stream multiple batches
        tokio::spawn(async move {
            // Ensure we can reuse the same query across batches
            let query_clone = query;
            // Todo: End the subscription if after is Some(u64:MAX).
            let mut next_after = after.map(|a| a.saturating_add(1));
            // Create a watch receiver for head updates
            let mut head_rx = request_handler.watch_head();

            loop {
                // TODO: Can remove this check when we sure that cancel_signal_for_task
                //  is fully respected by all paths in spawn_blocking(handler.read).
                // Exit if the client has gone away or the server is shutting down.
                if tx.is_closed() || *shutdown_watch_rx.borrow() {
                    cancel_signal_for_task.store(true, std::sync::atomic::Ordering::SeqCst);
                    break;
                }

                let handler = request_handler.clone();
                let query_val = query_clone.clone();
                let batch_size_val = Some(batch_size);
                let cancel_for_blocking = cancel_signal_for_task.clone();
                let mut blocking_handle = tokio::task::spawn_blocking(move || {
                    handler.read(
                        query_val,
                        next_after,
                        false,
                        batch_size_val,
                        Some(cancel_for_blocking),
                    )
                });

                let res = tokio::select! {
                    res = &mut blocking_handle => {
                        res.map_err(|e| DcbError::InternalError(e.to_string())).and_then(|res| res)
                    }
                    _ = tx.closed() => {
                        cancel_signal_for_task.store(true, std::sync::atomic::Ordering::SeqCst);
                        let _ = blocking_handle.await;
                        break;
                    }
                    _ = shutdown_watch_rx.changed() => {
                        cancel_signal_for_task.store(true, std::sync::atomic::Ordering::SeqCst);
                        let _ = blocking_handle.await;
                        break;
                    }
                };

                match res {
                    Ok((dcb_sequenced_events, _unused_db_head)) => {
                        // Map events to protobuf type
                        let sequenced_event_protos: Vec<umadb_proto::v1::SequencedEvent> =
                            dcb_sequenced_events
                                .into_iter()
                                .map(umadb_proto::v1::SequencedEvent::from)
                                .collect();

                        if sequenced_event_protos.is_empty() {
                            // For subscriptions, wait for new events instead of terminating
                            tokio::select! {
                                _ = head_rx.changed() => {},
                                _ = shutdown_watch_rx.changed() => {},
                                _ = tx.closed() => {},
                            }
                            continue;
                        }

                        let last_event_position = sequenced_event_protos.last().map(|e| e.position);

                        let response = umadb_proto::v1::SubscribeResponse {
                            events: sequenced_event_protos,
                        };

                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }

                        // Advance the cursor (use a new reader on the next loop iteration)
                        // Todo: End the subscription if last_event_position is Some(u64:MAX).
                        next_after = last_event_position.map(|p| p.saturating_add(1));

                        // Yield to let other tasks progress under high concurrency
                        tokio::task::yield_now().await;
                    }
                    Err(e) => {
                        if matches!(e, DcbError::CancelledByUser()) {
                            // Silently stop if cancelled by user
                        } else {
                            let _ = tx.send(Err(status_from_dcb_error(e))).await;
                        }
                        break;
                    }
                }
            }
        });

        // Return the receiver as a stream
        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::SubscribeStream
        ))
    }

    async fn append(
        &self,
        request: Request<umadb_proto::v1::AppendRequest>,
    ) -> Result<Response<umadb_proto::v1::AppendResponse>, Status> {
        // Enforce API key if configured
        self.enforce_api_key(request.metadata())?;
        let req = request.into_inner();

        // Convert protobuf types to API types
        let events: Vec<DcbEvent> = match req.events.into_iter().map(|e| e.try_into()).collect() {
            Ok(events) => events,
            Err(e) => {
                return Err(status_from_dcb_error(e));
            }
        };
        let condition = req.condition.map(|c| c.into());

        let cancel_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancel_signal_for_task = cancel_signal.clone();

        // Create a way to watch for the request being cancelled/dropped
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let _guard = CancellationGuard(Some(cancel_tx));

        // Spawn a monitoring task that survives the gRPC future being dropped
        let cancel_signal_for_monitoring = cancel_signal.clone();
        tokio::spawn(async move {
            // This resolves when _guard is dropped (client disconnects)
            // or when the gRPC method finishes normally.
            let _ = cancel_rx.await;
            cancel_signal_for_monitoring.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        // Call the event store append method
        let res = self
            .request_handler
            .append(
                events,
                condition,
                req.tracking_info.map(|t| TrackingInfo {
                    source: t.source,
                    position: t.position,
                }),
                Some(cancel_signal_for_task.clone()),
            )
            .await;

        match res {
            Ok(position) => Ok(Response::new(umadb_proto::v1::AppendResponse { position })),
            Err(e) => Err(status_from_dcb_error(e)),
        }
    }

    async fn head(
        &self,
        request: Request<umadb_proto::v1::HeadRequest>,
    ) -> Result<Response<umadb_proto::v1::HeadResponse>, Status> {
        // Enforce API key if configured
        self.enforce_api_key(request.metadata())?;
        // Call the event store head method
        match self.request_handler.head() {
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
        self.enforce_api_key(request.metadata())?;
        let req = request.into_inner();
        match self.request_handler.get_tracking_info(req.source) {
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
        events: Vec<DcbEvent>,
        condition: Option<DcbAppendCondition>,
        tracking_info: Option<TrackingInfo>,
        response_tx: oneshot::Sender<DcbResult<u64>>,
        cancel: Option<Arc<std::sync::atomic::AtomicBool>>,
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
    fn new(storage_options: StorageOptions) -> DcbResult<Self> {
        // Create a channel for sending requests to the writer thread
        let (request_tx, mut request_rx) = mpsc::channel::<WriterRequest>(1024);

        // Build a shared Mvcc instance (Arc) upfront so reads can proceed concurrently
        let mvcc = Arc::new(Mvcc::new(false, storage_options)?);

        // Initialize the head watch channel with the current head.
        let init_head = {
            let header_page = mvcc.get_latest_header_page()?;
            let header = header_page.as_header_node()?;
            let last = header.next_position.0.saturating_sub(1);
            if last == 0 { None } else { Some(last) }
        };
        let (head_tx, _head_rx) = watch::channel::<Option<u64>>(init_head);

        // Spawn a thread for processing writer requests.
        let mvcc_for_writer = mvcc.clone();
        let head_tx_writer = head_tx.clone();
        thread::spawn(move || {
            let db = UmaDb::from_arc(mvcc_for_writer);

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
                            cancel,
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

                            let mut responders: Vec<oneshot::Sender<DcbResult<u64>>> = Vec::new();
                            let mut results: Vec<DcbResult<u64>> = Vec::new();

                            // Track abort state for non-integrity error within the batch
                            let mut abort_idx: Option<usize> = None;
                            let mut abort_err: Option<DcbError> = None;

                            responders.push(response_tx);
                            let result = UmaDb::process_append_request(
                                events,
                                condition,
                                tracking_info,
                                mvcc,
                                &mut writer,
                                cancel,
                            );
                            // Record result and possibly mark abort
                            match &result {
                                Ok(_) => results.push(result),
                                Err(e) if is_integrity_error(e) => {
                                    results.push(Err(clone_dcb_error(e)))
                                }
                                Err(e) if is_invalid_argument_error(e) => {
                                    results.push(Err(clone_dcb_error(e)))
                                }
                                Err(e) => {
                                    abort_idx = Some(0);
                                    abort_err = Some(clone_dcb_error(e));
                                    results.push(Err(clone_dcb_error(e)));
                                }
                            }

                            // Drain the channel for more pending writer requests without awaiting.
                            // Important: do not drop a popped request when hitting the batch limit.
                            // We stop draining BEFORE attempting to recv if we've reached the limit.
                            loop {
                                if total_events >= APPEND_BATCH_MAX_EVENTS {
                                    break;
                                }
                                // Stop draining if we've already decided to abort
                                if abort_idx.is_some() {
                                    break;
                                }
                                match request_rx.try_recv() {
                                    Ok(WriterRequest::Append {
                                        events,
                                        condition,
                                        tracking_info,
                                        response_tx,
                                        cancel,
                                    }) => {
                                        let ev_len = events.len();
                                        let idx_in_batch = responders.len();
                                        responders.push(response_tx);
                                        let res_next = UmaDb::process_append_request(
                                            events,
                                            condition,
                                            tracking_info,
                                            mvcc,
                                            &mut writer,
                                            cancel,
                                        );
                                        match &res_next {
                                            Ok(_) => results.push(res_next),
                                            Err(e) if is_integrity_error(e) => {
                                                results.push(Err(clone_dcb_error(e)))
                                            }
                                            Err(e) if is_invalid_argument_error(e) => {
                                                results.push(Err(clone_dcb_error(e)))
                                            }
                                            Err(e) => {
                                                abort_idx = Some(idx_in_batch);
                                                abort_err = Some(clone_dcb_error(e));
                                                results.push(Err(clone_dcb_error(e)));
                                                // Do not accumulate more into the batch
                                            }
                                        }
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

                            if let (Some(failed_at), Some(orig_err)) = (abort_idx, abort_err) {
                                // Abort batch: skip commit; respond to all items in this batch
                                let shadow = shadow_for_batch_abort(&orig_err);
                                for (i, tx) in responders.into_iter().enumerate() {
                                    if i == failed_at {
                                        let _ = tx.send(Err(clone_dcb_error(&orig_err)));
                                    } else {
                                        let _ = tx.send(Err(clone_dcb_error(&shadow)));
                                    }
                                }
                                // Do not update head, since nothing was committed
                                continue;
                            }

                            // Single commit at the end of the batch
                            let batch_result = match mvcc.commit(&mut writer) {
                                Ok(_) => Ok(results),
                                Err(err) => Err(err),
                            };

                            match batch_result {
                                Ok(results) => {
                                    // Send individual results back to requesters
                                    for (res, tx) in results.into_iter().zip(responders.into_iter())
                                    {
                                        let _ = tx.send(res);
                                    }
                                    // After a successful batch commit, publish the updated head from writer.next_position.
                                    let last_committed = writer.next_position.0.saturating_sub(1);
                                    let new_head = if last_committed == 0 {
                                        None
                                    } else {
                                        Some(last_committed)
                                    };
                                    let _ = head_tx_writer.send(new_head);
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

    fn read(
        &self,
        query: Option<DcbQuery>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
        cancel: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> DcbResult<(Vec<DcbSequencedEvent>, Option<u64>)> {
        let reader = self.mvcc.reader()?;
        let db_head = if reader.next_position > Position(1) {
            Some(reader.next_position.0.saturating_sub(1))
        } else {
            None
        };

        let q = query.unwrap_or(DcbQuery { items: vec![] });
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
            cancel,
        )
        .map_err(|e| match e {
            DcbError::CancelledByUser() => DcbError::CancelledByUser(),
            _ => DcbError::Corruption(format!("{e}")),
        })?;

        Ok((events, db_head))
    }

    fn head(&self) -> DcbResult<Option<u64>> {
        let header_page = self
            .mvcc
            .get_latest_header_page()
            .map_err(|e| DcbError::Corruption(format!("{e}")))?;
        let header = header_page
            .as_header_node()
            .map_err(|e| DcbError::Corruption(format!("{e}")))?;
        let last = header.next_position.0.saturating_sub(1);
        if last == 0 { Ok(None) } else { Ok(Some(last)) }
    }

    fn get_tracking_info(&self, source: String) -> DcbResult<Option<u64>> {
        let db = UmaDb::from_arc(self.mvcc.clone());
        db.get_tracking_info(&source)
    }

    pub async fn append(
        &self,
        events: Vec<DcbEvent>,
        condition: Option<DcbAppendCondition>,
        tracking_info: Option<TrackingInfo>,
        cancel: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> DcbResult<u64> {
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
                None,
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
                    cancel.clone(),
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
                        return Err(DcbError::IntegrityError(msg));
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
                // Request was idempotent — just return the existing position.
                Ok(last_found_position)
            }
            PreAppendDecision::UseCondition(adjusted_condition) => {
                // Proceed with append operation on the writer thread.
                let (response_tx, response_rx) = oneshot::channel();

                self.writer_request_tx
                    .send(WriterRequest::Append {
                        events,
                        condition: adjusted_condition,
                        tracking_info,
                        response_tx,
                        cancel,
                    })
                    .await
                    .map_err(|_| {
                        DcbError::Io(std::io::Error::other(
                            "failed to send append request to EventStore thread",
                        ))
                    })?;

                response_rx.await.map_err(|_| {
                    DcbError::Io(std::io::Error::other(
                        "failed to receive append response from EventStore thread",
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
    UseCondition(Option<DcbAppendCondition>),
    /// Skip append operation because the request was idempotent; return last recorded position
    AlreadyAppended(u64),
}
