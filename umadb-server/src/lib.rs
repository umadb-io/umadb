use futures::Stream;
use std::fs;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, watch, Semaphore};

use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Identity, ServerTlsConfig};
use tonic::{transport::Server, Request, Response, Status};
pub use umadb_core::mvcc::{
    Mvcc, ReadMethod, StorageOptions, DEFAULT_DB_FILENAME, DEFAULT_PAGE_SIZE,
};
use umadb_dcb::{
    DcbError, DcbEvent, DcbQuery, DcbResult, TrackingInfo,
};

use tonic::codegen::http;
use tonic::transport::server::TcpIncoming;
use std::convert::Infallible;
use std::future::Future;
use std::task::{Context, Poll};
use tonic::server::NamedService;
use handler::UmaDbServerRequestHandler;
use umadb_proto::status_from_dcb_error;

mod handler;

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

pub fn server_uptime() -> std::time::Duration {
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

/// Raise the process's open-file limit (`RLIMIT_NOFILE`) soft cap toward the hard
/// cap, once per process.
///
/// Each client connection is a socket = one file descriptor. With the common
/// default soft limit of 1024, ~1024 concurrent clients exhaust the descriptor
/// table (after stdio, the listener, the DB file, epoll, etc.), which surfaces as
/// connection/stream failures across every workload at ~1024 clients while lower
/// concurrencies are fine. Raising the soft limit to the hard limit removes that
/// wall without requiring operators to remember `ulimit -n`.
pub fn raise_open_file_limit() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(raise_open_file_limit_inner);
}

#[cfg(unix)]
fn raise_open_file_limit_inner() {
    // SAFETY: get/setrlimit are simple syscalls; we pass a valid, fully-initialized
    // `rlimit` and only read the returned values.
    unsafe {
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) != 0 {
            eprintln!(
                "UmaDB: could not read open-file limit: {}",
                std::io::Error::last_os_error()
            );
            return;
        }
        let previous = lim.rlim_cur;
        if lim.rlim_max != libc::RLIM_INFINITY && previous >= lim.rlim_max {
            println!(
                "UmaDB open-file limit: soft {previous} already at hard limit {}",
                lim.rlim_max
            );
            return;
        }
        // Prefer the hard limit. When the hard limit is "unlimited" (common on
        // macOS) some kernels reject an unlimited soft value, so try large concrete
        // targets and fall back if the kernel rejects them.
        let candidates: &[libc::rlim_t] = if lim.rlim_max == libc::RLIM_INFINITY {
            &[1_048_576, 65_536]
        } else {
            &[lim.rlim_max]
        };
        let mut attempted = false;
        for &target in candidates.iter() {
            if target <= previous {
                continue;
            }
            attempted = true;
            let new = libc::rlimit {
                rlim_cur: target,
                rlim_max: lim.rlim_max,
            };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &new) == 0 {
                println!(
                    "UmaDB raised open-file limit: soft {previous} -> {target}"
                );
                return;
            }
        }
        if !attempted {
            // Soft limit is already at least as high as anything we'd set.
            println!(
                "UmaDB open-file limit: soft {previous} is already sufficient"
            );
            return;
        }
        eprintln!(
            "UmaDB: could not raise open-file limit from soft {previous}: {}. \
             Consider raising it manually (e.g. `ulimit -n 262144`).",
            std::io::Error::last_os_error()
        );
    }
}

#[cfg(not(unix))]
fn raise_open_file_limit_inner() {}

pub async fn start_server_with_options(
    options: ServerOptions,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Ensure we can accept many concurrent client connections (one fd each).
    raise_open_file_limit();

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

    // Construct the actual gRPC server implementation.
    let dcb_server = match UmaDbServer::new(srv_shutdown_rx, options.api_key.clone(), options.storage)
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
    println!("UmaDB started in {:?}", server_uptime());
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
            println!("\nUmaDB server shutdown complete");
        })
        .await?;

    Ok(())
}

/// Maximum number of blocking batch read allowed to execute
/// concurrently on the blocking thread pool.
///
/// This bounds CPU oversubscription so that the (small, fixed) Tokio reactor
/// threads always have CPU to service HTTP/2 keepalive frames. Permits are
/// acquired per batch and released between batches, so this does NOT limit the
/// number of live (mostly-parked) reads or subscriptions — only how many are
/// actively scanning storage at any instant.
///
/// Defaults to `available_parallelism() * PER_CORE`; overridable via the
/// `UMADB_READER_THREADS` environment variable.
fn readers_concurrency_limit() -> usize {
    const PER_CORE: usize = 4;
    if let Some(n) = std::env::var("UMADB_READER_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
    {
        return n;
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .saturating_mul(PER_CORE)
        .max(PER_CORE)
}

// gRPC server implementation
pub struct UmaDbServer {
    pub(crate) request_handler: UmaDbServerRequestHandler,
    shutdown_watch_rx: watch::Receiver<bool>,
    api_key: Option<String>,
    // Limits concurrent blocking read/subscribe batch-scans (see `read_scan_concurrency_limit`).
    readers_semaphore: Arc<Semaphore>,
}

impl UmaDbServer {
    pub fn new(
        shutdown_watch_rx: watch::Receiver<bool>,
        api_key: Option<String>,
        storage_options: StorageOptions,
    ) -> DcbResult<Self> {
        let request_handler = UmaDbServerRequestHandler::new(storage_options)?;
        let readers_semaphore = Arc::new(Semaphore::new(readers_concurrency_limit()));
        Ok(Self {
            request_handler,
            shutdown_watch_rx,
            api_key,
            readers_semaphore,
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
impl umadb_proto::v1::dcb_server::Dcb for UmaDbServer {
    type ReadStream =
        Pin<Box<dyn Stream<Item = Result<umadb_proto::v1::ReadResponse, Status>> + Send + 'static>>;
    async fn read(
        &self,
        request: Request<umadb_proto::v1::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        // Enforce API key if configured
        self.enforce_api_key(request.metadata())?;
        let read_request = request.into_inner();

        // Avoid confusion by reporting the usage error with guidance.
        #[allow(deprecated)]
        if read_request.subscribe.unwrap_or(false) {
            return Err(status_from_dcb_error(DcbError::InvalidArgument(
                "The `subscribe` argument of `read()` has been deprecated. \
                Please call the `subscribe()` method instead."
                    .to_string(),
            )));
        }

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
        let read_scan_semaphore = self.readers_semaphore.clone();

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
                // Throttle concurrent blocking scans so reactor threads stay free to
                // service HTTP/2 keepalive. The permit is held only for this batch scan
                // (moved into the closure), and released before we send the batch below.
                let permit = match read_scan_semaphore.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => break,
                };
                let handler = request_handler.clone();
                let query_val = query_clone.clone();
                let limit_val = Some(read_limit);
                let cancel_for_blocking = cancel_signal_for_task.clone();
                let mut blocking_handle = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
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

    type SubscribeStream = Pin<
        Box<dyn Stream<Item = Result<umadb_proto::v1::SubscribeResponse, Status>> + Send + 'static>,
    >;

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
        let read_scan_semaphore = self.readers_semaphore.clone();

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

                // Throttle concurrent blocking scans so reactor threads stay free to
                // service HTTP/2 keepalive. The permit is held only for this batch scan
                // (moved into the closure), and released before we send the batch or wait
                // for new events below — so long-lived subscriptions do not tie up a permit.
                let permit = match read_scan_semaphore.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => break,
                };
                let handler = request_handler.clone();
                let query_val = query_clone.clone();
                let batch_size_val = Some(batch_size);
                let cancel_for_blocking = cancel_signal_for_task.clone();
                let mut blocking_handle = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
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
        // `head()` reads a single header page (O(1)), but it is still blocking storage
        // I/O and must not run on a reactor thread, or it steals time from HTTP/2
        // keepalive under high concurrency. It is cheap enough not to need a permit.
        let request_handler = self.request_handler.clone();
        let res = tokio::task::spawn_blocking(move || request_handler.head())
            .await
            .map_err(|e| status_from_dcb_error(DcbError::InternalError(e.to_string())))?;
        match res {
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
        // This does a tracking-tree descent (bounded, but real file I/O), so run it off
        // the reactor and gate it with the same semaphore as read scans to bound
        // concurrent blocking storage work. The permit is released as soon as it returns.
        let permit = match self.readers_semaphore.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                return Err(status_from_dcb_error(DcbError::InternalError(
                    "read-scan semaphore closed".to_string(),
                )));
            }
        };
        let request_handler = self.request_handler.clone();
        let res = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            request_handler.get_tracking_info(req.source)
        })
        .await
        .map_err(|e| status_from_dcb_error(DcbError::InternalError(e.to_string())))?;
        match res {
            Ok(position) => Ok(Response::new(umadb_proto::v1::TrackingResponse {
                position,
            })),
            Err(e) => Err(status_from_dcb_error(e)),
        }
    }
}

// Function to start the gRPC server with a shutdown signal
// - this is only used in tests and benchmarks
#[cfg(any(test, feature = "test-utils"))]
pub async fn start_server<P: AsRef<std::path::Path>>(
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
