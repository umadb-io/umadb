use async_trait::async_trait;
use futures::Stream;
use futures::ready;
use std::collections::VecDeque;
use std::fs;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

use tokio::runtime::{Handle, Runtime};
use umadb_dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBEventStoreAsync, DCBEventStoreSync, DCBQuery,
    DCBReadResponseAsync, DCBReadResponseSync, DCBResult, DCBSequencedEvent, DCBSubscriptionAsync,
    DCBSubscriptionSync, TrackingInfo,
};

use std::sync::{Once, OnceLock};
use tokio::sync::watch;

/// A global watch channel for shutdown/cancel signals.
static CANCEL_SENDER: OnceLock<watch::Sender<()>> = OnceLock::new();

/// Returns a receiver subscribed to the global cancel signal.
fn cancel_receiver() -> watch::Receiver<()> {
    let sender = CANCEL_SENDER.get_or_init(|| {
        let (tx, _rx) = watch::channel::<()>(());
        tx
    });
    sender.subscribe()
}

/// Sends the cancel signal to all receivers (e.g., on Ctrl-C).
pub fn trigger_cancel() {
    if let Some(sender) = CANCEL_SENDER.get() {
        let _ = sender.send(()); // ignore error if already closed
    }
}

static REGISTER_SIGINT: Once = Once::new();

pub fn register_cancel_sigint_handler() {
    REGISTER_SIGINT.call_once(|| {
        // Capture the current runtime handle; panic if none exists
        let handle = Handle::current();

        // Spawn a detached task on that runtime
        handle.spawn(async {
            if tokio::signal::ctrl_c().await.is_ok() {
                trigger_cancel();
            }
        });
    });
}

pub struct UmaDBClient {
    url: String,
    ca_path: Option<String>,
    batch_size: Option<u32>,
    without_sigint_handler: bool,
    api_key: Option<String>,
}

impl UmaDBClient {
    pub fn new(url: String) -> Self {
        Self {
            url,
            ca_path: None,
            batch_size: None,
            without_sigint_handler: false,
            api_key: None,
        }
    }

    pub fn ca_path(self, ca_path: String) -> Self {
        Self {
            ca_path: Some(ca_path),
            ..self
        }
    }

    pub fn api_key(self, api_key: String) -> Self {
        Self {
            api_key: Some(api_key),
            ..self
        }
    }

    pub fn batch_size(self, batch_size: u32) -> Self {
        Self {
            batch_size: Some(batch_size),
            ..self
        }
    }

    pub fn without_sigint_handler(self) -> Self {
        Self {
            without_sigint_handler: true,
            ..self
        }
    }

    pub fn connect(&self) -> DCBResult<SyncUmaDBClient> {
        let client = SyncUmaDBClient::connect(
            self.url.clone(),
            self.ca_path.clone(),
            self.batch_size,
            self.api_key.clone(),
        );
        if !self.without_sigint_handler
            && let Ok(client) = &client
        {
            client.register_cancel_sigint_handler();
        }
        client
    }
    pub async fn connect_async(&self) -> DCBResult<AsyncUmaDBClient> {
        let client = AsyncUmaDBClient::connect(
            self.url.clone(),
            self.ca_path.clone(),
            self.batch_size,
            self.api_key.clone(),
        )
        .await;
        if !self.without_sigint_handler
            && let Ok(client) = &client
        {
            client.register_cancel_sigint_handler().await;
        }
        client
    }
}

// --- Sync wrapper around the async client ---
pub struct SyncUmaDBClient {
    async_client: AsyncUmaDBClient,
    handle: Handle,
    _runtime: Option<Runtime>, // Keeps runtime alive if we created it
}

impl SyncUmaDBClient {
    /// Subscribe to events starting from an optional position.
    /// This is a convenience wrapper around the async client's Subscribe RPC.
    /// The returned iterator yields events indefinitely until cancelled or the stream ends.
    pub fn subscribe(
        &self,
        query: Option<DCBQuery>,
        after: Option<u64>,
    ) -> DCBResult<Box<dyn DCBSubscriptionSync + Send + 'static>> {
        let async_subscription = self
            .handle
            .block_on(self.async_client.subscribe(query, after))?;
        Ok(Box::new(SyncClientSubscription {
            rt: self.handle.clone(),
            async_resp: async_subscription,
            buffer: VecDeque::new(),
            finished: false,
        }))
    }
    pub fn connect(
        url: String,
        ca_path: Option<String>,
        batch_size: Option<u32>,
        api_key: Option<String>,
    ) -> DCBResult<Self> {
        let (rt, handle) = Self::get_rt_handle();
        let async_client =
            handle.block_on(AsyncUmaDBClient::connect(url, ca_path, batch_size, api_key))?;
        Ok(Self {
            async_client,
            _runtime: rt, // Keep runtime alive for the client lifetime
            handle,
        })
    }

    pub fn connect_with_tls_options(
        url: String,
        tls_options: Option<ClientTlsOptions>,
        batch_size: Option<u32>,
    ) -> DCBResult<Self> {
        let (rt, handle) = Self::get_rt_handle();
        let async_client = handle.block_on(AsyncUmaDBClient::connect_with_tls_options(
            url,
            tls_options,
            batch_size,
            None,
        ))?;
        Ok(Self {
            async_client,
            _runtime: rt, // Keep runtime alive for the client lifetime
            handle,
        })
    }

    fn get_rt_handle() -> (Option<Runtime>, Handle) {
        let (rt, handle) = {
            // Try to use an existing runtime first
            if let Ok(handle) = Handle::try_current() {
                (None, handle)
            } else {
                // No runtime → create and own one
                let rt = Runtime::new().expect("failed to create Tokio runtime");
                let handle = rt.handle().clone();
                (Some(rt), handle)
            }
        };
        (rt, handle)
    }

    pub fn register_cancel_sigint_handler(&self) {
        self.handle
            .block_on(self.async_client.register_cancel_sigint_handler());
    }
}

impl DCBEventStoreSync for SyncUmaDBClient {
    fn read(
        &self,
        query: Option<DCBQuery>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
        subscribe: bool, // Deprecated - remove in v1.0.
    ) -> DCBResult<Box<dyn DCBReadResponseSync + Send + 'static>> {
        let async_read_response = self.handle.block_on(
            self.async_client
                .read(query, start, backwards, limit, subscribe),
        )?;
        Ok(Box::new(SyncClientReadResponse {
            rt: self.handle.clone(),
            async_resp: async_read_response,
            buffer: VecDeque::new(),
            finished: false,
        }))
    }

    fn head(&self) -> DCBResult<Option<u64>> {
        self.handle.block_on(self.async_client.head())
    }

    fn get_tracking_info(&self, source: &str) -> DCBResult<Option<u64>> {
        self.handle
            .block_on(self.async_client.get_tracking_info(source))
    }

    fn append(
        &self,
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
        tracking_info: Option<TrackingInfo>,
    ) -> DCBResult<u64> {
        self.handle
            .block_on(self.async_client.append(events, condition, tracking_info))
    }
}

pub struct SyncClientReadResponse {
    rt: Handle,
    async_resp: Box<dyn DCBReadResponseAsync + Send + 'static>,
    buffer: VecDeque<DCBSequencedEvent>, // efficient pop_front()
    finished: bool,
}

impl SyncClientReadResponse {
    /// Fetch the next batch from the async response, filling the buffer
    fn fetch_next_batch(&mut self) -> DCBResult<()> {
        if self.finished {
            return Ok(());
        }

        let batch = self.rt.block_on(self.async_resp.next_batch())?;
        if batch.is_empty() {
            self.finished = true;
        } else {
            self.buffer = batch.into();
        }
        Ok(())
    }
}

impl Iterator for SyncClientReadResponse {
    type Item = DCBResult<DCBSequencedEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        // Fetch the next batch if the buffer is empty.
        while self.buffer.is_empty() && !self.finished {
            if let Err(e) = self.fetch_next_batch() {
                return Some(Err(e));
            }
        }

        self.buffer.pop_front().map(Ok)
    }
}

impl DCBReadResponseSync for SyncClientReadResponse {
    fn head(&mut self) -> DCBResult<Option<u64>> {
        self.rt.block_on(self.async_resp.head())
    }

    fn collect_with_head(&mut self) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        let mut out = Vec::new();
        for result in self.by_ref() {
            out.push(result?);
        }
        Ok((out, self.head()?))
    }

    fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>> {
        // If there are remaining events in the buffer, drain them
        if !self.buffer.is_empty() {
            return Ok(self.buffer.drain(..).collect());
        }

        // Otherwise fetch a new batch
        self.fetch_next_batch()?;
        Ok(self.buffer.drain(..).collect())
    }
}

pub struct SyncClientSubscription {
    rt: Handle,
    async_resp: Box<dyn DCBSubscriptionAsync + Send + 'static>,
    buffer: VecDeque<DCBSequencedEvent>, // efficient pop_front()
    finished: bool,
}

impl SyncClientSubscription {
    /// Fetch the next batch from the async response, filling the buffer
    fn fetch_next_batch(&mut self) -> DCBResult<()> {
        if self.finished {
            return Ok(());
        }

        let batch = self.rt.block_on(self.async_resp.next_batch())?;
        if batch.is_empty() {
            self.finished = true;
        } else {
            self.buffer = batch.into();
        }
        Ok(())
    }
}

impl Iterator for SyncClientSubscription {
    type Item = DCBResult<DCBSequencedEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        // Fetch the next batch if the buffer is empty.
        while self.buffer.is_empty() && !self.finished {
            if let Err(e) = self.fetch_next_batch() {
                return Some(Err(e));
            }
        }

        self.buffer.pop_front().map(Ok)
    }
}

impl DCBSubscriptionSync for SyncClientSubscription {
    fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>> {
        // If there are remaining events in the buffer, drain them
        if !self.buffer.is_empty() {
            return Ok(self.buffer.drain(..).collect());
        }

        // Otherwise fetch a new batch
        self.fetch_next_batch()?;
        Ok(self.buffer.drain(..).collect())
    }
}

// Async client implementation
pub struct AsyncUmaDBClient {
    client: umadb_proto::v1::dcb_client::DcbClient<Channel>,
    batch_size: Option<u32>,
    tls_enabled: bool,
    api_key: Option<String>,
}

impl AsyncUmaDBClient {
    pub async fn subscribe(
        &self,
        query: Option<DCBQuery>,
        after: Option<u64>,
    ) -> DCBResult<Box<dyn DCBSubscriptionAsync + Send + 'static>> {
        let query_proto = query.map(|q| q.into());
        let mut client = self.client.clone();
        let req_body = umadb_proto::v1::SubscribeRequest {
            query: query_proto,
            after,
            batch_size: self.batch_size,
        };
        let req = self.add_auth(Request::new(req_body))?;
        let response = client
            .subscribe(req)
            .await
            .map_err(umadb_proto::dcb_error_from_status)?;
        let stream = response.into_inner();
        Ok(Box::new(AsyncClientSubscribeResponse::new(stream)))
    }
    pub async fn connect(
        url: String,
        ca_path: Option<String>,
        batch_size: Option<u32>,
        api_key: Option<String>,
    ) -> DCBResult<Self> {
        // Try to read the CA certificate.
        let ca_pem = {
            if let Some(ca_path) = ca_path {
                let ca_path = PathBuf::from(ca_path);
                Some(
                    fs::read(&ca_path)
                        .unwrap_or_else(|_| panic!("Couldn't read cert_path: {:?}", ca_path)),
                )
            } else {
                None
            }
        };

        let client_tls_options = Some(ClientTlsOptions {
            domain: None,
            ca_pem,
        });

        Self::connect_with_tls_options(url, client_tls_options, batch_size, api_key).await
    }

    pub async fn connect_with_tls_options(
        url: String,
        tls_options: Option<ClientTlsOptions>,
        batch_size: Option<u32>,
        api_key: Option<String>,
    ) -> DCBResult<Self> {
        let tls_enabled = url.starts_with("https://") || url.starts_with("grpcs://");
        match new_channel(url, tls_options).await {
            Ok(channel) => Ok(Self {
                client: umadb_proto::v1::dcb_client::DcbClient::new(channel),
                batch_size,
                tls_enabled,
                api_key,
            }),
            Err(err) => Err(DCBError::TransportError(format!(
                "failed to connect: {:?}",
                err
            ))),
        }
    }

    fn add_auth<T>(&self, mut req: Request<T>) -> DCBResult<Request<T>> {
        if let Some(key) = &self.api_key {
            if !self.tls_enabled {
                return Err(DCBError::TransportError(
                    "API key configured but TLS is not enabled; refusing to send credentials over insecure channel".to_string(),
                ));
            }
            let token = MetadataValue::from_str(&format!("Bearer {}", key))
                .map_err(|e| DCBError::TransportError(format!("invalid API key: {}", e)))?;
            req.metadata_mut().insert("authorization", token);
        }
        Ok(req)
    }

    pub async fn register_cancel_sigint_handler(&self) {
        register_cancel_sigint_handler();
    }
}

#[async_trait]
impl DCBEventStoreAsync for AsyncUmaDBClient {
    // Async inherent methods: use the gRPC client directly (no trait required)
    async fn read<'a>(
        &'a self,
        query: Option<DCBQuery>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
        subscribe: bool,
    ) -> DCBResult<Box<dyn DCBReadResponseAsync + Send + 'static>> {
        let query_proto = query.map(|q| q.into());
        let req_body = umadb_proto::v1::ReadRequest {
            query: query_proto,
            start,
            backwards: Some(backwards),
            limit,
            subscribe: Some(subscribe),
            batch_size: self.batch_size,
        };
        let mut client = self.client.clone();
        let req = self.add_auth(Request::new(req_body))?;
        let response = client
            .read(req)
            .await
            .map_err(umadb_proto::dcb_error_from_status)?;
        let stream = response.into_inner();
        Ok(Box::new(AsyncClientReadResponse::new(stream)))
    }

    async fn head(&self) -> DCBResult<Option<u64>> {
        let mut client = self.client.clone();
        let req = self.add_auth(Request::new(umadb_proto::v1::HeadRequest {}))?;
        match client.head(req).await {
            Ok(response) => Ok(response.into_inner().position),
            Err(status) => Err(umadb_proto::dcb_error_from_status(status)),
        }
    }

    async fn get_tracking_info(&self, source: &str) -> DCBResult<Option<u64>> {
        let mut client = self.client.clone();
        let req = self.add_auth(Request::new(umadb_proto::v1::TrackingRequest {
            source: source.to_string(),
        }))?;
        match client.get_tracking_info(req).await {
            Ok(response) => Ok(response.into_inner().position),
            Err(status) => Err(umadb_proto::dcb_error_from_status(status)),
        }
    }

    async fn append(
        &self,
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
        tracking_info: Option<TrackingInfo>,
    ) -> DCBResult<u64> {
        let events_proto: Vec<umadb_proto::v1::Event> = events
            .into_iter()
            .map(umadb_proto::v1::Event::from)
            .collect();
        let condition_proto = condition.map(|c| umadb_proto::v1::AppendCondition {
            fail_if_events_match: Some(c.fail_if_events_match.into()),
            after: c.after,
        });
        let tracking_info_proto = tracking_info.map(|t| umadb_proto::v1::TrackingInfo {
            source: t.source,
            position: t.position,
        });
        let body = umadb_proto::v1::AppendRequest {
            events: events_proto,
            condition: condition_proto,
            tracking_info: tracking_info_proto,
        };
        let mut client = self.client.clone();
        let req = self.add_auth(Request::new(body))?;
        match client.append(req).await {
            Ok(response) => Ok(response.into_inner().position),
            Err(status) => Err(umadb_proto::dcb_error_from_status(status)),
        }
    }
}

/// Async read response wrapper that provides batched access and head metadata
pub struct AsyncClientReadResponse {
    stream: tonic::Streaming<umadb_proto::v1::ReadResponse>,
    buffered: VecDeque<DCBSequencedEvent>,
    last_head: Option<Option<u64>>, // None = unknown yet; Some(x) = known
    ended: bool,
    cancel: watch::Receiver<()>,
}

impl AsyncClientReadResponse {
    pub fn new(stream: tonic::Streaming<umadb_proto::v1::ReadResponse>) -> Self {
        Self {
            stream,
            buffered: VecDeque::new(),
            last_head: None,
            ended: false,
            cancel: cancel_receiver(),
        }
    }

    /// Fetches the next batch if needed, filling the buffer
    async fn fetch_next_if_needed(&mut self) -> DCBResult<()> {
        if !self.buffered.is_empty() || self.ended {
            return Ok(());
        }

        tokio::select! {
            _ = self.cancel.changed() => {
                self.ended = true;
                // return Ok(());
                return Err(DCBError::CancelledByUser());
            }
            msg = self.stream.message() => {
                match msg {
                    Ok(Some(resp)) => {
                        self.last_head = Some(resp.head);
                        let mut buffered = VecDeque::with_capacity(resp.events.len());
                        for e in resp.events {
                            if let Some(ev) = e.event {
                                let event = DCBEvent::try_from(ev)?;
                                buffered.push_back(DCBSequencedEvent { position: e.position, event });
                            }
                        }
                        self.buffered = buffered;
                    }
                    Ok(None) => self.ended = true,
                    Err(status) => return Err(umadb_proto::dcb_error_from_status(status)),
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl DCBReadResponseAsync for AsyncClientReadResponse {
    async fn head(&mut self) -> DCBResult<Option<u64>> {
        if let Some(h) = self.last_head {
            return Ok(h);
        }
        // Need to read at least one message to learn head
        self.fetch_next_if_needed().await?;
        Ok(self.last_head.unwrap_or(None))
    }

    async fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>> {
        if !self.buffered.is_empty() {
            return Ok(self.buffered.drain(..).collect());
        }

        self.fetch_next_if_needed().await?;

        if !self.buffered.is_empty() {
            return Ok(self.buffered.drain(..).collect());
        }

        Ok(Vec::new())
    }
}

impl Stream for AsyncClientReadResponse {
    type Item = DCBResult<DCBSequencedEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            // Return buffered event if available
            if let Some(ev) = this.buffered.pop_front() {
                return Poll::Ready(Some(Ok(ev)));
            }

            // Stop if the stream ended.
            if this.ended {
                return Poll::Ready(None);
            }

            // Poll the underlying tonic::Streaming
            return match ready!(Pin::new(&mut this.stream).poll_next(cx)) {
                Some(Ok(resp)) => {
                    this.last_head = Some(resp.head);

                    let mut buffered = VecDeque::with_capacity(resp.events.len());
                    for e in resp.events {
                        if let Some(ev) = e.event {
                            // Propagate any conversion error using DCBResult.
                            let event = match DCBEvent::try_from(ev) {
                                Ok(event) => event,
                                Err(err) => return Poll::Ready(Some(Err(err))),
                            };
                            buffered.push_back(DCBSequencedEvent {
                                position: e.position,
                                event,
                            });
                        }
                    }

                    this.buffered = buffered;

                    // If the batch is empty, loop again to poll the next message
                    if this.buffered.is_empty() {
                        continue;
                    }

                    // Otherwise, return the first event
                    let ev = this.buffered.pop_front().unwrap();
                    Poll::Ready(Some(Ok(ev)))
                }
                Some(Err(status)) => {
                    this.ended = true;
                    Poll::Ready(Some(Err(umadb_proto::dcb_error_from_status(status))))
                }
                None => {
                    this.ended = true;
                    Poll::Ready(None)
                }
            };
        }
    }
}

// Async subscribe response wrapper: similar to AsyncClientReadResponse but without head
pub struct AsyncClientSubscribeResponse {
    stream: tonic::Streaming<umadb_proto::v1::SubscribeResponse>,
    buffered: VecDeque<DCBSequencedEvent>,
    ended: bool,
    cancel: watch::Receiver<()>,
}

impl AsyncClientSubscribeResponse {
    pub fn new(stream: tonic::Streaming<umadb_proto::v1::SubscribeResponse>) -> Self {
        Self {
            stream,
            buffered: VecDeque::new(),
            ended: false,
            cancel: cancel_receiver(),
        }
    }

    async fn fetch_next_if_needed(&mut self) -> DCBResult<()> {
        if !self.buffered.is_empty() || self.ended {
            return Ok(());
        }

        tokio::select! {
            _ = self.cancel.changed() => {
                self.ended = true;
                return Err(DCBError::CancelledByUser());
            }
            msg = self.stream.message() => {
                match msg {
                    Ok(Some(resp)) => {
                        let mut buffered = VecDeque::with_capacity(resp.events.len());
                        for e in resp.events {
                            if let Some(ev) = e.event {
                                let event = DCBEvent::try_from(ev)?;
                                buffered.push_back(DCBSequencedEvent { position: e.position, event });
                            }
                        }
                        self.buffered = buffered;
                    }
                    Ok(None) => self.ended = true,
                    Err(status) => return Err(umadb_proto::dcb_error_from_status(status)),
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl DCBSubscriptionAsync for AsyncClientSubscribeResponse {
    async fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>> {
        if !self.buffered.is_empty() {
            return Ok(self.buffered.drain(..).collect());
        }
        self.fetch_next_if_needed().await?;
        if !self.buffered.is_empty() {
            return Ok(self.buffered.drain(..).collect());
        }
        Ok(Vec::new())
    }
}

impl Stream for AsyncClientSubscribeResponse {
    type Item = DCBResult<DCBSequencedEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if let Some(ev) = this.buffered.pop_front() {
                return Poll::Ready(Some(Ok(ev)));
            }
            if this.ended {
                return Poll::Ready(None);
            }

            return match ready!(Pin::new(&mut this.stream).poll_next(cx)) {
                Some(Ok(resp)) => {
                    let mut buffered = VecDeque::with_capacity(resp.events.len());
                    for e in resp.events {
                        if let Some(ev) = e.event {
                            let event = match DCBEvent::try_from(ev) {
                                Ok(event) => event,
                                Err(err) => return Poll::Ready(Some(Err(err))),
                            };
                            buffered.push_back(DCBSequencedEvent {
                                position: e.position,
                                event,
                            });
                        }
                    }
                    this.buffered = buffered;
                    if this.buffered.is_empty() {
                        continue;
                    }
                    let ev = this.buffered.pop_front().unwrap();
                    Poll::Ready(Some(Ok(ev)))
                }
                Some(Err(status)) => {
                    this.ended = true;
                    Poll::Ready(Some(Err(umadb_proto::dcb_error_from_status(status))))
                }
                None => {
                    this.ended = true;
                    Poll::Ready(None)
                }
            };
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ClientTlsOptions {
    pub domain: Option<String>,
    pub ca_pem: Option<Vec<u8>>, // trusted CA cert in PEM for self-signed setups
}

async fn new_channel(
    url: String,
    tls: Option<ClientTlsOptions>,
) -> Result<Channel, tonic::transport::Error> {
    new_endpoint(url, tls)?.connect().await
}

fn new_endpoint(
    url: String,
    tls: Option<ClientTlsOptions>,
) -> Result<Endpoint, tonic::transport::Error> {
    use std::time::Duration;

    // Accept grpcs:// as an alias for https://
    let mut url_owned = url.to_string();
    if url_owned.starts_with("grpcs://") {
        url_owned = url_owned.replacen("grpcs://", "https://", 1);
    }

    let mut endpoint = Endpoint::from_shared(url_owned)?
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(5))
        .keep_alive_timeout(Duration::from_secs(10))
        .initial_stream_window_size(Some(4 * 1024 * 1024))
        .initial_connection_window_size(Some(8 * 1024 * 1024));

    if let Some(opts) = tls {
        let mut cfg = ClientTlsConfig::new();
        if let Some(domain) = &opts.domain {
            cfg = cfg.domain_name(domain.clone());
        }
        if let Some(ca) = opts.ca_pem {
            cfg = cfg.ca_certificate(Certificate::from_pem(ca));
        }
        endpoint = endpoint.tls_config(cfg)?;
    } else if url.starts_with("https://") {
        // When using https without explicit options, still enable default TLS.
        endpoint = endpoint.tls_config(ClientTlsConfig::new())?;
    }

    Ok(endpoint)
}
