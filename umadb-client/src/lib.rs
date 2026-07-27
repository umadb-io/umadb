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
    DcbAppendCondition, DcbError, DcbEvent, DcbEventStoreAsync, DcbEventStoreSync, DcbQuery,
    DcbReadResponseAsync, DcbReadResponseSync, DcbResult, DcbSequencedEvent, DcbSubscriptionAsync,
    DcbSubscriptionSync, ShutdownStatus, StopHandle, TrackingInfo,
};

use futures::FutureExt;
use futures::future::BoxFuture;
use std::sync::{Arc, Once, OnceLock};
use std::time::Duration;
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

/// A global watch channel for stop signals.
static STOP_SENDER: OnceLock<watch::Sender<()>> = OnceLock::new();

/// Returns a receiver subscribed to the global stop signal.
fn stop_receiver() -> watch::Receiver<()> {
    let sender = STOP_SENDER.get_or_init(|| {
        let (tx, _rx) = watch::channel::<()>(());
        tx
    });
    sender.subscribe()
}

/// Sends the stop signal to all receivers (e.g., on Ctrl-C).
pub fn trigger_stop() {
    if let Some(sender) = STOP_SENDER.get() {
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

pub trait ShutdownEvaluator {
    // These methods act as "getters" for the underlying watch channels
    fn cancel_channel(&self) -> &watch::Receiver<()>;
    fn stop_channel(&self) -> &watch::Receiver<()>;
    fn local_stop_channel(&self) -> &watch::Receiver<()>;
    fn is_ended(&self) -> bool;

    fn evaluate_shutdown(&self) -> ShutdownStatus {
        // 1. Prioritize user cancellations (Ctrl-C / interrupt triggers)
        if self.cancel_channel().has_changed().unwrap_or(false) {
            return ShutdownStatus::CancelledByUser;
        }

        // 2. Fall back to graceful stop paths
        if self.is_ended()
            || self.local_stop_channel().has_changed().unwrap_or(false)
            || self.stop_channel().has_changed().unwrap_or(false)
        {
            return ShutdownStatus::StoppedGracefully;
        }

        ShutdownStatus::NotStopped
    }
}

pub trait IteratorWithTimeout {
    type Item;

    /// Pulls the next item, returning an explicit Timeout error if the window expires.
    fn next_timeout(&mut self, timeout: Duration) -> Option<Self::Item>;
}

/// Async read response wrapper that provides batched access and head metadata
pub struct AsyncClientReadResponse {
    stream: tonic::Streaming<umadb_proto::v1::ReadResponse>,
    buffer: VecDeque<DcbSequencedEvent>,
    last_head: Option<Option<u64>>, // None = unknown yet; Some(x) = known
    ended: bool,
    cancel: watch::Receiver<()>,
    stop: watch::Receiver<()>,
    // Per-stream stop signal, affecting only this individual response.
    local_stop_tx: Arc<watch::Sender<()>>,
    local_stop: watch::Receiver<()>,
}

impl AsyncClientReadResponse {
    pub fn new(stream: tonic::Streaming<umadb_proto::v1::ReadResponse>) -> Self {
        let (local_stop_tx, local_stop) = watch::channel(());
        let local_stop_tx = Arc::new(local_stop_tx);
        Self {
            stream,
            buffer: VecDeque::new(),
            last_head: None,
            ended: false,
            cancel: cancel_receiver(),
            stop: stop_receiver(),
            local_stop_tx,
            local_stop,
        }
    }

    /// Fetches the next batch if needed, filling the buffer
    async fn fetch_next_if_needed(&mut self) -> DcbResult<()> {
        if !self.buffer.is_empty() || self.ended {
            return Ok(());
        }

        tokio::select! {
            biased;
            _ = self.cancel.changed() => {
                self.ended = true;
                return Err(DcbError::CancelledByUser());
            }
            _ = self.stop.changed() => {
                self.ended = true;
                return Ok(());
            }
            _ = self.local_stop.changed() => {
                self.ended = true;
                return Ok(());
            }
            msg = self.stream.message() => {
                match msg {
                    Ok(Some(resp)) => {
                        self.last_head = Some(resp.head);
                        let mut buffer = VecDeque::with_capacity(resp.events.len());
                        for e in resp.events {
                            if let Some(ev) = e.event {
                                let event = DcbEvent::try_from(ev)?;
                                buffer.push_back(DcbSequencedEvent { position: e.position, event });
                            }
                        }
                        self.buffer = buffer;
                    }
                    Ok(None) => self.ended = true,
                    Err(status) => return Err(umadb_proto::dcb_error_from_status(status)),
                }
            }
        }

        Ok(())
    }

    async fn fetch_next_if_needed_timeout(&mut self, timeout: Duration) -> DcbResult<()> {
        if !self.buffer.is_empty() || self.ended {
            return Ok(());
        }

        tokio::select! {
            biased;
            _ = self.cancel.changed() => {
                self.ended = true;
                return Err(DcbError::CancelledByUser());
            }
            _ = self.stop.changed() => {
                self.ended = true;
                return Ok(());
            }
            _ = self.local_stop.changed() => {
                self.ended = true;
                return Ok(());
            }
            _ = tokio::time::sleep(timeout) => {
                return Err(DcbError::Timeout());
            }
            msg = self.stream.message() => {
                match msg {
                    Ok(Some(resp)) => {
                        self.last_head = Some(resp.head);
                        let mut buffer = VecDeque::with_capacity(resp.events.len());
                        for e in resp.events {
                            if let Some(ev) = e.event {
                                let event = DcbEvent::try_from(ev)?;
                                buffer.push_back(DcbSequencedEvent { position: e.position, event });
                            }
                        }
                        self.buffer = buffer;
                    }
                    Ok(None) => self.ended = true,
                    Err(status) => return Err(umadb_proto::dcb_error_from_status(status)),
                }
            }
        }

        Ok(())
    }
}

impl ShutdownEvaluator for AsyncClientReadResponse {
    fn cancel_channel(&self) -> &watch::Receiver<()> {
        &self.cancel
    }
    fn stop_channel(&self) -> &watch::Receiver<()> {
        &self.stop
    }
    fn local_stop_channel(&self) -> &watch::Receiver<()> {
        &self.local_stop
    }
    fn is_ended(&self) -> bool {
        self.ended
    }
}

#[async_trait]
impl DcbReadResponseAsync for AsyncClientReadResponse {
    async fn head(&mut self) -> DcbResult<Option<u64>> {
        if let Some(h) = self.last_head {
            return Ok(h);
        }
        // Need to read at least one message to learn head
        self.fetch_next_if_needed().await?;
        Ok(self.last_head.unwrap_or(None))
    }

    async fn next_batch(&mut self) -> DcbResult<Vec<DcbSequencedEvent>> {
        if !self.buffer.is_empty() {
            return Ok(self.buffer.drain(..).collect());
        }

        self.fetch_next_if_needed().await?;

        if !self.buffer.is_empty() {
            return Ok(self.buffer.drain(..).collect());
        }

        Ok(Vec::new())
    }

    async fn next_batch_timeout(&mut self, timeout: Duration) -> DcbResult<Vec<DcbSequencedEvent>> {
        if !self.buffer.is_empty() {
            return Ok(self.buffer.drain(..).collect());
        }

        // Invoke the updated timeout select loop
        self.fetch_next_if_needed_timeout(timeout).await?;

        if !self.buffer.is_empty() {
            return Ok(self.buffer.drain(..).collect());
        }

        Ok(Vec::new())
    }

    fn stop(&mut self) {
        let _ = self.local_stop_tx.send(());
    }

    fn stop_handle(&self) -> Option<StopHandle> {
        let tx = self.local_stop_tx.clone();
        Some(StopHandle::new(move || {
            let _ = tx.send(());
        }))
    }

    fn check_shutdown_status(&self) -> ShutdownStatus {
        <Self as ShutdownEvaluator>::evaluate_shutdown(self)
    }
}

impl Stream for AsyncClientReadResponse {
    type Item = DcbResult<DcbSequencedEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match this.evaluate_shutdown() {
                ShutdownStatus::CancelledByUser => {
                    this.buffer.clear();
                    // Explicitly bubble user cancellation up to the async stream collector
                    return Poll::Ready(Some(Err(DcbError::CancelledByUser())));
                }
                ShutdownStatus::StoppedGracefully => {
                    this.buffer.clear();
                    // Clean, silent stream end
                    return Poll::Ready(None);
                }
                ShutdownStatus::NotStopped => {}
            }

            // Return buffered event if available
            if let Some(ev) = this.buffer.pop_front() {
                return Poll::Ready(Some(Ok(ev)));
            }

            // Poll the underlying tonic::Streaming
            return match ready!(Pin::new(&mut this.stream).poll_next(cx)) {
                Some(Ok(resp)) => {
                    this.last_head = Some(resp.head);

                    let mut buffer = VecDeque::with_capacity(resp.events.len());
                    for e in resp.events {
                        if let Some(ev) = e.event {
                            let event = match DcbEvent::try_from(ev) {
                                Ok(event) => event,
                                Err(err) => return Poll::Ready(Some(Err(err))),
                            };
                            buffer.push_back(DcbSequencedEvent {
                                position: e.position,
                                event,
                            });
                        }
                    }

                    this.buffer = buffer;

                    // If the batch is empty, loop again to poll the next message
                    if this.buffer.is_empty() {
                        continue;
                    }

                    // Otherwise, return the first event
                    let ev = this.buffer.pop_front().unwrap();
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
pub struct AsyncClientSubscription {
    stream: tonic::Streaming<umadb_proto::v1::SubscribeResponse>,
    buffer: VecDeque<DcbSequencedEvent>,
    ended: bool,
    cancel: watch::Receiver<()>,
    stop: watch::Receiver<()>,
    // Per-stream stop signal, affecting only this individual subscription.
    local_stop_tx: Arc<watch::Sender<()>>,
    local_stop: watch::Receiver<()>,
}

impl AsyncClientSubscription {
    pub fn new(stream: tonic::Streaming<umadb_proto::v1::SubscribeResponse>) -> Self {
        let (local_stop_tx, local_stop) = watch::channel(());
        let local_stop_tx = Arc::new(local_stop_tx);
        Self {
            stream,
            buffer: VecDeque::new(),
            ended: false,
            cancel: cancel_receiver(),
            stop: stop_receiver(),
            local_stop_tx,
            local_stop,
        }
    }

    async fn fetch_next_if_needed(&mut self, timeout: Duration) -> DcbResult<()> {
        if !self.buffer.is_empty() || self.ended {
            return Ok(());
        }

        tokio::select! {
            biased;
            _ = self.cancel.changed() => {
                self.ended = true;
                return Err(DcbError::CancelledByUser());
            }
            _ = self.stop.changed() => {
                self.ended = true;
            }
            _ = self.local_stop.changed() => {
                self.ended = true;
            }
            _ = tokio::time::sleep(timeout) => {
                return Err(DcbError::Timeout());
            }
            msg = self.stream.message() => {
                match msg {
                    Ok(Some(resp)) => {
                        let mut buffer = VecDeque::with_capacity(resp.events.len());
                        for e in resp.events {
                            if let Some(ev) = e.event {
                                let event = DcbEvent::try_from(ev)?;
                                buffer.push_back(DcbSequencedEvent { position: e.position, event });
                            }
                        }
                        self.buffer = buffer;
                    }
                    Ok(None) => self.ended = true,
                    Err(status) => return Err(umadb_proto::dcb_error_from_status(status)),
                }
            }
        }
        Ok(())
    }
}

impl ShutdownEvaluator for AsyncClientSubscription {
    fn cancel_channel(&self) -> &watch::Receiver<()> {
        &self.cancel
    }
    fn stop_channel(&self) -> &watch::Receiver<()> {
        &self.stop
    }
    fn local_stop_channel(&self) -> &watch::Receiver<()> {
        &self.local_stop
    }
    fn is_ended(&self) -> bool {
        self.ended
    }
}

#[async_trait]
impl DcbSubscriptionAsync for AsyncClientSubscription {
    async fn next_batch(&mut self) -> DcbResult<Vec<DcbSequencedEvent>> {
        if !self.buffer.is_empty() {
            return Ok(self.buffer.drain(..).collect());
        }
        self.fetch_next_if_needed(Duration::from_secs(u64::MAX))
            .await?;
        if !self.buffer.is_empty() {
            return Ok(self.buffer.drain(..).collect());
        }
        Ok(Vec::new())
    }

    async fn next_batch_timeout(&mut self, timeout: Duration) -> DcbResult<Vec<DcbSequencedEvent>> {
        if !self.buffer.is_empty() {
            return Ok(self.buffer.drain(..).collect());
        }
        self.fetch_next_if_needed(timeout).await?;
        if !self.buffer.is_empty() {
            return Ok(self.buffer.drain(..).collect());
        }
        Ok(Vec::new())
    }

    fn stop(&mut self) {
        let _ = self.local_stop_tx.send(());
    }

    fn stop_handle(&self) -> Option<StopHandle> {
        let tx = self.local_stop_tx.clone();
        Some(StopHandle::new(move || {
            let _ = tx.send(());
        }))
    }

    fn check_shutdown_status(&self) -> ShutdownStatus {
        <Self as ShutdownEvaluator>::evaluate_shutdown(self)
    }
}

impl Stream for AsyncClientSubscription {
    type Item = DcbResult<DcbSequencedEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match this.evaluate_shutdown() {
                ShutdownStatus::CancelledByUser => {
                    this.buffer.clear();
                    // Explicitly bubble user cancellation up to the async stream collector
                    return Poll::Ready(Some(Err(DcbError::CancelledByUser())));
                }
                ShutdownStatus::StoppedGracefully => {
                    this.buffer.clear();
                    // Clean, silent stream end
                    return Poll::Ready(None);
                }
                ShutdownStatus::NotStopped => {}
            }

            if let Some(ev) = this.buffer.pop_front() {
                return Poll::Ready(Some(Ok(ev)));
            }

            return match ready!(Pin::new(&mut this.stream).poll_next(cx)) {
                Some(Ok(resp)) => {
                    let mut buffer = VecDeque::with_capacity(resp.events.len());
                    for e in resp.events {
                        if let Some(ev) = e.event {
                            let event = match DcbEvent::try_from(ev) {
                                Ok(event) => event,
                                Err(err) => return Poll::Ready(Some(Err(err))),
                            };
                            buffer.push_back(DcbSequencedEvent {
                                position: e.position,
                                event,
                            });
                        }
                    }
                    this.buffer = buffer;
                    if this.buffer.is_empty() {
                        continue;
                    }
                    let ev = this.buffer.pop_front().unwrap();
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

// Avoid duplication in sync "read response" and "subscription" code.
pub trait AsyncStreamBackend: Send + Unpin {
    fn next_batch_box(&mut self) -> BoxFuture<'_, DcbResult<Vec<DcbSequencedEvent>>>;
    fn next_batch_timeout_box(
        &mut self,
        timeout: Duration,
    ) -> BoxFuture<'_, DcbResult<Vec<DcbSequencedEvent>>>;
    fn stop(&mut self);
    fn stop_handle(&self) -> Option<StopHandle>;
    fn check_shutdown_status(&self) -> ShutdownStatus;
}

// Bridge to DcbReadResponseAsync.
impl AsyncStreamBackend for dyn DcbReadResponseAsync + Send + 'static {
    fn next_batch_box(&mut self) -> BoxFuture<'_, DcbResult<Vec<DcbSequencedEvent>>> {
        self.next_batch().boxed()
    }
    fn next_batch_timeout_box(
        &mut self,
        timeout: Duration,
    ) -> BoxFuture<'_, DcbResult<Vec<DcbSequencedEvent>>> {
        self.next_batch_timeout(timeout).boxed()
    }
    fn stop(&mut self) {
        self.stop();
    }
    fn stop_handle(&self) -> Option<StopHandle> {
        self.stop_handle()
    }

    fn check_shutdown_status(&self) -> ShutdownStatus {
        self.check_shutdown_status()
    }
}

// Bridge to DcbSubscriptionAsync.
impl AsyncStreamBackend for dyn DcbSubscriptionAsync + Send + 'static {
    fn next_batch_box(&mut self) -> BoxFuture<'_, DcbResult<Vec<DcbSequencedEvent>>> {
        self.next_batch().boxed()
    }
    fn next_batch_timeout_box(
        &mut self,
        timeout: Duration,
    ) -> BoxFuture<'_, DcbResult<Vec<DcbSequencedEvent>>> {
        self.next_batch_timeout(timeout).boxed()
    }
    fn stop(&mut self) {
        self.stop();
    }
    fn stop_handle(&self) -> Option<StopHandle> {
        self.stop_handle()
    }
    fn check_shutdown_status(&self) -> ShutdownStatus {
        self.check_shutdown_status()
    }
}

pub struct SyncStreamEngine<A: AsyncStreamBackend + ?Sized> {
    pub rt: Handle,
    pub finished: bool,
    pub buffer: VecDeque<DcbSequencedEvent>,
    pub async_resp: Box<A>,
}

impl<A: AsyncStreamBackend + ?Sized> SyncStreamEngine<A> {
    pub fn new(rt: Handle, async_resp: Box<A>) -> Self {
        Self {
            rt,
            finished: false,
            buffer: VecDeque::new(),
            async_resp,
        }
    }

    /// Reusable implementation for standard Iterator::next()
    pub fn next_item(&mut self) -> Option<DcbResult<DcbSequencedEvent>> {
        // Process shutdown status inline to avoid mismatched type boundaries
        if !self.finished {
            match self.async_resp.check_shutdown_status() {
                ShutdownStatus::CancelledByUser => {
                    self.finished = true;
                    self.buffer.clear();
                    return Some(Err(DcbError::CancelledByUser()));
                }
                ShutdownStatus::StoppedGracefully => {
                    self.finished = true;
                    self.buffer.clear();
                    return None; // Clean iterator end!
                }
                ShutdownStatus::NotStopped => {}
            }
        }

        while self.buffer.is_empty() && !self.finished {
            let res = self.rt.block_on(self.async_resp.next_batch_box());
            match res {
                Ok(batch) => {
                    if batch.is_empty() {
                        self.finished = true;
                    } else {
                        self.buffer = batch.into();
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }

        self.buffer.pop_front().map(Ok)
    }

    /// The single, reusable implementation for IteratorWithTimeout::next_timeout()
    pub fn next_item_timeout(&mut self, timeout: Duration) -> Option<DcbResult<DcbSequencedEvent>> {
        if !self.finished {
            match self.async_resp.check_shutdown_status() {
                ShutdownStatus::CancelledByUser => {
                    self.finished = true;
                    self.buffer.clear();
                    return Some(Err(DcbError::CancelledByUser()));
                }
                ShutdownStatus::StoppedGracefully => {
                    self.finished = true;
                    self.buffer.clear();
                    return None; // Clean timeout end
                }
                ShutdownStatus::NotStopped => {}
            }
        }

        if let Some(ev) = self.buffer.pop_front() {
            return Some(Ok(ev));
        }

        if self.finished {
            return None;
        }

        let res = self
            .rt
            .block_on(self.async_resp.next_batch_timeout_box(timeout));
        match res {
            Ok(batch) => {
                if batch.is_empty() {
                    self.finished = true;
                } else {
                    self.buffer = batch.into();
                }
            }
            Err(DcbError::Timeout()) => return Some(Err(DcbError::Timeout())),
            Err(e) => return Some(Err(e)),
        }

        self.buffer.pop_front().map(Ok)
    }

    /// Centralized next_batch logic shared by all synchronous traits
    pub fn engine_next_batch(&mut self) -> DcbResult<Vec<DcbSequencedEvent>> {
        if !self.buffer.is_empty() {
            return Ok(self.buffer.drain(..).collect());
        }

        // Use the internal trait object bridge to load fresh logs
        let batch = self.rt.block_on(self.async_resp.next_batch_box())?;
        if batch.is_empty() {
            self.finished = true;
        } else {
            self.buffer = batch.into();
        }
        Ok(self.buffer.drain(..).collect())
    }

    /// Centralized next_batch_timeout logic
    pub fn engine_next_batch_timeout(
        &mut self,
        timeout: Duration,
    ) -> DcbResult<Vec<DcbSequencedEvent>> {
        if !self.buffer.is_empty() {
            return Ok(self.buffer.drain(..).collect());
        }

        let batch = self
            .rt
            .block_on(self.async_resp.next_batch_timeout_box(timeout))?;
        if batch.is_empty() {
            self.finished = true;
        } else {
            self.buffer = batch.into();
        }
        Ok(self.buffer.drain(..).collect())
    }

    /// Centralized stop routine
    pub fn engine_stop(&mut self) {
        self.finished = true;
        self.async_resp.stop();
    }

    /// Centralized stop handle retrieval
    pub fn engine_stop_handle(&self) -> Option<StopHandle> {
        self.async_resp.stop_handle()
    }
}

pub struct SyncClientReadResponse {
    engine: SyncStreamEngine<dyn DcbReadResponseAsync + Send + 'static>,
}

impl SyncClientReadResponse {
    pub fn new(rt: Handle, async_resp: Box<dyn DcbReadResponseAsync + Send + 'static>) -> Self {
        Self {
            engine: SyncStreamEngine::new(rt, async_resp),
        }
    }
}

impl Iterator for SyncClientReadResponse {
    type Item = DcbResult<DcbSequencedEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        self.engine.next_item()
    }
}

impl IteratorWithTimeout for SyncClientReadResponse {
    type Item = DcbResult<DcbSequencedEvent>;

    fn next_timeout(&mut self, timeout: Duration) -> Option<Self::Item> {
        // Delegates directly to the generic engine, identical to the subscription!
        self.engine.next_item_timeout(timeout)
    }
}

impl DcbReadResponseSync for SyncClientReadResponse {
    fn head(&mut self) -> DcbResult<Option<u64>> {
        self.engine.rt.block_on(self.engine.async_resp.head())
    }

    fn collect_with_head(&mut self) -> DcbResult<(Vec<DcbSequencedEvent>, Option<u64>)> {
        let mut out = Vec::new();
        for result in self.by_ref() {
            out.push(result?);
        }
        Ok((out, self.head()?))
    }

    fn next_batch(&mut self) -> DcbResult<Vec<DcbSequencedEvent>> {
        self.engine.engine_next_batch()
    }

    fn stop(&mut self) {
        self.engine.engine_stop();
    }

    fn stop_handle(&self) -> Option<StopHandle> {
        self.engine.async_resp.stop_handle()
    }

    fn next_timeout(&mut self, timeout: Duration) -> Option<DcbResult<DcbSequencedEvent>> {
        <Self as IteratorWithTimeout>::next_timeout(self, timeout)
    }
}

pub struct SyncClientSubscription {
    engine: SyncStreamEngine<dyn DcbSubscriptionAsync + Send + 'static>,
}

impl SyncClientSubscription {
    pub fn new(rt: Handle, async_resp: Box<dyn DcbSubscriptionAsync + Send + 'static>) -> Self {
        Self {
            engine: SyncStreamEngine::new(rt, async_resp),
        }
    }
}

impl Iterator for SyncClientSubscription {
    type Item = DcbResult<DcbSequencedEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        self.engine.next_item()
    }
}

impl IteratorWithTimeout for SyncClientSubscription {
    type Item = DcbResult<DcbSequencedEvent>;

    fn next_timeout(&mut self, timeout: Duration) -> Option<Self::Item> {
        self.engine.next_item_timeout(timeout)
    }
}

impl DcbSubscriptionSync for SyncClientSubscription {
    fn next_batch(&mut self) -> DcbResult<Vec<DcbSequencedEvent>> {
        self.engine.engine_next_batch()
    }

    fn next_batch_timeout(&mut self, timeout: Duration) -> DcbResult<Vec<DcbSequencedEvent>> {
        self.engine.engine_next_batch_timeout(timeout)
    }

    fn stop(&mut self) {
        self.engine.async_resp.stop();
    }

    fn stop_handle(&self) -> Option<StopHandle> {
        self.engine.async_resp.stop_handle()
    }

    fn next_timeout(&mut self, timeout: Duration) -> Option<DcbResult<DcbSequencedEvent>> {
        <Self as IteratorWithTimeout>::next_timeout(self, timeout)
    }
}

#[derive(Clone, Debug, Default)]
pub struct ClientTlsOptions {
    pub domain: Option<String>,
    pub ca_pem: Option<Vec<u8>>, // trusted CA cert in PEM for self-signed setups
}

fn new_endpoint(
    url: String,
    tls: Option<ClientTlsOptions>,
) -> Result<Endpoint, tonic::transport::Error> {
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

async fn new_channel(
    url: String,
    tls: Option<ClientTlsOptions>,
) -> Result<Channel, tonic::transport::Error> {
    new_endpoint(url, tls)?.connect().await
}

// Async client implementation
pub struct AsyncUmaDbClient {
    client: umadb_proto::v1::dcb_client::DcbClient<Channel>,
    batch_size: Option<u32>,
    tls_enabled: bool,
    api_key: Option<String>,
}

impl AsyncUmaDbClient {
    pub async fn connect(
        url: String,
        ca_path: Option<String>,
        batch_size: Option<u32>,
        api_key: Option<String>,
    ) -> DcbResult<Self> {
        // Try to read the CA certificate.
        let ca_pem = {
            if let Some(ca_path) = ca_path {
                let ca_path = PathBuf::from(ca_path);
                Some(
                    fs::read(&ca_path)
                        .unwrap_or_else(|_| panic!("couldn't read cert_path: {:?}", ca_path)),
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

    pub async fn subscribe(
        &self,
        query: Option<DcbQuery>,
        after: Option<u64>,
    ) -> DcbResult<Box<dyn DcbSubscriptionAsync + Send + 'static>> {
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
        Ok(Box::new(AsyncClientSubscription::new(stream)))
    }

    pub async fn connect_with_tls_options(
        url: String,
        tls_options: Option<ClientTlsOptions>,
        batch_size: Option<u32>,
        api_key: Option<String>,
    ) -> DcbResult<Self> {
        let tls_enabled = url.starts_with("https://") || url.starts_with("grpcs://");
        match new_channel(url, tls_options).await {
            Ok(channel) => Ok(Self {
                client: umadb_proto::v1::dcb_client::DcbClient::new(channel),
                batch_size,
                tls_enabled,
                api_key,
            }),
            Err(err) => Err(DcbError::TransportError(format!("{err}"))),
        }
    }

    fn add_auth<T>(&self, mut req: Request<T>) -> DcbResult<Request<T>> {
        if let Some(key) = &self.api_key {
            if !self.tls_enabled {
                return Err(DcbError::TransportError(
                    "API key configured but TLS is not enabled; refusing to send credentials over insecure channel".to_string(),
                ));
            }
            let token = MetadataValue::from_str(&format!("Bearer {}", key))
                .map_err(|e| DcbError::TransportError(format!("invalid API key: {}", e)))?;
            req.metadata_mut().insert("authorization", token);
        }
        Ok(req)
    }

    pub async fn register_cancel_sigint_handler(&self) {
        register_cancel_sigint_handler();
    }
}

#[async_trait]
impl DcbEventStoreAsync for AsyncUmaDbClient {
    // Async inherent methods: use the gRPC client directly (no trait required)
    async fn read<'a>(
        &'a self,
        query: Option<DcbQuery>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
    ) -> DcbResult<Box<dyn DcbReadResponseAsync + Send + 'static>> {
        let query_proto = query.map(|q| q.into());
        #[allow(deprecated)]
        let req_body = umadb_proto::v1::ReadRequest {
            query: query_proto,
            start,
            backwards: Some(backwards),
            limit,
            subscribe: None,
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

    async fn head(&self) -> DcbResult<Option<u64>> {
        let mut client = self.client.clone();
        let req = self.add_auth(Request::new(umadb_proto::v1::HeadRequest {}))?;
        match client.head(req).await {
            Ok(response) => Ok(response.into_inner().position),
            Err(status) => Err(umadb_proto::dcb_error_from_status(status)),
        }
    }

    async fn get_tracking_info(&self, source: &str) -> DcbResult<Option<u64>> {
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
        events: Vec<DcbEvent>,
        condition: Option<DcbAppendCondition>,
        tracking_info: Option<TrackingInfo>,
    ) -> DcbResult<u64> {
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

// --- Sync wrapper around the async client ---
pub struct SyncUmaDbClient {
    async_client: AsyncUmaDbClient,
    handle: Handle,
    _runtime: Option<Runtime>, // Keeps runtime alive if we created it
}

impl SyncUmaDbClient {
    pub fn connect(
        url: String,
        ca_path: Option<String>,
        batch_size: Option<u32>,
        api_key: Option<String>,
    ) -> DcbResult<Self> {
        let (rt, handle) = Self::get_rt_handle();
        let async_client =
            handle.block_on(AsyncUmaDbClient::connect(url, ca_path, batch_size, api_key))?;
        Ok(Self {
            async_client,
            _runtime: rt, // Keep runtime alive for the client lifetime
            handle,
        })
    }

    /// Subscribe to events starting from an optional position.
    /// This is a convenience wrapper around the async client's Subscribe RPC.
    /// The returned iterator yields events indefinitely until cancelled or the stream ends.
    pub fn subscribe(
        &self,
        query: Option<DcbQuery>,
        after: Option<u64>,
    ) -> DcbResult<Box<dyn DcbSubscriptionSync + Send + 'static>> {
        let async_subscription = self
            .handle
            .block_on(self.async_client.subscribe(query, after))?;

        // Use the updated engine-backed constructor
        Ok(Box::new(SyncClientSubscription::new(
            self.handle.clone(),
            async_subscription,
        )))
    }

    // pub fn connect_with_tls_options(
    //     url: String,
    //     tls_options: Option<ClientTlsOptions>,
    //     batch_size: Option<u32>,
    // ) -> DcbResult<Self> {
    //     let (rt, handle) = Self::get_rt_handle();
    //     let async_client = handle.block_on(AsyncUmaDbClient::connect_with_tls_options(
    //         url,
    //         tls_options,
    //         batch_size,
    //         None,
    //     ))?;
    //     Ok(Self {
    //         async_client,
    //         _runtime: rt, // Keep runtime alive for the client lifetime
    //         handle,
    //     })
    // }

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

impl DcbEventStoreSync for SyncUmaDbClient {
    fn read(
        &self,
        query: Option<DcbQuery>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
    ) -> DcbResult<Box<dyn DcbReadResponseSync + Send + 'static>> {
        let async_read_response = self
            .handle
            .block_on(self.async_client.read(query, start, backwards, limit))?;

        // Use the updated engine-backed constructor
        Ok(Box::new(SyncClientReadResponse::new(
            self.handle.clone(),
            async_read_response,
        )))
    }

    fn head(&self) -> DcbResult<Option<u64>> {
        self.handle.block_on(self.async_client.head())
    }

    fn get_tracking_info(&self, source: &str) -> DcbResult<Option<u64>> {
        self.handle
            .block_on(self.async_client.get_tracking_info(source))
    }

    fn append(
        &self,
        events: Vec<DcbEvent>,
        condition: Option<DcbAppendCondition>,
        tracking_info: Option<TrackingInfo>,
    ) -> DcbResult<u64> {
        self.handle
            .block_on(self.async_client.append(events, condition, tracking_info))
    }
}

pub struct UmaDbClient {
    url: String,
    ca_path: Option<String>,
    batch_size: Option<u32>,
    without_sigint_handler: bool,
    api_key: Option<String>,
}

impl UmaDbClient {
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

    pub fn connect(&self) -> DcbResult<SyncUmaDbClient> {
        let client = SyncUmaDbClient::connect(
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
    pub async fn connect_async(&self) -> DcbResult<AsyncUmaDbClient> {
        let client = AsyncUmaDbClient::connect(
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
