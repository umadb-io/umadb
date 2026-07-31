use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch::Sender;
use umadb_core::common::{PageID, Position};
use umadb_core::db::{clone_dcb_error, is_integrity_error, is_invalid_argument_error, process_append_request, read_conditional, shadow_for_batch_abort, UmaDb};
use umadb_core::mvcc::{spawn_commit_io, Mvcc, StorageOptions, WriterSnapshot};
use umadb_core::page::Page;
use umadb_dcb::{DcbAppendCondition, DcbError, DcbEvent, DcbQuery, DcbResult, DcbSequencedEvent, TrackingInfo};
use crate::APPEND_BATCH_MAX_EVENTS;

// Message types for communication between the gRPC server and the writer thread
enum WriterThreadRequest {
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
pub struct UmaDbRequestHandler {
    mvcc: Arc<Mvcc>,
    head_watch_tx: Sender<Option<u64>>,
    writer_request_tx: mpsc::Sender<WriterThreadRequest>,
}

impl UmaDbRequestHandler {
    pub fn new(storage_options: StorageOptions) -> DcbResult<Self> {
        // Create a channel for sending requests to the writer thread
        let (writer_request_tx, writer_request_rx) = mpsc::channel::<WriterThreadRequest>(1024);

        // Build a shared Mvcc instance (Arc) upfront so reads can proceed concurrently
        let mvcc = Arc::new(Mvcc::new(false, storage_options)?);

        // Initialize the head watch channel with the current head.
        let init_head = {
            let header_page = mvcc.get_latest_header_page()?;
            let header = header_page.as_header_node()?;
            let last = header.next_position.0.saturating_sub(1);
            if last == 0 { None } else { Some(last) }
        };
        let (head_watch_tx, _head_rx) = watch::channel::<Option<u64>>(init_head);

        // Spawn a thread for processing writer requests.
        let mvcc_for_writer = mvcc.clone();
        let head_tx_writer = head_watch_tx.clone();
        thread::spawn(move || writer_thread(mvcc_for_writer, writer_request_rx, head_tx_writer));

        Ok(Self {
            mvcc,
            head_watch_tx,
            writer_request_tx,
        })
    }

    pub fn read(
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
            self.mvcc.as_ref(),
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

    pub fn head(&self) -> DcbResult<Option<u64>> {
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

    pub async fn append(
        &self,
        events: Vec<DcbEvent>,
        condition: Option<DcbAppendCondition>,
        tracking_info: Option<TrackingInfo>,
        cancel: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> DcbResult<u64> {
        let (response_tx, response_rx) = oneshot::channel();

        self.writer_request_tx
            .send(WriterThreadRequest::Append {
                events,
                condition,
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

    pub fn get_tracking_info(&self, source: String) -> DcbResult<Option<u64>> {
        let db = UmaDb::from_arc(self.mvcc.clone());
        db.get_tracking_info(&source)
    }

    pub fn watch_head(&self) -> watch::Receiver<Option<u64>> {
        self.head_watch_tx.subscribe()
    }

    #[allow(dead_code)]
    async fn shutdown(&self) {
        let _ = self.writer_request_tx.send(WriterThreadRequest::Shutdown).await;
    }
}

// Clone implementation for EventStoreHandle
impl Clone for UmaDbRequestHandler {
    fn clone(&self) -> Self {
        Self {
            mvcc: self.mvcc.clone(),
            head_watch_tx: self.head_watch_tx.clone(),
            writer_request_tx: self.writer_request_tx.clone(),
        }
    }
}

struct InflightCommit {
    io_rx: oneshot::Receiver<DcbResult<()>>,
    responders: Vec<oneshot::Sender<DcbResult<u64>>>,
    results: Vec<DcbResult<u64>>,
    new_head: Option<u64>,
    wet_pages_to_cache: HashMap<PageID, Arc<Page>>,
}

fn writer_thread(
    mvcc_arc: Arc<Mvcc>,
    mut request_rx: Receiver<WriterThreadRequest>,
    head_watch_tx: Sender<Option<u64>>,
) {
    let mvcc = mvcc_arc.as_ref();
    let mut wet_pages: HashMap<PageID, Arc<Page>> = HashMap::new();
    let mut inflight_io: Option<InflightCommit> = None;

    // Helper closure to avoid duplicating the acknowledgment logic
    let acknowledge_inflight = |inflight: InflightCommit| {
        match inflight.io_rx.blocking_recv() {
            Ok(Ok(())) => {
                let _ = mvcc.update_page_cache(inflight.wet_pages_to_cache);
                for (res, tx) in inflight.results.into_iter().zip(inflight.responders.into_iter()) {
                    let _ = tx.send(res);
                }
                let _ = head_watch_tx.send(inflight.new_head);
            }
            Ok(Err(db_err)) => {
                for tx in inflight.responders {
                    let _ = tx.send(Err(clone_dcb_error(&db_err)));
                }
            }
            Err(_) => {
                // Background thread panicked or dropped
                for tx in inflight.responders {
                    let _ = tx.send(Err(DcbError::InternalError("Disk I/O thread failed".into())));
                }
            }
        }
    };

    loop {
        // 1. FETCH WORK OR WAIT FOR I/O
        // If we have inflight I/O, we CANNOT block on the request channel.
        let first_request = if inflight_io.is_some() {
            match request_rx.try_recv() {
                Ok(req) => Some(req),
                Err(mpsc::error::TryRecvError::Empty) => None,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        } else {
            // No inflight I/O, it is safe to sleep until a request arrives.
            match request_rx.blocking_recv() {
                Some(req) => Some(req),
                None => break,
            }
        };

        if let Some(request) = first_request {
            // --- WE HAVE A REQUEST (CPU PHASE) ---
            match request {
                WriterThreadRequest::Shutdown => break,
                WriterThreadRequest::Append {
                    events,
                    condition,
                    tracking_info,
                    response_tx,
                    cancel,
                } => {
                    let mut total_events = events.len();

                    let mut writer = match mvcc.writer() {
                        Ok(writer) => writer,
                        Err(err) => {
                            let _ = response_tx.send(Err(err));
                            continue;
                        }
                    };

                    let snapshot = WriterSnapshot {
                        base_mvcc: mvcc,
                        wet_pages: &wet_pages,
                    };

                    let mut responders = vec![response_tx];
                    let mut results = Vec::new();
                    let mut abort_idx = None;
                    let mut abort_err = None;

                    let result = process_append_request(
                        events, condition, tracking_info, &snapshot, &mut writer, cancel, mvcc.page_size,
                    );

                    match &result {
                        Ok(_) => results.push(result),
                        Err(e) if is_integrity_error(e) => results.push(Err(clone_dcb_error(e))),
                        Err(e) if is_invalid_argument_error(e) => results.push(Err(clone_dcb_error(e))),
                        Err(e) => {
                            abort_idx = Some(0);
                            abort_err = Some(clone_dcb_error(e));
                            results.push(Err(clone_dcb_error(e)));
                        }
                    }

                    // DRAIN CHANNEL FOR GROUP COMMIT
                    loop {
                        if total_events >= APPEND_BATCH_MAX_EVENTS || abort_idx.is_some() {
                            break;
                        }
                        match request_rx.try_recv() {
                            Ok(WriterThreadRequest::Append {
                                   events, condition, tracking_info, response_tx, cancel,
                               }) => {
                                let ev_len = events.len();
                                let idx_in_batch = responders.len();
                                responders.push(response_tx);

                                let res_next = process_append_request(
                                    events, condition, tracking_info, &snapshot, &mut writer, cancel, mvcc.page_size,
                                );

                                match &res_next {
                                    Ok(_) => results.push(res_next),
                                    Err(e) if is_integrity_error(e) => results.push(Err(clone_dcb_error(e))),
                                    Err(e) if is_invalid_argument_error(e) => results.push(Err(clone_dcb_error(e))),
                                    Err(e) => {
                                        abort_idx = Some(idx_in_batch);
                                        abort_err = Some(clone_dcb_error(e));
                                        results.push(Err(clone_dcb_error(e)));
                                    }
                                }
                                total_events += ev_len;
                            }
                            Ok(WriterThreadRequest::Shutdown) => break,
                            Err(_) => break,
                        }
                    }

                    // --- PIPELINE BARRIER ---
                    // We finished building Batch N+1. Before we can commit it,
                    // we MUST wait for Batch N to finish writing to disk.
                    if let Some(inflight) = inflight_io.take() {
                        acknowledge_inflight(inflight);
                    }

                    // Handle aborts for the new batch
                    if let (Some(failed_at), Some(orig_err)) = (abort_idx, abort_err) {
                        let shadow = shadow_for_batch_abort(&orig_err);
                        for (i, tx) in responders.into_iter().enumerate() {
                            if i == failed_at {
                                let _ = tx.send(Err(clone_dcb_error(&orig_err)));
                            } else {
                                let _ = tx.send(Err(clone_dcb_error(&shadow)));
                            }
                        }
                        continue;
                    }

                    // --- DISPATCH BATCH N+1 ---
                    match mvcc.prepare_commit(&mut writer, &snapshot) {
                        Ok((prepared_commit, new_wet_pages)) => {
                            let last_committed = writer.next_position.0.saturating_sub(1);
                            let new_head = if last_committed == 0 { None } else { Some(last_committed) };

                            wet_pages = new_wet_pages.clone();

                            let io_rx = spawn_commit_io(Arc::clone(&mvcc_arc), prepared_commit);

                            inflight_io = Some(InflightCommit {
                                io_rx,
                                responders,
                                results,
                                new_head,
                                wet_pages_to_cache: new_wet_pages,
                            });
                        }
                        Err(e) => {
                            for tx in responders {
                                let _ = tx.send(Err(clone_dcb_error(&e)));
                            }
                        }
                    }
                }
            }
        } else {
            // --- IDLE PHASE ---
            // The channel is empty, but we have inflight I/O.
            // Wait for the disk to finish so we can acknowledge the clients!
            if let Some(inflight) = inflight_io.take() {
                acknowledge_inflight(inflight);
            }
        }
    }

    // 6. SHUTDOWN REAPING
    if let Some(inflight) = inflight_io.take() {
        acknowledge_inflight(inflight);
    }
}