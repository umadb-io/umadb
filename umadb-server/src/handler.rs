use std::sync::Arc;
use std::thread;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch::Sender;
use umadb_core::common::Position;
use umadb_core::db::{clone_dcb_error, is_integrity_error, is_invalid_argument_error, process_append_request, read_conditional, shadow_for_batch_abort, UmaDb};
use umadb_core::mvcc::{Mvcc, StorageOptions};
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
        thread::spawn(move || Self::writer_thread(mvcc_for_writer, writer_request_rx, head_tx_writer));

        Ok(Self {
            mvcc,
            head_watch_tx,
            writer_request_tx,
        })
    }

    fn writer_thread(mvcc: Arc<Mvcc>, mut request_rx: Receiver<WriterThreadRequest>, head_watch_tx: Sender<Option<u64>>) {
        let db = UmaDb::from_arc(mvcc);

        // Process writer requests.
        while let Some(request) = request_rx.blocking_recv() {
            match request {
                WriterThreadRequest::Append {
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
                    let result = process_append_request(
                        events,
                        condition,
                        tracking_info,
                        mvcc.as_ref(),
                        &mut writer,
                        cancel,
                        mvcc.page_size,
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
                            Ok(WriterThreadRequest::Append {
                                   events,
                                   condition,
                                   tracking_info,
                                   response_tx,
                                   cancel,
                               }) => {
                                let ev_len = events.len();
                                let idx_in_batch = responders.len();
                                responders.push(response_tx);
                                let res_next = process_append_request(
                                    events,
                                    condition,
                                    tracking_info,
                                    mvcc.as_ref(),
                                    &mut writer,
                                    cancel,
                                    mvcc.page_size,
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
                            Ok(WriterThreadRequest::Shutdown) => {
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
                            let _ = head_watch_tx.send(new_head);
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
                WriterThreadRequest::Shutdown => {
                    break;
                }
            }
        }
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