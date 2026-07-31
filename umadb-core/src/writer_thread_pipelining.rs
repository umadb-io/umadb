use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch::Sender;
use umadb_dcb::{DcbError, DcbResult};
use crate::common::PageID;
use crate::db::{clone_dcb_error, is_integrity_error, is_invalid_argument_error, process_append_request, shadow_for_batch_abort};
use crate::io_shell::io_shell::IoJob;
use crate::mvcc::{Mvcc, Writer, WriterSnapshot};
use crate::page::Page;
use crate::writer_thread_request::WriterThreadRequest;


struct InflightCommit {
    io_rx: oneshot::Receiver<DcbResult<()>>,
    responders: Vec<oneshot::Sender<DcbResult<u64>>>,
    results: Vec<DcbResult<u64>>,
    new_head: Option<u64>,
    wet_pages_to_cache: HashMap<PageID, Arc<Page>>,
}


pub fn writer_thread_pipelining(
    io_tx: mpsc::UnboundedSender<IoJob>,
    mvcc_arc: Arc<Mvcc>,
    mut request_rx: Receiver<WriterThreadRequest>,
    head_watch_tx: Sender<Option<u64>>,
    append_batch_max_events: usize,
) {
    let mvcc = mvcc_arc.as_ref();
    let mut wet_pages: HashMap<PageID, Arc<Page>> = HashMap::new();
    let mut inflight_io: Option<InflightCommit> = None;

    // The writer is persisted across batches. Batch N is written to disk in the
    // background while we build batch N+1, so batch N+1's starting state (tree
    // roots, `next_page_id`, `next_position`, TSN) cannot be re-derived from the
    // header — the header still reflects the last acknowledged batch, one behind
    // the in-flight one. Re-deriving it (the old `mvcc.writer()` per batch) made
    // two batches allocate the same page IDs and corrupted the trees. Instead we
    // keep a single `active_writer` and let each batch continue where the previous
    // one left off; `prepare_commit` advances it (TSN, header slot, clears the
    // per-page cache) so it is primed for the next batch.
    let mut active_writer = match mvcc.writer() {
        Ok(w) => w,
        Err(e) => {
            eprintln!("Fatal error initializing writer thread: {e:?}");
            return;
        }
    };

    // Acknowledge a completed in-flight commit: wait for its disk I/O, then on
    // success publish its pages to the cache, reply to clients and bump the head.
    // Returns whether the I/O succeeded — a failure means `active_writer` was built
    // on top of a batch that never became durable, so the caller must roll back.
    let acknowledge_inflight = |inflight: InflightCommit, mvcc: &Mvcc| -> bool {
        match inflight.io_rx.blocking_recv() {
            Ok(Ok(())) => {
                let _ = mvcc.update_page_cache(inflight.wet_pages_to_cache);
                for (res, tx) in inflight.results.into_iter().zip(inflight.responders.into_iter()) {
                    let _ = tx.send(res);
                }
                let _ = head_watch_tx.send(inflight.new_head);
                true
            }
            Ok(Err(db_err)) => {
                for tx in inflight.responders {
                    let _ = tx.send(Err(clone_dcb_error(&db_err)));
                }
                false
            }
            Err(_) => {
                // Background thread panicked or dropped
                for tx in inflight.responders {
                    let _ = tx.send(Err(DcbError::InternalError("Disk I/O thread failed".into())));
                }
                false
            }
        }
    };

    // Reset the writer to the latest durable state after an I/O failure discards
    // the in-flight batch (and the not-yet-durable batch we built on top of it).
    let rollback = |active_writer: &mut Writer, wet_pages: &mut HashMap<PageID, Arc<Page>>| {
        wet_pages.clear();
        match mvcc.writer() {
            Ok(w) => *active_writer = w,
            Err(e) => {
                // The durable state itself is unreadable; nothing safe to do but stop.
                eprintln!("Fatal error rolling back writer after I/O failure: {e:?}");
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

                    let snapshot = WriterSnapshot {
                        base_mvcc: mvcc,
                        wet_pages: &wet_pages,
                    };

                    // Refresh the set of reusable (freed and now-reclaimable) page
                    // IDs for this batch. The old `mvcc.writer()` did this per batch;
                    // with a persisted writer we must do it explicitly, reading the
                    // free-list through the snapshot so it sees the previous batch's
                    // not-yet-durable pages, and gating on the smallest live reader
                    // TSN so we never reuse a page a live reader can still see.
                    if let Err(e) = active_writer
                        .find_reusable_page_ids_snap(&snapshot, mvcc.reader_tsns.min())
                    {
                        let _ = response_tx.send(Err(clone_dcb_error(&e)));
                        continue;
                    }

                    let mut responders = vec![response_tx];
                    let mut results = Vec::new();
                    let mut abort_idx = None;
                    let mut abort_err = None;

                    let result = process_append_request(
                        events, condition, tracking_info, &snapshot, &mut active_writer, cancel,
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
                        if total_events >= append_batch_max_events || abort_idx.is_some() {
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
                                    events, condition, tracking_info, &snapshot, &mut active_writer, cancel,
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
                        if !acknowledge_inflight(inflight, mvcc) {
                            // Batch N's disk I/O failed. Batch N+1 was built on top of
                            // it, so both are invalid. Reset from durable state, reject
                            // the in-progress batch, and start fresh.
                            rollback(&mut active_writer, &mut wet_pages);
                            for tx in responders {
                                let _ = tx.send(Err(DcbError::InternalError(
                                    "batch discarded: preceding commit failed".into(),
                                )));
                            }
                            continue;
                        }
                    }

                    // Handle aborts for the new batch. A non-integrity error means the
                    // writer's in-memory state is now inconsistent (a partial mutation),
                    // so we must roll back rather than commit anything.
                    if let (Some(failed_at), Some(orig_err)) = (abort_idx, abort_err) {
                        let shadow = shadow_for_batch_abort(&orig_err);
                        for (i, tx) in responders.into_iter().enumerate() {
                            if i == failed_at {
                                let _ = tx.send(Err(clone_dcb_error(&orig_err)));
                            } else {
                                let _ = tx.send(Err(clone_dcb_error(&shadow)));
                            }
                        }
                        rollback(&mut active_writer, &mut wet_pages);
                        continue;
                    }

                    // --- DISPATCH BATCH N+1 ---
                    match mvcc.prepare_commit(&mut active_writer, &snapshot) {
                        Ok((prepared_commit, new_wet_pages)) => {
                            let last_committed = active_writer.next_position.0.saturating_sub(1);
                            let new_head = if last_committed == 0 { None } else { Some(last_committed) };

                            wet_pages = new_wet_pages.clone();

                            // INSTANTLY FIRE OFF TO THE IO_URING THREAD
                            let (tx, io_rx) = tokio::sync::oneshot::channel();
                            let _ = io_tx.send((prepared_commit, tx)); // io_tx is the Sender to the background thread

                            // let io_rx = spawn_commit_io(Arc::clone(&mvcc_arc), prepared_commit);

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
                            // prepare_commit may have partially advanced the writer;
                            // reset from durable state before the next batch.
                            rollback(&mut active_writer, &mut wet_pages);
                        }
                    }
                }
            }
        } else {
            // --- IDLE PHASE ---
            // The channel is empty, but we have inflight I/O.
            // Wait for the disk to finish so we can acknowledge the clients!
            if let Some(inflight) = inflight_io.take() {
                if !acknowledge_inflight(inflight, mvcc) {
                    rollback(&mut active_writer, &mut wet_pages);
                }
            }
        }
    }

    // 6. SHUTDOWN REAPING
    if let Some(inflight) = inflight_io.take() {
        acknowledge_inflight(inflight, mvcc);
    }
}