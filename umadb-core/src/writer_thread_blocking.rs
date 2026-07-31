use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use umadb_dcb::{DcbError, DcbResult};
use crate::db::{clone_dcb_error, is_integrity_error, is_invalid_argument_error, process_append_request, shadow_for_batch_abort, UmaDb};
use crate::mvcc::Mvcc;
use crate::writer_thread_request::WriterThreadRequest;

pub fn writer_thread_blocking(mvcc: Arc<Mvcc>, mut request_rx: mpsc::Receiver<WriterThreadRequest>, head_watch_tx: watch::Sender<Option<u64>>, append_batch_max_events: usize) {
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
                    if total_events >= append_batch_max_events {
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
