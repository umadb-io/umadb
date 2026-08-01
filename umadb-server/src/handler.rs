use crate::APPEND_BATCH_MAX_EVENTS;
use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::thread;
use tokio::sync::{mpsc, oneshot, watch};
use umadb_core::common::Position;
use umadb_core::db::{UmaDb, read_conditional};
use umadb_core::io_shell::io_shell;
use umadb_core::mvcc::{Mvcc, StorageOptions};
use umadb_core::writer_thread_blocking::writer_thread_blocking;
use umadb_core::writer_thread_pipelining::writer_thread_pipelining;
use umadb_core::writer_thread_request::WriterThreadRequest;
use umadb_dcb::{
    DcbAppendCondition, DcbError, DcbEvent, DcbQuery, DcbResult, DcbSequencedEvent, TrackingInfo,
};

// Thread-safe request handler
pub struct UmaDbServerRequestHandler {
    mvcc: Arc<Mvcc>,
    head_watch_tx: watch::Sender<Option<u64>>,
    writer_request_tx: mpsc::Sender<WriterThreadRequest>,
}

impl UmaDbServerRequestHandler {
    pub fn new(storage_options: StorageOptions) -> DcbResult<Self> {
        // Create a channel for sending requests to the writer thread
        let (writer_request_tx, writer_request_rx) = mpsc::channel::<WriterThreadRequest>(1024);

        let pipelined_writer_option = storage_options.pipelined_writer;
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
        if pipelined_writer_option {
            // Create a channel to communicate with the async I/O shell.
            let (io_tx, io_rx) = tokio::sync::mpsc::unbounded_channel();

            #[cfg(target_os = "linux")]
            {
                println!("UmaDB writing with io_uring");
                let file_path = mvcc.db_path.clone();
                let page_size = mvcc.page_size;
                std::thread::spawn(move || {
                    io_shell::start_io_uring_thread(file_path, page_size, io_rx);
                });
            }

            #[cfg(not(target_os = "linux"))]
            {
                let mvcc_clone = Arc::clone(&mvcc);
                std::thread::spawn(move || {
                    io_shell::start_blocking_io_thread(mvcc_clone, io_rx);
                });
            }

            thread::spawn(move || {
                writer_thread_pipelining(
                    io_tx,
                    mvcc_for_writer,
                    writer_request_rx,
                    head_tx_writer,
                    APPEND_BATCH_MAX_EVENTS,
                )
            });
        } else {
            thread::spawn(move || {
                writer_thread_blocking(
                    mvcc_for_writer,
                    writer_request_rx,
                    head_tx_writer,
                    APPEND_BATCH_MAX_EVENTS,
                )
            });
        }

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
            &FxHashMap::default(),
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
        let _ = self
            .writer_request_tx
            .send(WriterThreadRequest::Shutdown)
            .await;
    }
}

// Clone implementation for EventStoreHandle
impl Clone for UmaDbServerRequestHandler {
    fn clone(&self) -> Self {
        Self {
            mvcc: self.mvcc.clone(),
            head_watch_tx: self.head_watch_tx.clone(),
            writer_request_tx: self.writer_request_tx.clone(),
        }
    }
}
