#[cfg(target_os = "linux")]
pub mod io_shell {
    use crate::mvcc::PreparedCommit;
    use std::path::PathBuf;
    use tokio::sync::{mpsc, oneshot};
    use tokio_uring::fs::File;
    use umadb_dcb::{DcbError, DcbResult};

    pub type IoJob = (PreparedCommit, oneshot::Sender<DcbResult<()>>);

    pub fn start_io_uring_thread(
        file_path: PathBuf,
        page_size: usize,
        mut io_rx: mpsc::UnboundedReceiver<IoJob>,
    ) {
        let page_size_u64 = page_size as u64;

        // tokio_uring requires its own specialized single-threaded runtime
        tokio_uring::start(async move {
            // Open the file inside the uring runtime
            let file = tokio_uring::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&file_path)
                .await
                .expect("Failed to open DB file for io_uring");

            // Process pipeline batches continuously
            while let Some((prepared, response_tx)) = io_rx.recv().await {
                let result = do_uring_commit(&file, page_size_u64, prepared).await;
                let _ = response_tx.send(result);
            }
        });
    }

    async fn do_uring_commit(
        file: &File,
        page_size_u64: u64,
        prepared: PreparedCommit,
    ) -> DcbResult<()> {
        // 1. SUBMIT ALL DATA PAGES CONCURRENTLY
        let mut writes = Vec::with_capacity(prepared.pages_to_write.len());
        for (page_id, page_data) in prepared.pages_to_write {
            let offset = page_id.0 * page_size_u64;

            // In io_uring, the kernel takes ownership of the buffer memory
            // while it writes. We push the futures into a vector.
            writes.push(file.write_at(page_data, offset).submit());
        }

        // Wait for all data pages to hit the OS page cache concurrently
        let results = futures::future::join_all(writes).await;
        for (res, _returned_buffer) in results {
            res.map_err(|e| DcbError::Io(e))?;
        }

        // 2. FSYNC DATA (Barrier)
        file.sync_data().await.map_err(|e| DcbError::Io(e))?;

        // 3. WRITE HEADER
        let (header_id, header_data) = prepared.header_to_write;
        let header_offset = header_id.0 * page_size_u64;
        let (res, _buf) = file.write_at(header_data, header_offset).submit().await;
        res.map_err(|e| DcbError::Io(e))?;

        // 4. FSYNC HEADER (Barrier)
        file.sync_all().await.map_err(|e| DcbError::Io(e))?;

        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
pub mod io_shell {
    use crate::mvcc::{Mvcc, PreparedCommit};
    use std::sync::Arc;
    use tokio::sync::{mpsc, oneshot};
    use umadb_dcb::DcbResult;

    // The exact same job signature as the io_uring version
    pub type IoJob = (PreparedCommit, oneshot::Sender<DcbResult<()>>);

    pub fn start_blocking_io_thread(mvcc: Arc<Mvcc>, mut io_rx: mpsc::UnboundedReceiver<IoJob>) {
        // Just a simple, infinite blocking loop on this dedicated OS thread
        while let Some((prepared, response_tx)) = io_rx.blocking_recv() {
            let result = do_blocking_commit(&mvcc, prepared);

            // Send the result back to the writer thread pipeline barrier
            let _ = response_tx.send(result);
        }
    }

    fn do_blocking_commit(mvcc: &Mvcc, prepared: PreparedCommit) -> DcbResult<()> {
        // 1. Write Data Pages sequentially
        for (page_id, page_data) in prepared.pages_to_write {
            mvcc.rw.pager.write_page_data(page_id, &page_data)?;
        }

        // 2. Fsync Data (Barrier)
        mvcc.fsync()?;

        // 3. Write Header
        let (header_id, header_data) = prepared.header_to_write;
        mvcc.rw.pager.write_page_data(header_id, &header_data)?;

        // 4. Fsync Header (Barrier)
        mvcc.fsync()?;

        Ok(())
    }
}
