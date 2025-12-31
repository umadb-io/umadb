// cargo bench --bench grpc_read_flame --features flamegraphs

use umadb_client::UmaDBClient;

fn main() -> std::io::Result<()> {
    use pprof::ProfilerGuard;
    use std::fs::File;
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;
    use tokio::runtime::Builder as RtBuilder;
    use tokio::sync::oneshot;
    use umadb_client::AsyncUmaDBClient;
    use umadb_core::db::UmaDB;
    use umadb_dcb::{DCBEvent, DCBEventStoreAsync, DCBEventStoreSync};
    use umadb_server::start_server;

    fn init_db_with_events(num_events: usize) -> (tempfile::TempDir, String) {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().to_str().unwrap().to_string();

        // Populate the database using the local EventStore (fast, in-process)
        let store = UmaDB::new(&path).expect("create event store");

        // Prepare events and append in moderate batches to avoid huge allocations
        let batch_size = 1000usize.min(num_events.max(1));
        let mut remaining = num_events;
        while remaining > 0 {
            let current = remaining.min(batch_size);
            let mut events = Vec::with_capacity(current);
            for i in 0..current {
                let ev = DCBEvent {
                    event_type: "bench".to_string(),
                    data: format!("event-{}", i).into_bytes(),
                    tags: vec!["tag1".to_string()],
                    uuid: None,
                };
                events.push(ev);
            }
            store.append(events, None, None).expect("append to store");
            remaining -= current;
        }

        (dir, path)
    }

    fn profile_concurrent_reads<F>(name: &str, mut work: F) -> std::io::Result<()>
    where
        F: FnMut(),
    {
        let guard = ProfilerGuard::new(100).expect("ProfilerGuard");
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            work();
        }

        if let Ok(report) = guard.report().build() {
            let out_dir = std::path::Path::new("../../target/flamegraphs");
            std::fs::create_dir_all(out_dir)?;
            let path = out_dir.join(format!("{name}.svg"));
            let mut opts = pprof::flamegraph::Options::default();
            let file = File::create(&path)?;
            report
                .flamegraph_with_options(file, &mut opts)
                .expect("write flamegraph");
            println!("✅ Wrote {}", path.canonicalize()?.display());
        }
        Ok(())
    }

    fn generate_flamegraphs() -> std::io::Result<()> {
        const TOTAL_EVENTS: u32 = 10_000;
        const READ_BATCH_SIZE: u32 = 1000;

        // Initialize DB and server with some events
        let (_tmp_dir, db_path) = init_db_with_events(TOTAL_EVENTS as usize);

        // Find a free localhost port
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind to ephemeral port");
        let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
        drop(listener);

        let addr_http = format!("http://{}", addr);

        // Start the gRPC server in a background thread, with shutdown channel
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let db_path_clone = db_path.clone();
        let addr_clone = addr.clone();

        let server_thread = thread::spawn(move || {
            let server_threads = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            let rt = RtBuilder::new_multi_thread()
                .worker_threads(server_threads)
                .max_blocking_threads(2048)
                .enable_all()
                .build()
                .expect("build tokio rt for server");
            rt.block_on(async move {
                start_server(db_path_clone, &addr_clone, shutdown_rx)
                    .await
                    .expect("start server");
            });
        });

        // Wait until the server is actually accepting connections
        {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                if std::net::TcpStream::connect(&addr).is_ok() {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    panic!("server did not start listening within timeout at {}", addr);
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }

        // Profile for different concurrency levels
        for &threads in &[1usize, 16, 128, 1024] {
            // Build a Tokio runtime and multiple persistent clients
            let rt = RtBuilder::new_multi_thread()
                .worker_threads(threads + 10)
                .enable_all()
                .build()
                .expect("build tokio rt (client)");

            // Establish independent gRPC connections upfront
            let mut clients: Vec<Arc<AsyncUmaDBClient>> = Vec::with_capacity(threads);
            for _ in 0..threads {
                let c = rt
                    .block_on(
                        UmaDBClient::new(addr_http.clone())
                            .batch_size(READ_BATCH_SIZE)
                            .connect_async(),
                    )
                    .expect("connect client");
                clients.push(Arc::new(c));
            }
            let clients = Arc::new(clients);

            profile_concurrent_reads(&format!("grpc_read_concurrent_{threads}"), || {
                let clients_clone = clients.clone();
                rt.block_on(async {
                    // Spawn `threads` concurrent read futures and await them all
                    use futures::future::join_all;
                    let futs = (0..threads).map(|i| {
                        let client = clients_clone[i].clone();
                        async move {
                            let mut resp = client
                                .read(None, None, false, Some(TOTAL_EVENTS), false)
                                .await
                                .expect("start read response");
                            let mut count = 0usize;
                            loop {
                                let batch = resp.next_batch().await.expect("next_batch ok");
                                if batch.is_empty() {
                                    break;
                                }
                                count += batch.len();
                            }
                            assert_eq!(
                                count, TOTAL_EVENTS as usize,
                                "expected to read all preloaded events"
                            );
                        }
                    });
                    let _ = join_all(futs).await;
                });
            })?;
        }

        // Shutdown server
        let _ = shutdown_tx.send(());
        let _ = server_thread.join();

        Ok(())
    }

    generate_flamegraphs()
}
