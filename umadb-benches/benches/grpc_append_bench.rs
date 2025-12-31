use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::future::join_all;
use std::hint::black_box;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;
use tokio::runtime::Builder as RtBuilder;
use tokio::sync::oneshot;
use umadb_client::{AsyncUmaDBClient, UmaDBClient};
use umadb_core::db::UmaDB;
use umadb_dcb::{DCBEvent, DCBEventStoreAsync, DCBEventStoreSync};
use umadb_server::start_server;

fn get_events_per_request() -> usize {
    std::env::var("EVENTS_PER_REQUEST")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10)
}

fn get_max_threads() -> Option<usize> {
    std::env::var("MAX_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
}

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
                event_type: "bench-init".to_string(),
                data: format!("init-{}", i).into_bytes(),
                tags: vec!["init".to_string()],
                uuid: None,
            };
            events.push(ev);
        }
        store.append(events, None, None).expect("append to store");
        remaining -= current;
    }

    (dir, path)
}

pub fn grpc_append_benchmark(c: &mut Criterion) {
    // for &threads in &[1usize, 2, 4] {
    let events_per_request = get_events_per_request();
    let group_name = format!("grpc_append_{}_per_request", events_per_request);
    let mut group = c.benchmark_group(&group_name);
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(20));

    let all_threads = [1usize, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024];
    let max_threads = get_max_threads();
    let thread_counts: Vec<usize> = all_threads
        .iter()
        .copied()
        .filter(|&t| max_threads.map_or(true, |max| t <= max))
        .collect();

    for &threads in &thread_counts {
        // Initialize DB and server with 10_000 events (as requested)
        let initial_events = 1_000_000usize;
        let (_tmp_dir, db_path) = init_db_with_events(initial_events);

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
                .enable_all()
                .build()
                .expect("build tokio rt for server");
            rt.block_on(async move {
                start_server(db_path_clone, &addr_clone, shutdown_rx)
                    .await
                    .expect("start server");
            });
        });

        // Wait until the server is actually accepting connections (avoid race with startup)
        {
            use std::time::Duration;
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

        // Number of events appended per iteration per client
        let events_per_request = get_events_per_request();

        // Report throughput as the total across all runtime worker threads (informational)
        group.throughput(Throughput::Elements(
            (events_per_request as u64) * (threads as u64),
        ));

        // Build a Tokio runtime and multiple persistent clients (one per concurrent writer)
        let rt = RtBuilder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()
            .expect("build tokio rt (client)");
        let mut clients: Vec<Arc<AsyncUmaDBClient>> = Vec::with_capacity(threads);
        for _ in 0..threads {
            let c = rt
                .block_on(UmaDBClient::new(addr_http.clone()).connect_async())
                .expect("connect client");
            clients.push(Arc::new(c));
        }
        let clients = Arc::new(clients);

        group.bench_function(BenchmarkId::from_parameter(threads), move |b| {
            let clients = clients.clone();
            b.iter(|| {
                // Build the batch of events per iteration (per task)
                let events: Vec<DCBEvent> = (0..events_per_request)
                    .map(|i| DCBEvent {
                        event_type: "bench-append".to_string(),
                        data: format!("data-{i}").into_bytes(),
                        tags: vec!["append".to_string()],
                        // tags: vec![format!("append-{i}").to_string()],
                        uuid: None,
                    })
                    .collect();

                rt.block_on(async {
                    // Spawn `threads` concurrent append futures and await them all
                    let futs = (0..threads).map(|i| {
                        let client = clients[i].clone();
                        let evs = events.clone();
                        async move {
                            let _ = black_box(
                                client
                                    .append(black_box(evs), None, None)
                                    .await
                                    .expect("append events"),
                            );
                        }
                    });
                    let _ = join_all(futs).await;
                });
            });
        });
        // Shutdown server
        let _ = shutdown_tx.send(());
        let _ = server_thread.join();
    }

    group.finish();
}

criterion_group!(benches, grpc_append_benchmark);
criterion_main!(benches);
