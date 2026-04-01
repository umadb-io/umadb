use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::future::join_all;
use std::hint::black_box;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::runtime::Builder as RtBuilder;
use umadb_benches::server_helper::start_bench_server;
use umadb_client::{AsyncUmaDbClient, UmaDbClient};
use umadb_core::db::UmaDb;
use umadb_dcb::{DcbEvent, DcbEventStoreAsync, DcbEventStoreSync};

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
    let store = UmaDb::new(&path).expect("create event store");

    // Prepare events and append in moderate batches to avoid huge allocations
    let batch_size = 1000usize.min(num_events.max(1));
    let mut remaining = num_events;
    while remaining > 0 {
        let current = remaining.min(batch_size);
        let mut events = Vec::with_capacity(current);
        for i in 0..current {
            let ev = DcbEvent {
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
        let port = listener.local_addr().unwrap().port();
        let addr = format!("127.0.0.1:{}", port);
        drop(listener);

        let addr_http = format!("http://{}", addr);

        // Start the gRPC server in a background thread
        let server_handle = start_bench_server(db_path, addr.clone());

        let group_name = format!(
            "grpc_append_{}_per_request{}",
            events_per_request,
            if server_handle.use_docker { "_with_docker" } else { "" }
        );
        let mut group = c.benchmark_group(&group_name);
        group.sample_size(50);
        group.measurement_time(Duration::from_secs(20));

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
        let mut clients: Vec<Arc<AsyncUmaDbClient>> = Vec::with_capacity(threads);
        for _ in 0..threads {
            let c = rt
                .block_on(UmaDbClient::new(addr_http.clone()).connect_async())
                .expect("connect client");
            clients.push(Arc::new(c));
        }
        let clients = Arc::new(clients);

        group.bench_function(BenchmarkId::from_parameter(threads), move |b| {
            let clients = clients.clone();
            b.iter(|| {
                // Build the batch of events per iteration (per task)
                let events: Vec<DcbEvent> = (0..events_per_request)
                    .map(|i| DcbEvent {
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
        server_handle.shutdown();
        group.finish();
    }
}

criterion_group!(benches, grpc_append_benchmark);
criterion_main!(benches);
