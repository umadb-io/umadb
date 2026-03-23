use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use futures::future::join_all;
use std::hint::black_box;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::runtime::Builder as RtBuilder;
use umadb_benches::server_helper::start_bench_server;
use umadb_client::{AsyncUmaDBClient, UmaDBClient};
use umadb_core::db::UmaDB;
use umadb_dcb::{DCBEvent, DCBEventStoreAsync, DCBEventStoreSync};

fn get_max_threads() -> Option<usize> {
    std::env::var("MAX_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
}

fn get_test_duration() -> u64 {
    std::env::var("TEST_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60)
}

fn get_sample_size() -> usize {
    std::env::var("SAMPLE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10)
        .max(10) // Criterion requires at least 10
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

pub fn grpc_append_throughput_vs_writers_benchmark(c: &mut Criterion) {
    let all_threads = [1usize, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024];
    let max_threads = get_max_threads();
    let thread_counts: Vec<usize> = all_threads
        .iter()
        .copied()
        .filter(|&t| max_threads.map_or(true, |max| t <= max))
        .collect();

    for &threads in &thread_counts {
        // Initialize DB and server with 1_000_000 events (same as append_bench)
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
            "grpc_append_throughput_vs_writers{}",
            if server_handle.use_docker { "_with_docker" } else { "" }
        );
        let sample_size = get_sample_size();
        let test_duration_secs = get_test_duration();

        let mut group = c.benchmark_group(&group_name);
        group.sample_size(sample_size);
        group.warm_up_time(Duration::from_secs(1)); // Minimal warmup
        // Set measurement time to accommodate all samples (must be > 0)
        let total_measurement_time = (test_duration_secs * sample_size as u64).max(1) + 5;
        group.measurement_time(Duration::from_secs(total_measurement_time));

        // Number of events appended per iteration per client
        // let events_per_iter = 1usize;

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
            // Use iter_custom to run for configured duration and measure throughput
            b.iter_custom(|_iters| {
                let test_duration = Duration::from_secs(test_duration_secs);
                let start = Instant::now();
                let end_time = start + test_duration;

                // Spawn continuous append tasks, one per thread
                let handles: Vec<_> = (0..threads)
                    .map(|i| {
                        let client = clients[i].clone();
                        let end_time = end_time.clone();
                        rt.spawn(async move {
                            let mut count = 0u64;
                            while Instant::now() < end_time {
                                let event = DCBEvent {
                                    event_type: "bench-append".to_string(),
                                    data: format!("data-{}", count).into_bytes(),
                                    tags: vec!["append".to_string()],
                                    uuid: None,
                                };
                                match black_box(client.append(vec![event], None, None).await) {
                                    Ok(_) => count += 1,
                                    Err(_) => break,
                                }
                            }
                            count
                        })
                    })
                    .collect();

                // Wait for all tasks to complete and sum the total events
                let total_events: u64 = rt.block_on(async {
                    let results = join_all(handles).await;
                    results.into_iter().map(|r| r.unwrap_or(0)).sum()
                });

                let elapsed = start.elapsed();
                // Return the average time per event
                elapsed / total_events.max(1) as u32
            });
        });
        // Shutdown server
        server_handle.shutdown();
        group.finish();
    }
}

criterion_group!(benches, grpc_append_throughput_vs_writers_benchmark);
criterion_main!(benches);
