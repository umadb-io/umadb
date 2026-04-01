use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::runtime::Builder as RtBuilder;
use umadb_benches::server_helper::start_bench_server;
use umadb_client::{AsyncUmaDbClient, UmaDbClient};
use umadb_core::db::UmaDb;
use umadb_dcb::{DcbEvent, DcbEventStoreAsync, DcbEventStoreSync};

fn get_max_threads() -> Option<usize> {
    std::env::var("MAX_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
}

fn is_throttled() -> bool {
    std::env::var("BENCH_READ_THROTTLED")
        .ok()
        .map(|s| !s.is_empty())
        .unwrap_or(false)
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

pub fn grpc_read_benchmark(c: &mut Criterion) {
    use criterion::{BenchmarkId, Throughput};
    use futures::future::join_all;
    use std::net::TcpListener;
    const EVENTS_IN_DB: u32 = 1_000_000;
    const EVENTS_TO_READ: u32 = 100_000;
    const READ_BATCH_SIZE: u32 = 5000;
    let throttled = is_throttled();

    // Initialize DB and server with some events
    let (_tmp_dir, db_path) = init_db_with_events(EVENTS_IN_DB as usize);

    // Find a free localhost port
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind to ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let addr = format!("127.0.0.1:{}", port);
    drop(listener);

    let addr_http = format!("http://{}", addr);

    // Start the gRPC server in a background thread
    let server_handle = start_bench_server(db_path, addr.clone());

    let group_name = format!(
        "grpc_read_{}{}",
        if throttled { "throttled" } else { "unthrottled" },
        if server_handle.use_docker { "_with_docker" } else { "" }
    );
    let mut group = c.benchmark_group(&group_name);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    let all_threads = [1usize, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024];
    let max_threads = get_max_threads();
    let thread_counts: Vec<usize> = all_threads
        .iter()
        .copied()
        .filter(|&t| max_threads.map_or(true, |max| t <= max))
        .collect();

    for &threads in &thread_counts {
        // Report throughput as the total across all runtime worker threads
        group.throughput(Throughput::Elements(
            (EVENTS_TO_READ as u64) * (threads as u64),
        ));

        // Build a Tokio runtime and multiple persistent clients (one per concurrent reader)
        let rt = RtBuilder::new_multi_thread()
            .worker_threads(threads + 10)
            .enable_all()
            .build()
            .expect("build tokio rt (client)");

        // Establish independent gRPC connections upfront to avoid contention on a single HTTP/2 channel
        let mut clients: Vec<Arc<AsyncUmaDbClient>> = Vec::with_capacity(threads);
        for _ in 0..threads {
            let c = rt
                .block_on(
                    UmaDbClient::new(addr_http.clone())
                        .batch_size(READ_BATCH_SIZE)
                        .connect_async(),
                )
                .expect("connect client");
            clients.push(Arc::new(c));
        }
        let clients = Arc::new(clients);

        group.bench_function(BenchmarkId::from_parameter(threads), move |b| {
            let clients = clients.clone();
            b.iter(|| {
                rt.block_on(async {
                    // Spawn `threads` concurrent read futures and await them all
                    let futs = (0..threads).map(|i| {
                        let client = clients[i].clone();
                        async move {
                            let mut resp = client
                                .read(None, None, false, Some(EVENTS_TO_READ), false)
                                .await
                                .expect("start read response");
                            let mut count = 0usize;
                            let loop_start = Instant::now();
                            loop {
                                let batch = resp.next_batch().await.expect("next_batch ok");
                                if batch.is_empty() {
                                    break;
                                }
                                for item in batch.into_iter() {
                                    let _evt = std::hint::black_box(item);
                                    count += 1;
                                }
                                if throttled {
                                    let target_rate_per_second: u64 = 10_010;
                                    // Sleep only if we're ahead of the targeted total elapsed time for the
                                    // number of items processed so far, avoiding per-iteration stalls.
                                    let desired_elapsed = Duration::from_secs_f64(
                                        (count as f64) / (target_rate_per_second as f64),
                                    );
                                    let elapsed = loop_start.elapsed();
                                    if desired_elapsed > elapsed {
                                        tokio::time::sleep(desired_elapsed - elapsed).await;
                                    }
                                }
                            }
                            assert_eq!(
                                count, EVENTS_TO_READ as usize,
                                "expected to read all preloaded events"
                            );
                        }
                    });
                    let _ = join_all(futs).await;
                });
            });
        });
    }

    group.finish();

    // Shutdown server
    server_handle.shutdown();
}

criterion_group!(benches, grpc_read_benchmark);
criterion_main!(benches);
