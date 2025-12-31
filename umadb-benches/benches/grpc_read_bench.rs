use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::future::join_all;
use std::hint::black_box;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::runtime::Builder as RtBuilder;
use tokio::sync::oneshot;
use umadb_client::{AsyncUmaDBClient, UmaDBClient};
use umadb_core::db::UmaDB;
use umadb_dcb::{DCBEvent, DCBEventStoreAsync, DCBEventStoreSync};
use umadb_server::start_server;

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

pub fn grpc_read_benchmark(c: &mut Criterion) {
    const EVENTS_IN_DB: u32 = 1_000_000;
    const EVENTS_TO_READ: u32 = 100_000;
    const READ_BATCH_SIZE: u32 = 5000;
    let throttled = is_throttled();

    // Initialize DB and server with some events
    let (_tmp_dir, db_path) = init_db_with_events(EVENTS_IN_DB as usize);

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

    let group_name = if throttled {
        "grpc_read_throttled"
    } else {
        "grpc_read_unthrottled"
    };
    let mut group = c.benchmark_group(group_name);
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
                                    let _evt = black_box(item);
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
    let _ = shutdown_tx.send(());
    let _ = server_thread.join();
}

criterion_group!(benches, grpc_read_benchmark);
criterion_main!(benches);
