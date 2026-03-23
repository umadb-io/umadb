use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::future::join_all;
use std::hint::black_box;
use std::net::TcpListener;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tempfile::tempdir;
use tokio::runtime::Builder as RtBuilder;
use umadb_benches::server_helper::start_bench_server;
use umadb_client::{AsyncUmaDBClient, UmaDBClient};
use umadb_core::db::UmaDB;
use umadb_dcb::{DcbEvent, DcbEventStoreAsync, DcbEventStoreSync};

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

pub fn grpc_read_with_writers_benchmark(c: &mut Criterion) {
    const EVENTS_IN_DB: u32 = 1_000_000;
    const EVENTS_TO_READ: u32 = 100_000;
    const READ_BATCH_SIZE: u32 = 5000;
    const WRITER_COUNT: usize = 4;
    const WRITER_EVENTS_PER_APPEND: usize = 1; // small continuous appends

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

    // Start background writers that continuously append while reads are benchmarked
    let writers_running = Arc::new(AtomicBool::new(true));
    let writers_rt = RtBuilder::new_multi_thread()
        .worker_threads(WRITER_COUNT)
        .enable_all()
        .build()
        .expect("build tokio rt (writers)");

    // Create independent writer clients
    let mut writer_clients: Vec<Arc<AsyncUmaDBClient>> = Vec::with_capacity(WRITER_COUNT);
    for _ in 0..WRITER_COUNT {
        let c = writers_rt
            .block_on(UmaDBClient::new(addr_http.clone()).connect_async())
            .expect("connect writer client");
        writer_clients.push(Arc::new(c));
    }

    // Prebuild a tiny event batch for each append
    let writer_batch: Vec<DcbEvent> = (0..WRITER_EVENTS_PER_APPEND)
        .map(|i| DcbEvent {
            event_type: "writer".to_string(),
            data: format!("w-{}", i).into_bytes(),
            tags: vec!["w".to_string()],
            uuid: None,
        })
        .collect();
    let writer_batch = Arc::new(writer_batch);

    // Spawn continuous writer tasks
    let mut writer_handles = Vec::with_capacity(WRITER_COUNT);
    for i in 0..WRITER_COUNT {
        let client = writer_clients[i].clone();
        let running = writers_running.clone();
        let batch = writer_batch.clone();
        let handle = writers_rt.spawn(async move {
            while running.load(Ordering::Relaxed) {
                // ignore result intentionally; if an error occurs, just break
                let _ = client.append(batch.as_ref().clone(), None, None).await;
                // Yield a bit to avoid starving the system
                tokio::task::yield_now().await;
            }
        });
        writer_handles.push(handle);
    }

    let all_threads = [1usize, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024];
    let max_threads = get_max_threads();
    let thread_counts: Vec<usize> = all_threads
        .iter()
        .copied()
        .filter(|&t| max_threads.map_or(true, |max| t <= max))
        .collect();

    let group_name = format!(
        "grpc_read_4writers{}",
        if server_handle.use_docker { "_with_docker" } else { "" }
    );
    let mut group = c.benchmark_group(&group_name);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

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
                            loop {
                                let batch = resp.next_batch().await.expect("next_batch ok");
                                if batch.is_empty() {
                                    break;
                                }
                                for item in batch.into_iter() {
                                    let _evt = black_box(item);
                                    count += 1;
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

    // Stop writers and shutdown server
    writers_running.store(false, Ordering::Relaxed);
    // Wait briefly to let writer tasks exit
    writers_rt.block_on(async {
        for h in writer_handles {
            let _ = h.await;
        }
    });

    server_handle.shutdown();
}

criterion_group!(benches, grpc_read_with_writers_benchmark);
criterion_main!(benches);
