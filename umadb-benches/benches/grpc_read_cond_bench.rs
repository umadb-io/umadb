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
use umadb_dcb::{DCBEvent, DCBEventStoreAsync, DCBEventStoreSync, DCBQuery, DCBQueryItem};
use umadb_server::start_server;

const EVENTS_PER_TAG: u32 = 10;
const NUM_TAGS: u32 = 100000;

fn get_max_threads() -> Option<usize> {
    std::env::var("MAX_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
}

fn init_db_with_events() -> (tempfile::TempDir, String) {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().to_str().unwrap().to_string();

    // Populate the database using the local EventStore (fast, in-process)
    let store = UmaDB::new(&path).expect("create event store");

    // Prepare events and append in moderate batches to avoid huge allocations
    for _ in 0..EVENTS_PER_TAG {
        let mut events = Vec::with_capacity(NUM_TAGS as usize);
        for j in 0..NUM_TAGS {
            let ev = DCBEvent {
                event_type: "bench".to_string(),
                data: format!("event-{}", j).into_bytes(),
                tags: vec![format!("tag-{}", j).to_string()],
                uuid: None,
            };
            events.push(ev);
        }
        store.append(events, None).expect("append to store");
    }

    (dir, path)
}

pub fn grpc_read_cond_benchmark(c: &mut Criterion) {
    let query = DCBQuery::new().item(DCBQueryItem::new().tags(["tag-500"]));

    // Initialize DB and server with some events
    let (_tmp_dir, db_path) = init_db_with_events();

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

    let mut group = c.benchmark_group("grpc_read_cond");
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
            (EVENTS_PER_TAG as u64) * (threads as u64),
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
                        .batch_size(EVENTS_PER_TAG)
                        .connect_async(),
                )
                .expect("connect client");
            clients.push(Arc::new(c));
        }
        let clients = Arc::new(clients);
        let query = query.clone();

        group.bench_function(BenchmarkId::from_parameter(threads), move |b| {
            let clients = clients.clone();
            let query = query.clone();
            b.iter(|| {
                rt.block_on(async {
                    // Spawn `threads` concurrent read futures and await them all
                    let futs = (0..threads).map(|i| {
                        let client = clients[i].clone();
                        let query_clone = query.clone();
                        async move {
                            let mut resp = client
                                .read(Some(query_clone.clone()), None, false, None, false)
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
                                // if count % 1000 == 0 {
                                //     tokio::time::sleep(Duration::from_millis(90)).await;
                                // }
                            }
                            assert_eq!(
                                count, EVENTS_PER_TAG as usize,
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

criterion_group!(benches, grpc_read_cond_benchmark);
criterion_main!(benches);
