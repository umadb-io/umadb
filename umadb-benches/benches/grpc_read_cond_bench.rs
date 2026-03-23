use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::future::join_all;
use std::hint::black_box;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::runtime::Builder as RtBuilder;
use umadb_benches::server_helper::start_bench_server;
use umadb_client::{AsyncUmaDBClient, UmaDBClient};
use umadb_core::db::UmaDB;
use umadb_dcb::{DcbEvent, DcbEventStoreAsync, DcbEventStoreSync, DcbQuery, DcbQueryItem};

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
            let ev = DcbEvent {
                event_type: "bench".to_string(),
                data: format!("event-{}", j).into_bytes(),
                tags: vec![format!("tag-{}", j).to_string()],
                uuid: None,
            };
            events.push(ev);
        }
        store.append(events, None, None).expect("append to store");
    }

    (dir, path)
}

pub fn grpc_read_cond_benchmark(c: &mut Criterion) {
    let query = DcbQuery::new().item(DcbQueryItem::new().tags(["tag-500"]));

    // Initialize DB and server with some events
    let (_tmp_dir, db_path) = init_db_with_events();

    // Find a free localhost port
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind to ephemeral port");
    let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    drop(listener);

    let addr_http = format!("http://{}", addr);

    // Start the gRPC server in a background thread
    let server_handle = start_bench_server(db_path, addr.clone());

    let group_name = format!(
        "grpc_read_cond{}",
        if server_handle.use_docker { "_with_docker" } else { "" }
    );

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
    server_handle.shutdown();
}

criterion_group!(benches, grpc_read_cond_benchmark);
criterion_main!(benches);
