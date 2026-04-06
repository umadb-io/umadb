use clap::Parser;
use futures::future::join_all;
use serde::Serialize;
use std::fs;
use std::net::TcpListener;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use umadb_benches::server_helper::start_bench_server;
use umadb_client::{AsyncUmaDbClient, UmaDbClient};
use umadb_core::db::UmaDb;
use umadb_dcb::{DcbEvent, DcbEventStoreAsync, DcbEventStoreSync};
use tokio::sync::Barrier;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(version, about = "Measure sustained UmaDB append throughput")]
struct Args {
    /// Warm-up duration in seconds
    #[arg(long, default_value = "10")]
    warmup: u64,

    /// Measurement duration in seconds
    #[arg(long, default_value = "30")]
    duration: u64,

    /// Maximum number of writers to test (tests powers of 2: 1, 2, 4, ..., max)
    #[arg(long, default_value = "1024")]
    max_writers: usize,

    /// Output JSON file path
    #[arg(long, default_value = "")]
    output: String,

    /// Number of events to pre-populate the database with
    #[arg(long, default_value = "0")]
    // #[arg(long, default_value = "1000000")]
    initial_events: usize,

    /// Number of events per request
    #[arg(long, default_value = "10")]
    events_per_request: usize,
}

#[derive(Serialize, Debug)]
struct TestResult {
    writers: usize,
    warmup_secs: u64,
    duration_secs: u64,
    total_events: u64,
    throughput_events_per_sec: f64,
}

fn init_db_with_events(num_events: usize) -> (tempfile::TempDir, String) {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().to_str().unwrap().to_string();

    println!("Initializing database with {} events...", num_events);
    let store = UmaDb::new(&path).expect("create event store");

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

    println!("Database initialized at {}", path);
    (dir, path)
}

async fn run_sustained_throughput_test(
    writers: usize,
    warmup_secs: u64,
    duration_secs: u64,
    events_per_request: usize,
    addr_http: &str,
) -> TestResult {
    println!(
        "Testing {} writers (warmup: {}s, duration: {}s)...",
        writers, warmup_secs, duration_secs
    );

    let mut clients: Vec<Arc<AsyncUmaDbClient>> = Vec::with_capacity(writers);
    for _ in 0..writers {
        let c = UmaDbClient::new(addr_http.to_string())
            .connect_async()
            .await
            .expect("connect client");
        clients.push(Arc::new(c));
    }

    let start_barrier = Arc::new(Barrier::new(writers + 1));
    let stop_barrier = Arc::new(Barrier::new(writers + 1));
    let stop_signal = Arc::new(tokio::sync::watch::channel(false));

    let mut handles = Vec::new();

    let size = 2;  // Match UmaDB append-continuous benchmark

    for i in 0..writers {
        let client = clients[i].clone();
        let start_barrier = start_barrier.clone();
        let stop_barrier = stop_barrier.clone();
        let stop_rx = stop_signal.0.subscribe();
        let evs_per_req = events_per_request;
        let event_type = "test".to_string();
        let payload = vec![0u8; size];
        let handle = tokio::spawn(async move {

            // Wait for start signal
            start_barrier.wait().await;

            let mut count = 0u64;

            // Tight loop with minimal overhead
            let mut stream_name = format!("stream-");
            // let mut stream_name = format!("stream-{}-", Uuid::new_v4());
            let stream_len = 100000;
            let mut stream_position = 0;

            loop {
                if *stop_rx.borrow() {
                    break;
                }
                let events: Vec<DcbEvent> = (0..evs_per_req)
                    .map(|j| DcbEvent {
                        // event_type: format!("{}-{}", event_type.clone(), stream_position),
                        event_type: format!("{}", event_type.clone()),
                        data: payload.clone(),
                        tags: vec![stream_name.clone()],
                        uuid: None,
                    })
                    .collect();

                match client.append(events, None, None).await {
                    Ok(_) => count += evs_per_req as u64,
                    Err(e) => {
                        eprintln!("Writer {} error: {}", i, e);
                        break;
                    }
                }
                let _ = stop_rx.has_changed(); // best effort to pick up stop signal

                // Increment stream position, maybe reset and change name.
                // stream_position += 1;
                // if stream_position == stream_len {
                //     stream_name = format!("stream-{}-", Uuid::new_v4());
                //     stream_position = 0;
                // }

            }

            // Wait for stop signal acknowledgement
            stop_barrier.wait().await;
            count
        });
        handles.push(handle);
    }

    // Start all workers
    start_barrier.wait().await;
    
    // Warm-up
    tokio::time::sleep(Duration::from_secs(warmup_secs)).await;

    // Measurement period start
    let initial_head = clients[0].head().await.expect("get head").unwrap_or(0);
    let start_time = Instant::now();
    
    tokio::time::sleep(Duration::from_secs(duration_secs)).await;
    
    // Measurement period end
    let final_head = clients[0].head().await.expect("get head").unwrap_or(0);
    let elapsed = start_time.elapsed();

    // Signal workers to stop
    let _ = stop_signal.0.send(true);
    stop_barrier.wait().await;

    // Wait for all handles to finish
    let _ = join_all(handles).await;

    let total_events = final_head.saturating_sub(initial_head);
    let throughput = total_events as f64 / elapsed.as_secs_f64();

    println!(
        "  {} writers: {} events in {:.2}s = {:.2} events/sec",
        writers, total_events, elapsed.as_secs_f64(), throughput
    );

    TestResult {
        writers,
        warmup_secs,
        duration_secs,
        total_events,
        throughput_events_per_sec: throughput,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let writer_counts: Vec<usize> = (0..)
        .map(|i| 1usize << i)
        .take_while(|&n| n <= args.max_writers)
        .collect();

    println!("Sustained throughput benchmark");
    println!("Writer counts: {:?}", writer_counts);
    println!("Warmup: {}s, Duration: {}s, Events per request: {}", args.warmup, args.duration, args.events_per_request);

    let mut results = Vec::new();

    for &writers in &writer_counts {
        let (_tmp_dir, db_path) = init_db_with_events(args.initial_events);

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind to ephemeral port");
        let port = listener.local_addr().unwrap().port();
        let addr = format!("127.0.0.1:{}", port);
        drop(listener);

        let addr_http = format!("http://{}", addr);
        let server_handle = start_bench_server(db_path, addr.clone());

        let result = run_sustained_throughput_test(
            writers,
            args.warmup,
            args.duration,
            args.events_per_request,
            &addr_http,
        ).await;
        results.push(result);

        server_handle.shutdown();
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let output = if args.output.is_empty() {
        if umadb_benches::server_helper::use_docker() {
            format!("target/grpc_append_sustained_{}_per_request_with_docker.json", args.events_per_request)
        } else {
            format!("target/grpc_append_sustained_{}_per_request.json", args.events_per_request)
        }
    } else {
        args.output.clone()
    };

    let output_path = Path::new(&output);
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let json = serde_json::to_string_pretty(&results)?;
    fs::write(&output, json)?;

    println!("\nResults written to: {}", output);
    
    Ok(())
}
