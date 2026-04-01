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

#[derive(Parser, Debug)]
#[command(version, about = "Measure UmaDB append throughput vs number of concurrent writers")]
struct Args {
    /// Test duration per writer count in seconds
    #[arg(long, default_value = "60")]
    duration: u64,

    /// Maximum number of writers to test (tests powers of 2: 1, 2, 4, ..., max)
    #[arg(long, default_value = "1024")]
    max_writers: usize,

    /// Output JSON file path
    #[arg(long, default_value = "target/throughput_vs_writers.json")]
    output: String,

    /// Number of events to pre-populate the database with
    #[arg(long, default_value = "1000000")]
    initial_events: usize,
}

#[derive(Serialize, Debug)]
struct TestResult {
    writers: usize,
    duration_secs: f64,
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

async fn run_throughput_test(
    writers: usize,
    duration_secs: u64,
    addr_http: &str,
) -> TestResult {
    println!(
        "Testing {} writers for {} seconds...",
        writers, duration_secs
    );

    // Create clients (we're already in a tokio runtime from #[tokio::main])
    let mut clients: Vec<Arc<AsyncUmaDbClient>> = Vec::with_capacity(writers);
    for _ in 0..writers {
        let c = UmaDbClient::new(addr_http.to_string())
            .connect_async()
            .await
            .expect("connect client");
        clients.push(Arc::new(c));
    }

    let test_duration = Duration::from_secs(duration_secs);
    let start = Instant::now();
    let end_time = start + test_duration;

    // Spawn continuous append tasks, one per writer
    let handles: Vec<_> = (0..writers)
        .map(|i| {
            let client = clients[i].clone();
            tokio::spawn(async move {
                let mut count = 0u64;
                while Instant::now() < end_time {
                    let event = DcbEvent {
                        event_type: "bench-append".to_string(),
                        data: format!("data-{}", count).into_bytes(),
                        tags: vec!["append".to_string()],
                        uuid: None,
                    };
                    match client.append(vec![event], None, None).await {
                        Ok(_) => count += 1,
                        Err(e) => {
                            eprintln!("Writer {} error: {}", i, e);
                            break;
                        }
                    }
                }
                count
            })
        })
        .collect();

    // Wait for all tasks to complete and sum the total events
    let total_events: u64 = {
        let results = join_all(handles).await;
        results.into_iter().map(|r| r.unwrap_or(0)).sum()
    };

    let elapsed = start.elapsed();
    let duration_secs = elapsed.as_secs_f64();
    let throughput = total_events as f64 / duration_secs;

    println!(
        "  {} writers: {} events in {:.2}s = {:.2} events/sec",
        writers, total_events, duration_secs, throughput
    );

    TestResult {
        writers,
        duration_secs,
        total_events,
        throughput_events_per_sec: throughput,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Generate writer counts: 1, 2, 4, 8, ..., up to max_writers
    let writer_counts: Vec<usize> = (0..)
        .map(|i| 1usize << i) // 2^i
        .take_while(|&n| n <= args.max_writers)
        .collect();

    println!("Testing writer counts: {:?}", writer_counts);
    println!("Test duration: {} seconds per count", args.duration);

    let mut results = Vec::new();

    for &writers in &writer_counts {
        // Initialize DB for each test
        let (_tmp_dir, db_path) = init_db_with_events(args.initial_events);

        // Find a free localhost port
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind to ephemeral port");
        let port = listener.local_addr().unwrap().port();
        let addr = format!("127.0.0.1:{}", port);
        drop(listener);

        let addr_http = format!("http://{}", addr);

        // Start the gRPC server
        let server_handle = start_bench_server(db_path, addr.clone());

        // Run the test
        let result = run_throughput_test(writers, args.duration, &addr_http).await;
        results.push(result);

        // Shutdown server
        server_handle.shutdown();

        // Small delay between tests
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Write results to JSON file
    let output_path = Path::new(&args.output);
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let json = serde_json::to_string_pretty(&results)?;
    fs::write(&args.output, json)?;

    println!("\nResults written to: {}", args.output);
    println!("\nSummary:");
    for result in &results {
        println!(
            "  {} writers: {:.2} events/sec",
            result.writers, result.throughput_events_per_sec
        );
    }

    Ok(())
}
