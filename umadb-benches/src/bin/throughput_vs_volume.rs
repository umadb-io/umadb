use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;
use umadb_client::{AsyncUmaDBClient, UmaDBClient};
use umadb_dcb::{DCBEvent, DCBEventStoreAsync};
use uuid::Uuid;

use clap::{CommandFactory, FromArgMatches, Parser};
use tokio::pin;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    /// Server address, e.g. 127.0.0.1:50051
    #[arg(long = "db-url")]
    db_url: String,

    /// Path to log file
    #[arg(long = "log-path")]
    log_path: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Args::command();
    cmd = cmd.about(format!(
        "Throughput vs Volume Script ({}-{})",
        std::env::consts::OS,
        std::env::consts::ARCH
    ));

    let matches = cmd.get_matches();
    let args = Args::from_arg_matches(&matches)?; // <-- FromArgMatches trait

    // Connect to UmaDB instance at 127.0.0.1:50051
    let client = Arc::new(
        UmaDBClient::new(args.db_url.clone())
            .connect_async()
            .await
            .expect("Failed to connect to UmaDB"),
    );

    println!("Connected to UmaDB at {}", args.db_url.clone());

    // Open log file for appending measurements
    let log_path = args.log_path;
    println!("Logging measurements to {}", log_path.clone());
    let log_file = Arc::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)?,
    );

    let shutdown = tokio::signal::ctrl_c();
    pin!(shutdown); // <-- this makes it Unpin-safe
    println!("Press Ctrl-C to stop…");

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                println!("Shutting down…");
                break;
            }

            _ = do_some_work(client.clone(), log_file.clone()) => {
                // continue
            }
        }
    }
    Ok(())
}

async fn do_some_work(
    client: Arc<AsyncUmaDBClient>,
    mut log_file: Arc<File>,
) -> std::io::Result<()> {
    let mut batch_count = 0u64;
    // Append 9800 batches of 10 events each
    for _ in 0..99 {
        let tag = format!("tag-{}", Uuid::new_v4());
        let events: Vec<DCBEvent> = (0..1000)
            .map(|_| DCBEvent {
                event_type: "batch-type".to_string(),
                data: "batch-data".to_string().into_bytes(),
                tags: vec![tag.clone()],
                uuid: None,
            })
            .collect();

        client
            .append(events, None, None)
            .await
            .expect("Failed to append batch");

        batch_count += 1;
    }

    // After 99 batches, measure the rate of appending 1000 individual events
    let start = Instant::now();
    for _ in 0..1000 {
        let tag = format!("tag-{}", Uuid::new_v4());
        let event = DCBEvent {
            event_type: "batch-type".to_string(),
            data: "batch-data".to_string().into_bytes(),
            tags: vec![tag.clone()],
            uuid: None,
        };

        client
            .append(vec![event], None, None)
            .await
            .expect("Failed to append individual event");
    }
    let elapsed = start.elapsed();

    // Get the current head position (sequence position)
    let head_position = client
        .head()
        .await
        .expect("Failed to get head position")
        .unwrap_or(0);

    // Calculate rate (events per second)
    let rate = 1000.0 / elapsed.as_secs_f64();

    // Log the measurement: sequence_position rate
    // println!("Position: {}, rate: {}", head_position, rate);
    writeln!(log_file, "{} {}", head_position, rate)?;
    log_file.flush()?;

    println!(
        "Batch count: {}, Head position: {}, Rate: {:.2} events/sec",
        batch_count, head_position, rate
    );
    Ok(())
}

// Commands on AWS i4g.large:
//   lsblk -o NAME,TYPE,SIZE,MOUNTPOINT,FSTYPE
//   sudo mkfs.xfs -f /dev/nvme1n1
//   sudo mkdir -p /var/lib/umadb
//   echo "/dev/nvme1n1 /var/lib/umadb xfs noatime,nodiratime 0 2" | sudo tee -a /etc/fstab
//   sudo mount /var/lib/umadb
//   df -h | grep umadb
//   lsblk -o NAME,TYPE,SIZE,MOUNTPOINT,FSTYPE
//   sudo chown ec2-user:ec2-user /var/lib/umadb
//   sudo dnf install fio -y
//   fio --name=test --filename=/var/lib/umadb/test-fio --bs=4k --rw=write --ioengine=libaio --iodepth=32 --size=1G --direct=1
//   ./umadb --listen 127.0.0.1:50051 --db-path /var/lib/umadb/ &
//   ./throughput_vs_volume --db-url http://127.0.0.1:50051 --log-path ./umadb-throughput.log
