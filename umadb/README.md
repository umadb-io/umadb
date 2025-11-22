# UmaDB - Dynamic Consistency Boundary Event Store

**UmaDB** is a specialist open-source event store built for **dynamic consistency boundaries**.

This crate provides the `umadb` binary, a gRPC server for running UmaDB as a standalone service.

## Installation

### Via Cargo

Install the `umadb` binary using Cargo:

```bash
cargo install umadb
```

After installation, run the server:

```bash
umadb --listen 127.0.0.1:50051 --db-path ./data
```

### Pre-built Binaries

Pre-built binaries for various platforms are available on GitHub:

- [GitHub Releases](https://github.com/umadb-io/umadb/releases)

Download the appropriate binary for your platform and architecture.

### Docker Images

UmaDB Docker images are available from multiple registries:

**Docker Hub:**
```bash
docker pull umadb/umadb:latest
docker run -p 50051:50051 -v /path/to/data:/data umadb/umadb:latest
```

**GitHub Container Registry (GHCR):**
```bash
docker pull ghcr.io/umadb-io/umadb:latest
docker run -p 50051:50051 -v /path/to/data:/data ghcr.io/umadb-io/umadb:latest
```

## CLI Usage

The `umadb` command-line interface provides options to configure the server:

```
umadb [OPTIONS]
```

### Options

- `--db-path` - Path to the database file or directory
- `--listen` - Server bind address (e.g. `127.0.0.1:50051`)
- `--tls-cert` - Optional file path to TLS server certificate (also via UMADB_TLS_CERT)
- `--tls-key` - Optional file path to TLS server private key (also via UMADB_TLS_KEY)
- `-h, --help` - Print help
- `-V, --version` - Print version

### Examples

Run CLI listening to `0.0.0.0:50051` using `./data` to store events.

```bash
umadb --listen 0.0.0.0:50051 --db-path ./data
```

Run Docker image, publishing port `50051` and persisting data to a local volume:

```bash
docker run -p 50051:50051 -v $(pwd)/umadb-data:/data umadb/umadb:latest
```

## Quick Start

Once the server is running, you can connect using client libraries:

### Python Client

```bash
pip install umadb
```

```python
from umadb import Client, Event

# Connect to UmaDB server
client = Client("http://localhost:50051")

# Create and append events
event = Event(
    event_type="UserCreated",
    data=b"user data",
    tags=["user:123"],
)
position = client.append([event])
print(f"Event appended at position: {position}")

# Read events
events = client.read()
for seq in events:
    print(f"Position {seq.position}: {seq.event.event_type}")
```

### Rust Client

Add the client dependency to your `Cargo.toml`:

```toml
[dependencies]
umadb-client = "0.1"
```

Use the client in your code:

```rust
use umadb_client::UmaDBClient;
use umadb_dcb::{DCBEvent, DCBEventStoreAsync};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = UmaDBClient::new("http://localhost:50051".to_string())
        .connect_async()
        .await?;

    let events = vec![DCBEvent {
        event_type: "UserCreated".to_string(),
        data: b"user data".to_vec(),
        tags: vec!["user:123".to_string()],
        uuid: None,
    }];

    let position = client.append(events, None).await?;
    println!("Event appended at position: {}", position);

    Ok(())
}
```

## Features

- **High-performance concurrency** with non-blocking reads and writes
- **Optimistic concurrency control** to prevent simultaneous write conflicts
- **Dynamic business-rule enforcement** via query-driven append conditions
- **Real-time subscriptions** with seamless catch-up and continuous delivery
- **gRPC API** for cross-language compatibility

## Documentation

For more information about UmaDB and Dynamic Consistency Boundaries:

- [UmaDB Repository](https://github.com/umadb-io/umadb)
- [DCB Specification](https://dcb.events/specification/)
- [DCB Events Website](https://dcb.events)

## License

This project is licensed under either of:

- MIT License ([LICENSE-MIT](https://github.com/umadb-io/umadb/blob/main/LICENSE-MIT))
- Apache License 2.0 ([LICENSE-APACHE](https://github.com/umadb-io/umadb/blob/main/LICENSE-APACHE))

at your option.
