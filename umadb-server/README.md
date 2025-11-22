# umadb-server

gRPC server implementation for UmaDB event store.

## Overview

`umadb-server` provides the gRPC server implementation that exposes UmaDB's event store functionality over the network. It combines the storage engine (`umadb-core`), protocol definitions (`umadb-proto`), and DCB API (`umadb-dcb`) into a complete server library.

## Features

- **gRPC service implementation** for the UmaDB API
- **Request batching** - concurrent append requests are automatically grouped for higher throughput
- **Streaming** for real-time event subscriptions with catch-up
- **Health checks** via gRPC health checking protocol
- **Async runtime** built on Tokio for high-performance concurrent operations

## Usage

This crate is typically used by the `umadb` binary to run the server, but can also be embedded in custom applications:

```rust
use tokio::signal;
use tokio::sync::oneshot;
use umadb_server::start_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        let _ = tx.send(());
    });
    start_server("/path/to/db-file", "127.0.0.1:50051", rx).await?;
    Ok(())
}
```

## Request Batching

The server automatically batches concurrent append requests to amortize disk I/O costs while maintaining per-request atomicity and isolation. This significantly improves throughput under concurrent load.

## Streaming Subscriptions

The server supports real-time event subscriptions that:
- Start from any position in the event store
- Seamlessly catch up on historical events
- Continuously deliver new events as they are appended
- Handle backpressure gracefully

## Part of UmaDB

This crate is part of [UmaDB](https://github.com/umadb-io/umadb), a high-performance open-source event store built for Dynamic Consistency Boundaries.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](../LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
