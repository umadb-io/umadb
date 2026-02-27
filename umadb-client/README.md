# umadb-client

Rust gRPC client library for UmaDB event store.

## Overview

`umadb-client` provides a high-level, idiomatic Rust client for connecting to and interacting with UmaDB servers. It wraps the gRPC protocol defined in `umadb-proto` with a convenient async API.

## Features

- **Simple async API** for reading and writing events
- **Type-safe operations** using `umadb-dcb` types
- **Streaming subscriptions** for real-time event delivery
- **Connection management** with automatic reconnection
- **Built on Tokio and Tonic** for high-performance async I/O

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
umadb-client = "0.1"
```

Basic example:

```rust
use futures::StreamExt;
use umadb_client::UmaDBClient;
use umadb_dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBEventStoreAsync, DCBQuery, DCBQueryItem,
};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the gRPC server
    let url = "http://localhost:50051".to_string();
    let client = UmaDBClient::new(url).connect_async().await?;

    // Define a consistency boundary
    let boundary = DCBQuery::new().item(
        DCBQueryItem::new()
            .types(["example"])
            .tags(["tag1", "tag2"]),
    );

    // Read events for a decision model
    let mut read_response = client
        .read(Some(boundary.clone()), None, false, None, false)
        .await?;

    // Build decision model
    while let Some(result) = read_response.next().await {
        match result {
            Ok(event) => {
                println!(
                    "Got event at position {}: {:?}",
                    event.position, event.event
                );
            }
            Err(status) => panic!("gRPC stream error: {}", status),
        }
    }

    // Remember the last-known position
    let last_known_position = read_response.head().await?;
    println!("Last known position is: {:?}", last_known_position);

    // Produce new event
    let event = DCBEvent::default()
        .event_type("example")
        .tags(["tag1", "tag2"])
        .data(b"Hello, world!")
        .uuid(Uuid::new_v4());

    // Append event in consistency boundary
    let condition = DCBAppendCondition::new(boundary.clone()).after(last_known_position);
    let position1 = client
        .append(vec![event.clone()], Some(condition.clone()))
        .await?;

    println!("Appended event at position: {}", position1);

    // Append conflicting event - expect an error
    let conflicting_event = DCBEvent::default()
        .event_type("example")
        .tags(["tag1", "tag2"])
        .data(b"Hello, world!")
        .uuid(Uuid::new_v4()); // different UUID

    let conflicting_result = client
        .append(vec![conflicting_event.clone()], Some(condition.clone()))
        .await;

    // Expect an integrity error
    match conflicting_result {
        Err(DCBError::IntegrityError(integrity_error)) => {
            println!("Conflicting event was rejected: {:?}", integrity_error);
        }
        other => panic!("Expected IntegrityError, got {:?}", other),
    }

    // Appending with identical events IDs and append conditions is idempotent.
    println!(
        "Retrying to append event at position: {:?}",
        last_known_position
    );
    let position2 = client
        .append(vec![event.clone()], Some(condition.clone()))
        .await?;

    if position1 == position2 {
        println!("Append method returned same commit position: {}", position2);
    } else {
        panic!("Expected idempotent retry!")
    }

    // Subscribe to all events for a projection
    let mut subscription = client.sub(None, None).await?;

    // Build an up-to-date view
    while let Some(result) = subscription.next().await {
        match result {
            Ok(ev) => {
                println!("Processing event at {}: {:?}", ev.position, ev.event);
                if ev.position == position2 {
                    println!("Projection has processed new event!");
                    break;
                }
            }
            Err(status) => panic!("gRPC stream error: {}", status),
        }
    }
    Ok(())
}
```


## Part of UmaDB

This crate is part of [UmaDB](https://github.com/umadb-io/umadb), a high-performance open-source event store built for Dynamic Consistency Boundaries.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](../LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
