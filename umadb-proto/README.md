# umadb-proto

Protocol buffer definitions and gRPC service for UmaDB event store.

## Overview

`umadb-proto` provides the Protocol Buffers (protobuf) definitions and generated gRPC service code for communicating with UmaDB. This crate enables network communication between UmaDB clients and servers.

## Features

- **gRPC service definitions** for UmaDB operations
- **Protocol buffer messages** for events, queries, and append conditions
- **Type conversions** between protobuf and `umadb-dcb` types

## Service Operations

The UmaDB gRPC service provides:

- **Read** - Query and retrieve events with optional filtering
- **Append** - Write new events with optional consistency conditions

## Usage

This crate is used by both `umadb-server` (to implement the gRPC service) and `umadb-client` (to communicate with the server).

Clients send requests and convert gRPC status details to DCB errors.

```rust
use umadb_proto::{
    AppendRequest, ReadRequest, UmaDbServiceClient, dcb_error_from_status,
};
```

Servers convert DCB errors to gRPC status details and send responses.

```rust
use umadb_proto::{
    AppendResponse, ReadResponse, UmaDbServiceServer, status_from_dcb_error,
};
```

## Protocol Buffers

The protobuf definitions are automatically compiled from the `umadb.proto` file during the build process using `tonic-prost-build`.

## Part of UmaDB

This crate is part of [UmaDB](https://github.com/umadb-io/umadb), a high-performance open-source event store built for Dynamic Consistency Boundaries.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](../LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
