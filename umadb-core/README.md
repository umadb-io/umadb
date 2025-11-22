# umadb-core

Core event store implementation for UmaDB.

## Overview

`umadb-core` provides the low-level storage engine implementation for UmaDB, a high-performance event store built for [Dynamic Consistency Boundaries](https://dcb.events).

This crate handles the critical performance and durability aspects of the event store:

- **Append-only event storage** with monotonically increasing gapless positions
- **Multi-Version Concurrency Control (MVCC)** for high-performance non-blocking reads and writes
- **Copy-on-write page management** for atomic commits and crash safety
- **Efficient space reclamation** of unreachable page versions
- **Tag indexing** for fast, precise event filtering
- **Optimistic concurrency control** to prevent write conflicts
- **Full durability** before acknowledgements

## Architecture

UmaDB uses a custom storage engine with:

- **Copy-on-write B+ tree pages** for MVCC without blocking
- **Atomic header page commits** after all data is durably written
- **Page reuse tracking** to reclaim space from old versions
- **Memory-mapped file I/O** for high-performance reads
- **File I/O and fsync** for durability guarantees

## Usage

This crate is typically used through higher-level UmaDB components like `umadb-server` rather than directly. It implements the traits defined in `umadb-dcb`:

```rust
use umadb_core::db::UmaDB;
```

## Performance

The MVCC design enables:

- **Non-blocking reads** - readers see consistent snapshots without blocking writers
- **High-throughput writes** - batched append requests amortize disk I/O
- **Concurrent access** - multiple readers and writers without locks
- **Efficient paging** - copy-on-write enables atomic updates with minimal copying

## Part of UmaDB

This crate is part of [UmaDB](https://github.com/umadb-io/umadb), a high-performance open-source event store built for Dynamic Consistency Boundaries.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](../LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
