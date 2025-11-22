# umadb-dcb

Core types and traits for implementing the Dynamic Consistency Boundaries (DCB) API in UmaDB.

## Overview

`umadb-dcb` provides the fundamental types and trait definitions for the [Dynamic Consistency Boundaries](https://dcb.events) specification. This crate defines the core abstractions that enable flexible, query-driven consistency rules in event stores.

## What is Dynamic Consistency Boundaries?

Dynamic Consistency Boundaries (DCB) is an independent specification created by Bastian Waidelich, Sara Pellegrini, and Paul Grimshaw. It enables event-driven architectures where consistency rules can adapt dynamically to business needs, rather than being hardwired into the database.

## Key Types

- **Event types**: Core event structures and metadata
- **Query types**: Types for querying and filtering events
- **Append conditions**: Types for controlling when events can be appended
- **Trait definitions**: Core traits for implementing DCB-compliant event stores

## Usage

This crate is typically used as a dependency when building DCB-compliant event stores or working with UmaDB:

```rust
use umadb_dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBEventStoreAsync, DCBEventStoreSync, DCBQuery,
    DCBReadResponseAsync, DCBReadResponseSync, DCBResult, DCBSequencedEvent,
};
```

## Part of UmaDB

This crate is part of [UmaDB](https://github.com/umadb-io/umadb), a high-performance open-source event store built for Dynamic Consistency Boundaries.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](../LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
