# UmaDB Python Client

Official [Python client](https://umadb.io/python-client.html) for [UmaDB](https://umadb.io) using [Rust-powered bindings](https://umadb.io/rust-client.html) via
[PyO3](https://pyo3.rs/).

This package is adapted into the Python [eventsourcing](https://eventsourcing.readthedocs.io/en/stable/topics/dcb.html)
library via the [eventsourcing-umadb](https://pypi.org/project/eventsourcing-umadb/) package.


## Installation

### From PyPI

To install with pip, create and activate a virtual environment, then:

```bash
pip install umadb
```

## Connecting to UmaDB

Use the `Client` class as the main entry point for connecting to an UmaDB server.

```python
class Client(
    url: str,
    ca_path: str | None = None,
    api_key: str | None = None,
    batch_size: int | None = None,
):
    ...
```
### Parameters

| Name      | Type        | Description                                                                                                                                         |
|-----------|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `url`     | `str`       | Required connection string. If the argument starts with `https` or `grpcs`, a secure TLS channel is created; otherwise an insecure channel is used. |
| `ca_path` | `str\|None` | Optional path to a PEM-encoded root/CA certificate for TLS connections (useful for self-signed servers).                                            |
| `api_key` | `str\|None` | Optional API key used for authenticating with the server.                                                                                           |
| `batch_size` | `int\|None` | Optional hint for how many events to buffer per batch when reading. The server may cap this; a sensible default is used if unset..                  |

### Connection Examples

```python
from umadb import Client

# Insecure (no TLS)
client = Client("http://localhost:50051")

# Secure with TLS (system CAs)
client = Client("https://example.com:50051")

# Secure with TLS using self-signed CA
client = Client(
    url="https://localhost:50051",
    ca_path="server.pem",
)

# TLS + API key
client = Client(
    url="https://example.com:50051",
    ca_path="server.pem",
    api_key="umadb:example-api-key-4f7c2b1d9e5f4a038c1a",
)

# TLS + API key + batch size hint
client = Client(
    url="https://example.com:50051",
    ca_path="server.pem",
    api_key="umadb:example-api-key-4f7c2b1d9e5f4a038c1a",
    batch_size=1000,
)
```

## Appending Events

The `Client.append()` method writes new events to an UmaDB server.

```python
def append(
    events: list[Event],
    condition: AppendCondition | None = None,
    tracking_info: TrackingInfo | None = None,
) -> int:
    ...
```

The `Client.append()` method can be used to append new `Event` instances to UmaDB atomically, with an optional append
condition, and optional tracking information. Events are written in order.

Conditional appends with event UUIDs are idempotent. The server does not enforce uniqueness of events IDs.

### Parameters

| Name            | Type                    | Description                                                                        |
|-----------------|-------------------------|------------------------------------------------------------------------------------|
| `events`        | `list[Event]`           | The list of events to append. Each includes an event type, tags, and data payload. |
| `condition`     | `AppendCondition\|None` | Optional append condition to ensure no conflicting writes occur.                   |
| `tracking_info` | `TrackingInfo\|None`    | Optional tracking information – for event-processing components only.              |

### Return Value

Returns the sequence number (`int`) of the last successfully appended event from this operation. This
value can be used to wait for downstream event-processing components in a CQRS system to become up-to-date.

### Example

```python
import uuid

from umadb import AppendCondition, Client, Event, IntegrityError, Query, QueryItem

client = Client("http://localhost:50051")

# Define a consistency boundary (same query you use while reading)
cb = Query(items=[QueryItem(types=["example"], tags=["tag1", "tag2"])])

# Read to build decision model
read_resp = client.read(query=cb)
for r in read_resp:
    print(f"Existing event at {r.position}: {r.event}")

last_known = read_resp.head()
print("Last known position:", last_known)

# Produce a new event with a UUID (for idempotent retries)
ev = Event(
    event_type="example",
    tags=["tag1", "tag2"],
    data=b"Hello, world!",
    uuid=str(uuid.uuid4()),
)

# Append with an optimistic condition: fail if conflicting events exist after last_known
cond = AppendCondition(fail_if_events_match=cb, after=last_known)
position1 = client.append([ev], condition=cond)
print("Appended at:", position1)

# Conflicting append should raise an error (e.g. ValueError)
try:
    client.append(
        [
            Event(
                event_type="example",
                tags=["tag1", "tag2"],
                data=b"Hello, world!",
                uuid=str(uuid.uuid4()),
            )
        ],
        condition=cond,
    )
except IntegrityError as e:
    print("Conflicting event was rejected:", e)

# Idempotent retry with same event UUID and condition should return same commit position
position2 = client.append([ev], condition=cond)
assert position1 == position2
print("Idempotent retry returned position:", position2)
```

## Reading Events

The `Client.read()` method returns recorded events from an UmaDB server.

```python
def read(
    query: Query | None = None,
    start: int | None = None,
    backwards: bool = False,
    limit: int | None = None,
    subscribe: bool = False,
) -> ReadResponse:
    ...
```
The `Client.read()` method can be used both for constructing decision models in a domain layer, and for projecting events into
materialized views in CQRS. An optional `Query` can be provided to select by tags and types.

### Parameters

| Name        | Type          | Description                                                                                                                                                 |
|-------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `query`     | `Query\|None` | Optional structured query to filter events (by tags, event types, etc).                                                                                     |
| `start`     | `int\|None`   | Read events *from* this sequence number. Only events with positions greater than or equal will be returned (or less than or equal if `backwards` is `True`. |
| `backwards` | `bool`        | If `True` events will be read backwards, either from the given position or from the last recorded event.                                                    |
| `limit`     | `int\|None`   | Optional cap on the number of events to retrieve.                                                                                                           |
| `subscribe` | `bool`        | If `True`, keeps the stream open to deliver future events as they arrive.                                                                                   |

### Return Value

Returns an iterable "read response" instance from which `SequencedEvent` instances, and the most relevant "last known" sequence number, can be obtained.

### Example

```python
from umadb import Client, Query, QueryItem

client = Client("http://localhost:50051")

# Filter by type(s) and tag(s)
q = Query(items=[QueryItem(types=["example"], tags=["tag1", "tag2"])])

resp = client.read(
    query=q, start=None, backwards=False, limit=None, subscribe=False
)
for item in resp:
    print(f"Got event at position {item.position}: {item.event}")

last_known = resp.head()
print("Last known position:", last_known)

# Subscribe to new events
subscription = client.read(subscribe=True)
for se in subscription:
    print("New event:", se.position, se.event)
    # Break for demo purposes
    break
```

## Getting Head Position

The `Client.head()` method returns the position of the last event recorded in an UmaDB server.

```python
def head(self) -> int | None: ...
```

The `Client.head()` method can be used both for constructing decision models in a domain layer, and for projecting events into
materialized views in CQRS. An optional [`DCBQuery`](#query) can be provided to select by tags and types.

### Return Value

Returns the position (`u64`) of the last recorded event in the event store, or `None` if no events have been recorded yet.


## Getting Tracking Info

The `Client.get_tracking_info()` method returns the last recorded position of an upstream event.

```python
def get_tracking_info(self, source: str) -> int | None: ...
```

The `Client.get_tracking_info()` method can be used when starting or resuming an
event-processing component. The event-processing component will start by requesting new events from the upstream
sequence after this position. The position of an upstream event that has been processed successfully can be recorded
atomically when appending new events generated by processing that event.

### Parameters

| Name        | Type  | Description                                                                           |
|-------------|-------|---------------------------------------------------------------------------------------|
| `source`    | `str` | The name of the upstream source.                                                      |

Returns the position (`int`) of an event in an upstream sequence, or `None` if no such event has been processed yet.

## Data Structures

## Event

An `Event` represents a single event either to be appended or already stored in the event log.

| Field        | Type        | Description                                                   |
|--------------|-------------|---------------------------------------------------------------|
| `event_type` | `str`       | The event’s logical type (e.g. `"UserRegistered"`).           |
| `tags`       | `list<str>` | Tags assigned to the event (used for filtering and indexing). |
| `data`       | `bytes`     | Binary payload associated with the event.                     |
| `uuid`       | `str\|None` | Unique event ID.                                              |

Idempotent support for append operations is activated by setting a UUID on appended events.

Include in:
* Append requests when writing new events to the store.

Included in:
* `SequencedEvent` objects when the server responds to read requests.

Matched by:
* `QueryItem` during `read()` and `append()` operations.


## Append Condition

An `AppendCondition` causes an append request to fail if events match its `Query`, optionally after
a sequence number.

| Field                  | Type        | Description                   |
|------------------------|-------------|-------------------------------|
| `fail_if_events_match` | `Query`     | Query for conflicting events. |
| `after`                | `int\|None` | Sequence number.              |

Include in:
* Append requests to define optimistic concurrent control.

To implement a consistency boundary, command handlers can use the same `Query` used when
reading events as the value of `fail_if_events_match`, and the "head" sequence
number received from the read response as the value of `after`.


## Tracking Info

A `TrackingInfo` instance indicates the source and position of an upstream event that has been processed.

| Field      | Type  | Description      |
|------------|-------|------------------|
| `source`   | `str` | Source name.     |
| `position` | `int` | Sequence number. |

Include in:
* Append requests to specify tracking information; for event-processing components only.

To implement exactly-once semantics in event-processing components, pull events from an upstream
source after the last recorded upstream position, then record the upstream positions
of upstream events along with new state that results from processing those events.
By processing event sequentially in this way, each event will be processed at least once. And by
recording tracking information along with new state, the new state will be recorded at most once.
The combination of "at least once" processing and "at most once" recording will give "exactly once"
semantics from the point of view of consumers of the recorded state.


## Query

A `Query` defines criteria for selecting events in the event store.

| Field   | Type              | Description                                |
|---------|-------------------|--------------------------------------------|
| `items` | `list[QueryItem]` | A list of selection criteria (logical OR). |

An `Event` is selected if any `QueryItem` matches or the `items` field is empty.

Include in:
* Read requests to select events returned by the server.
* An `AppendCondition` to select conflicting events.


## Query Item

A `QueryItem` defines a criterion for matching events.

| Field   | Type        | Description                       |
|---------|-------------|-----------------------------------|
| `types` | `list[str]` | List of event types (logical OR). |
| `tags`  | `list[str]` | List of tags (logical AND).       |

A `QueryItem` will match an `Event if:
* one of its `types` matches the `Event.event_type` or its `types` field is empty; AND
* all of its `tags` match one of the `Event.tags` or its `tags` field is empty.


## Sequenced Event

A `SequencedEvent` represents a recorded `Event` along with its assigned sequence number.

| Field      | Type    | Description          |
|------------|---------|----------------------|
| `position` | `int`   | The sequence number. |
| `event`    | `Event` | The recorded event.  |

Included in:
* Read responses when the server responds to read requests.


## Error Handling

The Python client raises Python exceptions on error:

- Integrity/condition failure: `IntegrityError`
- Transport/connection errors: `TransportError`
- Authentication failures: `AuthenticationError`
- Other internal errors: `RuntimeError` or `OSError`

Your application should catch these as appropriate.

## Complete Example

```python
import uuid

from umadb import AppendCondition, Client, Event, IntegrityError, Query, QueryItem

# Connect to the gRPC server (TLS + API key)
client = Client(
    url="http://localhost:50051",
)

# Define a consistency boundary
cb = Query(items=[QueryItem(types=["example"], tags=["tag1", "tag2"])])

# Read events for a decision model
read_response = client.read(query=cb)
for result in read_response:
    print(f"Got event at position {result.position}: {result.event}")

# Remember the last-known position
last_known_position = read_response.head()
print("Last known position is:", last_known_position)

# Create an event with a UUID to enable idempotent append
event = Event(
    event_type="example",
    tags=["tag1", "tag2"],
    data=b"Hello, world!",
    uuid=str(uuid.uuid4()),
)

# Append event within the consistency boundary
condition = AppendCondition(fail_if_events_match=cb, after=last_known_position)
commit_position1 = client.append([event], condition=condition)
print("Appended event at position:", commit_position1)

# Append conflicting event — expect an error
try:
    conflicting_event = Event(
        event_type="example",
        tags=["tag1", "tag2"],
        data=b"Hello, world!",
        uuid=str(uuid.uuid4()),  # different UUID
    )
    client.append([conflicting_event], condition=condition)
except IntegrityError as e:
    print("Conflicting event was rejected:", e)

# Idempotent retry — same event ID and condition
print("Retrying to append event at position:", last_known_position)
commit_position2 = client.append([event], condition=condition)
assert commit_position1 == commit_position2
print("Append returned same commit position:", commit_position2)

# Subscribe to all events for a projection
subscription = client.read(subscribe=True)
for ev in subscription:
    print(f"Processing event at {ev.position}: {ev.event}")
    if ev.position == commit_position2:
        print("Projection has processed new event!")
        break
```

## Example with Tracking

```python
import uuid

from umadb import (
    AppendCondition,
    Client,
    Event,
    IntegrityError,
    Query,
    QueryItem,
    TrackingInfo,
)

# Connect to the gRPC server (TLS + API key)
client = Client(
    url="http://localhost:50051",
)

# Get last processed upstream event position
last_processed_position = client.get_tracking_info("upstream")

# Pull next unprocessed upstream event...
next_upstream_event_position = 1 + (last_processed_position or 0)

# Construct tracking information from next unprocessed event
tracking_info = TrackingInfo("upstream", next_upstream_event_position)

# Define a consistency boundary
cb = Query(items=[QueryItem(types=["example"], tags=["tag1", "tag2"])])

# Read events for a decision model
read_response = client.read(query=cb)
for result in read_response:
    print(f"Got event at position {result.position}: {result.event}")

# Remember the last-known position
last_known_position = read_response.head()
print("Last known position is:", last_known_position)

# Create an event with a UUID to enable idempotent append
event = Event(
    event_type="example",
    tags=["tag1", "tag2"],
    data=b"Hello, world!",
    uuid=str(uuid.uuid4()),
)

# Append event within the consistency boundary
condition = AppendCondition(fail_if_events_match=cb, after=last_known_position)
commit_position1 = client.append(
    [event], condition=condition, tracking_info=tracking_info
)
print("Appended event at position:", commit_position1)

# Idempotent retry — same event ID and condition
print("Retrying to append event at position:", last_known_position)
commit_position2 = client.append(
    [event], condition=condition, tracking_info=tracking_info
)
assert commit_position1 == commit_position2
print("Append returned same commit position:", commit_position2)

# Check tracking information
assert tracking_info.position == client.get_tracking_info("upstream")

# Unconditional append with conflicting tracking information — expect an error
try:
    conflicting_event = Event(
        event_type="example",
        tags=["tag1", "tag2"],
        data=b"Hello, world!",
        uuid=str(uuid.uuid4()),  # different UUID
    )
    client.append([conflicting_event], condition=None, tracking_info=tracking_info)
except IntegrityError as e:
    print("Conflicting event was rejected:", e)
```

## Notes

- Python client is synchronous and blocking; if you need async integration, run client calls in a thread pool or use an async worker that offloads to threads.
- Event data is binary (`bytes`). Use a consistent serialization (e.g., JSON serialized to UTF-8 bytes, protobuf, msgpack) for your domain.
- API keys must match the server configuration.
- For TLS with self-signed certs, pass `ca_path` with your root/CA certificate.


## Testing

The Python bindings can be tested by running a UmaDB server and executing Python code against it.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../LICENSE-APACHE))
- MIT license ([LICENSE-MIT](../LICENSE-MIT))

at your option.
