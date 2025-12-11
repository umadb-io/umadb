# UmaDB Python Client

Official Python client for [UmaDB](https://umadb.io), built using Rust bindings via PyO3 and Maturin.

## Installation

### From PyPI

First, create and activate a virtual environment. Then:

```bash
pip install umadb
```

## Usage

### Basic Example

```python
from umadb import Client, Event

# Connect to UmaDB server
client = Client("http://localhost:50051")

# Create and append events
event = Event(
    event_type="UserCreated",
    data=b"user data",
    tags=["user", "creation"],
)
position = client.append([event])
print(f"Event appended at position: {position}")

# Read events
events = client.read()
for seq_event in events:
    print(f"Position {seq_event.position}: {seq_event.event.event_type}")

print(f"Last known position: {events.head()}")
```

### Using Queries

```python
from umadb import Client, Query, QueryItem

client = Client("http://localhost:50051")

# Create a query to filter events
query_item = QueryItem(
    types=["UserCreated", "UserUpdated"],
    tags=["user"]
)
query = Query(items=[query_item])

# Read filtered events
events = client.read(query=query)
for seq_event in events:
    print(f"Position {seq_event.position}: {seq_event.event.event_type}")

print(f"Last known position: {events.head()}")
```

### Using Append Conditions

```python
from umadb import Client, Event, Query, QueryItem, AppendCondition

client = Client("http://localhost:50051")

# Create a condition that prevents duplicate events
fail_query_item = QueryItem(types=["UserCreated"], tags=["user:123"])
fail_query = Query(items=[fail_query_item])
condition = AppendCondition(fail_if_events_match=fail_query)

# This will fail if matching events exist
event = Event(
    event_type="UserCreated",
    data=b"user data",
    tags=["user:123"],
)
try:
    position = client.append([event], condition=condition)
except ValueError as e:
    print(f"Append failed: {e}")
```

### TLS/SSL Connection

```python
from umadb import Client

# Connect with TLS using CA certificate
client = Client(
    url="https://secure-server:50051",
    ca_path="/path/to/ca.pem"
)
```

### API key

```python
from umadb import Client

# Connect with API key
client = Client(
    url="https://secure-server:50051",
    ca_path="/path/to/ca.pem",
    api_key="umadb:example-api-key-4f7c2b1d9e5f4a038c1a"
)
```

### Reading with Options

```python
from umadb import Client

client = Client("http://localhost:50051", batch_size=100)

# Read backwards from position
events = client.read(start=100, backwards=True, limit=10)

# Subscribe to new events (streaming)
events = client.read(subscribe=True)
```

## API Reference

### Client

```python
Client(url: str, ca_path: str | None = None, batch_size: int | None = None, api_key: str | None = None)
```

Creates a new UmaDB client connection.

**Methods:**
- `read(query=None, start=None, backwards=False, limit=None, subscribe=False)`: Read events from the store
- `head()`: Get the current head position (returns `int | None`)
- `append(events, condition=None)`: Append events to the store (returns position as `int`)

### Event

```python
Event(event_type: str, data: bytes, tags: list[str] | None = None, uuid: str | None = None)
```

Represents an event in the event store.

**Properties:**
- `event_type`: Type of the event (string)
- `data`: Binary data (bytes)
- `tags`: List of tags (list of strings)
- `uuid`: Optional UUID (string)

### SequencedEvent

Represents an event with its position in the sequence.

**Properties:**
- `event`: The Event object
- `position`: Position in the sequence (int)

### Query

```python
Query(items: list[QueryItem] | None = None)
```

A query for filtering events.

### QueryItem

```python
QueryItem(types: list[str] | None = None, tags: list[str] | None = None)
```

A query item specifying event types and tags to match.

### AppendCondition

```python
AppendCondition(fail_if_events_match: Query, after: int | None = None)
```

Condition for conditional appends.

## Development

### Building

First, create and activate a virtual environment. Then:

```bash
# Install maturin
pip install maturin

# Build and install in development mode
maturin develop -m ./umadb-python/Cargo.toml

# Or build a wheel
maturin build -m ./umadb-python/Cargo.toml --release
```


### Testing

The Python bindings can be tested by running a UmaDB server and executing Python code against it.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../LICENSE-APACHE))
- MIT license ([LICENSE-MIT](../LICENSE-MIT))

at your option.
