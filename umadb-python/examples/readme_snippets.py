#!/usr/bin/env python3
"""
Manually copied snippets from README file.
"""


def read_example() -> None:
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


def append_example() -> None:
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


def complete_example() -> None:
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


def complete_example_with_tracking() -> None:
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


if __name__ == "__main__":
    read_example()
    append_example()
    complete_example()
    complete_example_with_tracking()
