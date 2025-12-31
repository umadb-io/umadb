#!/usr/bin/env python3
"""
Basic usage example for UmaDB Python client.

This example demonstrates:
1. Connecting to a UmaDB server
2. Appending events
3. Reading events
4. Using queries to filter events
5. Using append conditions
"""
import os
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


def main() -> None:
    # Connect to UmaDB server (make sure server is running)
    api_key = os.environ.get("UMADB_API_KEY")
    ca_path = os.environ.get("UMADB_TLS_CERT")
    if ca_path:
        url = "https://localhost:50051"
    else:
        url = "http://localhost:50051"
    print("Connecting to UmaDB server...")
    client = Client(url, ca_path=ca_path, api_key=api_key, batch_size=2)

    # Get current head position
    head = client.head()
    print(f"Current head position: {head}")

    # Create and append some events
    print("\nAppending events...")
    events = [
        Event(
            event_type="UserCreated",
            data=b'{"user_id": "123", "name": "Alice"}',
            tags=["user", "user:123"],
        ),
        Event(
            event_type="UserUpdated",
            data=b'{"user_id": "123", "email": "alice@example.com"}',
            tags=["user", "user:123"],
        ),
        Event(
            event_type="OrderCreated",
            data=b'{"order_id": "456", "user_id": "123"}',
            tags=["order", "user:123", "order:456"],
        ),
    ]

    position = client.append(events)
    print(f"Events appended, last position: {position}")

    # Read all events
    print("\nReading all events...")
    all_events = client.read()
    print(f"  - read response head is now: {all_events.head()}")
    for seq_event in all_events:
        print(
            f"  Position {seq_event.position}: {seq_event.event.event_type} - tags: {seq_event.event.tags}"
        )
        print(f"  - read response head is now: {all_events.head()}")
    print(f"  Last known position is: {all_events.head()}")

    # Read with query to filter events
    print("\nReading user-related events...")
    user_query_item = QueryItem(tags=["user"])
    user_query = Query(items=[user_query_item])
    user_events = client.read(query=user_query)
    print(f"  - read response head is now: {user_events.head()}")
    for seq_event in user_events:
        print(f"  Position {seq_event.position}: {seq_event.event.event_type}")
        print(f"  - read response head is now: {user_events.head()}")
    print(f"  Last known position is: {user_events.head()}")

    # Read with multiple filters (OR logic)
    print("\nReading UserCreated OR OrderCreated events...")
    query = Query(
        items=[
            QueryItem(types=["UserCreated"]),
            QueryItem(types=["OrderCreated"]),
        ]
    )
    filtered_events = client.read(query=query)
    print(f"  - read response head is now: {filtered_events.head()}")
    for seq_event in filtered_events:
        print(f"  Position {seq_event.position}: {seq_event.event.event_type}")
        print(f"  - read response head is now: {filtered_events.head()}")
    print(f"  Last known position is: {filtered_events.head()}")

    # Read with limit forwards
    print("\nReading first 2 events forwards from the start...")
    recent_events = client.read(limit=2)
    print(f"  - read response head is now: {recent_events.head()}")
    for seq_event in recent_events:
        print(f"  Position {seq_event.position}: {seq_event.event.event_type}")
        print(f"  - read response head is now: {recent_events.head()}")
    print(f"  Last known position is: {recent_events.head()}")

    # Read with limit forwards
    print("\nReading first 3 events forwards from the start...")
    recent_events = client.read(limit=3)
    print(f"  - read response head is now: {recent_events.head()}")
    for seq_event in recent_events:
        print(f"  Position {seq_event.position}: {seq_event.event.event_type}")
        print(f"  - read response head is now: {recent_events.head()}")
    print(f"  Last known position is: {recent_events.head()}")

    # Read with limit backwards
    print("\nReading last 2 events backwards from the end...")
    old_events = client.read(limit=2, backwards=True)
    print(f"  - read response head is now: {old_events.head()}")
    for seq_event in old_events:
        print(f"  Position {seq_event.position}: {seq_event.event.event_type}")
        print(f"  - read response head is now: {old_events.head()}")
    print(f"  Last known position is: {old_events.head()}")

    # Read backwards without limit
    print("\nReading all events backwards from the end...")
    backwards_events = client.read(backwards=True)
    print(f"  - read response head is now: {backwards_events.head()}")
    for seq_event in backwards_events:
        print(f"  Position {seq_event.position}: {seq_event.event.event_type}")
        print(f"  - read response head is now: {backwards_events.head()}")
    print(f"  Last known position is: {backwards_events.head()}")

    # Try conditional append (preventing duplicate UserCreated for user:123)
    print("\nTrying conditional append (should fail)...")
    fail_query = Query(items=[QueryItem(types=["UserCreated"], tags=["user:123"])])
    condition = AppendCondition(fail_if_events_match=fail_query)

    duplicate_event = Event(
        event_type="UserCreated",
        data=b'{"user_id": "123", "name": "Alice Again"}',
        tags=["user", "user:123"],
    )

    try:
        client.append([duplicate_event], condition=condition)
        print("  Unexpected: append succeeded")
    except IntegrityError as e:
        print(f"  Expected: append failed - {e}")

    # Conditional append that should succeed
    print("\nTrying conditional append for new user (should succeed)...")
    new_user_id = str(uuid.uuid4())
    new_user_event = Event(
        event_type="UserCreated",
        data=b'{"user_id": "456", "name": "Bob"}',
        tags=["user", new_user_id],
    )
    fail_query = Query(items=[QueryItem(types=["UserCreated"], tags=[new_user_id])])
    condition = AppendCondition(fail_if_events_match=fail_query)

    try:
        position = client.append([new_user_event], condition=condition)
        print(f"  Success: event appended at position {position}")
    except ValueError as e:
        print(f"  Failed: {e}")

    # Demonstrate tracking positions
    print("\nDemonstrating tracking positions...")
    assert (
        client.get_tracking_info("not-a-source") is None
    ), "Expected no tracking position"

    tracking_source = "example-source"
    pos_before = client.get_tracking_info(tracking_source)
    print(f"  tracking('{tracking_source}') before append: {pos_before}")

    # Increment position
    tracking_info = TrackingInfo(tracking_source, (pos_before or 0) + 1)
    events = [
        Event(
            event_type="UpstreamCommitted",
            data=b"{}",
            tags=["tracking-demo"],
        )
    ]
    last = client.append(events, tracking_info=tracking_info)
    print(f"  Appended with tracking info {tracking_info}, recorded position: {last}")

    pos_after = client.get_tracking_info(tracking_source)
    print(f"  tracking('{tracking_source}') after append: {pos_after}")
    assert pos_after == tracking_info.position

    print("  Attempting append again with SAME tracking position (should fail)...")
    try:
        client.append(events, tracking_info=tracking_info)
        print("    Unexpected: append with same tracking succeeded")
    except IntegrityError as ie:
        print(f"    Expected failure: {ie}")

    # Final head position
    final_head = client.head()
    print(f"\nFinal head position: {final_head}")


if __name__ == "__main__":
    main()
