from umadb import Client, Event

def test_umadb_server_url(umadb_server_url: str) -> None:
    # Connect the Client using the dynamic URI provided by the fixture
    with Client(umadb_server_url) as client:

        # 1. Create and append a test event
        event = Event(
            event_type="UserCreated",
            data=b"user data",
            tags=["user:123"],
        )

        position = client.append([event])
        assert position is not None, "Expected a valid position sequence after appending."

        # 2. Read the events back and verify the data
        events = list(client.read())

        assert len(events) == 1, "Expected exactly one event in the database."
        assert events[0].position == position
        assert events[0].event.event_type == "UserCreated"
        assert events[0].event.data == b"user data"
        assert "user:123" in events[0].event.tags

def test_umadb_client(umadb_client: Client) -> None:
    # 1. Create and append a test event
    event = Event(
        event_type="UserCreated",
        data=b"user data",
        tags=["user:123"],
    )

    position = umadb_client.append([event])
    assert position is not None, "Expected a valid position sequence after appending."

    # 2. Read the events back and verify the data
    events = list(umadb_client.read())

    assert len(events) == 1, "Expected exactly one event in the database."
    assert events[0].position == position
    assert events[0].event.event_type == "UserCreated"
    assert events[0].event.data == b"user data"
    assert "user:123" in events[0].event.tags
