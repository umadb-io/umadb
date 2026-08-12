from collections.abc import Iterator
from typing import cast

import pytest
import subprocess
import signal
import tempfile
import socket
from pathlib import Path

from umadb import Client, Event


def get_free_port() -> int:
    """Requests a free ephemeral port from the OS."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return cast(int, (s.getsockname()[1]))


@pytest.fixture
def umadb_server_url() -> Iterator[str]:
    """Starts UmaDB on an available port in a temp directory, yielding the connection URI."""
    port = get_free_port()
    listen_addr = f"127.0.0.1:{port}"
    connection_uri = f"http://{listen_addr}"

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_data.db"

        process = subprocess.Popen(
            ["umadb", "--db-path", str(db_path), "--listen", listen_addr],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # # Give the process a moment to initialize
        # time.sleep(0.5)

        # if process.poll() is not None:
        #     stdout, stderr = process.communicate()
        #     raise RuntimeError(
        #         f"UmaDB failed to start on {listen_addr}. stderr: {stderr}"
        #     )

        yield connection_uri

        # Teardown
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def test_umadb_client_append_and_read(umadb_server_url: str) -> None:
    # Connect the Client using the dynamic URI provided by the fixture
    client = Client(umadb_server_url)

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
