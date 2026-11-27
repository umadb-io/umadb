from __future__ import annotations

import signal
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import cast

from umadb import Client


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return cast(int, s.getsockname()[1])


def _wait_for_port(
        host: str, port: int, process: subprocess.Popen[str], timeout: float = 10.0
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            msg = f"UmaDB exited early with code {process.returncode}"
            raise RuntimeError(msg)
        try:
            with socket.create_connection((host, port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    msg = f"UmaDB did not start listening on {host}:{port} within {timeout}s"
    raise TimeoutError(msg)


@contextmanager
def umadb_server_url() -> Iterator[str]:
    """Start a temporary UmaDB server on an ephemeral port and yield its URI."""
    port = _get_free_port()
    listen_addr = f"127.0.0.1:{port}"
    connection_uri = f"http://{listen_addr}"

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_data.db"
        log_path = Path(tmpdir) / "umadb.log"

        with log_path.open("w+") as log_file:
            if sys.platform == "win32":
                creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                creationflags = 0

            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "umadb",
                    "--db-path",
                    str(db_path),
                    "--listen",
                    listen_addr,
                ],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                creationflags=creationflags,
            )

            try:
                _wait_for_port("127.0.0.1", port, process)
                yield connection_uri
            finally:
                if process.poll() is None:
                    if sys.platform == "win32":
                        process.terminate()
                    else:
                        process.send_signal(signal.SIGINT)
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()


@contextmanager
def umadb_client() -> Iterator[Client]:
    """Start a temporary UmaDB server and yield a connected Client."""
    with umadb_server_url() as url, Client(url) as client:
        yield client