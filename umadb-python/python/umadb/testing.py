from __future__ import annotations

import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TextIO

from umadb import Client

_LISTENING_RE = re.compile(r"UmaDB is listening on (\S+)")


def _wait_for_listen_addr(
    process: subprocess.Popen[str],
    log_path: Path,
    timeout: float = 10.0,
) -> str:
    assert process.stdout is not None
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if process.poll() is not None:
            remaining = process.stdout.read()
            if remaining:
                with log_path.open("a", encoding="utf-8", errors="replace") as log_file:
                    log_file.write(remaining)
            logs = log_path.read_text(encoding="utf-8", errors="replace") if log_path.exists() else ""
            msg = f"UmaDB exited early with code {process.returncode}\nLogs:\n{logs}"
            raise RuntimeError(msg)

        line = process.stdout.readline()
        if not line:
            time.sleep(0.02)
            continue

        with log_path.open("a", encoding="utf-8", errors="replace") as log_file:
            log_file.write(line)

        match = _LISTENING_RE.search(line)
        if match:
            return match.group(1)

    logs = log_path.read_text(encoding="utf-8", errors="replace") if log_path.exists() else ""
    msg = f"UmaDB did not report listening address within {timeout}s\nLogs:\n{logs}"
    raise TimeoutError(msg)


def _drain_remaining_output(stdout: TextIO | None, log_path: Path) -> None:
    if stdout is None:
        return
    try:
        rest = stdout.read()
        if rest:
            with log_path.open("a", encoding="utf-8", errors="replace") as log_file:
                log_file.write(rest)
    except OSError:
        pass


@contextmanager
def umadb_server_url() -> Iterator[str]:
    """Start a temporary UmaDB server on an ephemeral port and yield its URI."""
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
                "127.0.0.1:0",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            creationflags=creationflags,
        )

        drainer: threading.Thread | None = None
        try:
            listen_addr = _wait_for_listen_addr(process, log_path)
            host, port_str = listen_addr.rsplit(":", 1)
            port = int(port_str)

            # Drain remaining stdout in a background thread so the pipe buffer never fills up
            drainer = threading.Thread(
                target=_drain_remaining_output,
                args=(process.stdout, log_path),
                daemon=True,
            )
            drainer.start()

            with socket.create_connection((host, port), timeout=2.0):
                pass

            yield f"http://{listen_addr}"
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
            if drainer is not None:
                drainer.join(timeout=1)
            if process.stdout is not None:
                process.stdout.close()


@contextmanager
def umadb_client() -> Iterator[Client]:
    """Start a temporary UmaDB server and yield a connected Client."""
    with umadb_server_url() as url, Client(url) as client:
        yield client