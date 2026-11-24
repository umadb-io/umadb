from collections.abc import Iterator
import pytest
from umadb import Client
from umadb.testing import umadb_server


@pytest.fixture
def umadb_server_url() -> Iterator[str]:
    with umadb_server() as url:
        yield url


@pytest.fixture
def umadb_client(umadb_server_url: str) -> Iterator[Client]:
    with Client(umadb_server_url) as client:
        yield client