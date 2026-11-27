from collections.abc import Iterator
import pytest
from umadb import Client
import umadb.testing


@pytest.fixture
def umadb_server_url() -> Iterator[str]:
    with umadb.testing.umadb_server_url() as url:
        yield url


@pytest.fixture
def umadb_client() -> Iterator[Client]:
    with umadb.testing.umadb_client() as client:
        yield client
