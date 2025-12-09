"""
UmaDB Python Client

A Python client for UmaDB event store using Rust bindings via PyO3.
"""


from umadb._umadb import (
    Client,
    Event,
    SequencedEvent,
    Query,
    QueryItem,
    AppendCondition,
    IntegrityError,
    TransportError,
    CorruptionError,
    ReadResponse,
)

__version__ = "0.1.29"

__all__ = [
    "Client",
    "Event",
    "SequencedEvent",
    "Query",
    "QueryItem",
    "AppendCondition",
    "IntegrityError",
    "TransportError",
    "CorruptionError",
    "ReadResponse",
]
