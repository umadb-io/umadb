"""
UmaDB Python Client

A Python client for UmaDB event store using Rust bindings via PyO3.
"""

from umadb._umadb import (
    AppendCondition,
    Client,
    CorruptionError,
    Event,
    IntegrityError,
    Query,
    QueryItem,
    ReadResponse,
    SequencedEvent,
    TrackingInfo,
    TransportError,
)

__version__ = "0.3.1"

__all__ = [
    "AppendCondition",
    "Client",
    "CorruptionError",
    "Event",
    "IntegrityError",
    "Query",
    "QueryItem",
    "ReadResponse",
    "SequencedEvent",
    "TrackingInfo",
    "TransportError",
]
