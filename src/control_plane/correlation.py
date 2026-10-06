"""Correlation-id propagation (cross-cutting, dependency-free).

The id travels HTTP request -> outbox row -> AMQP ``correlation_id`` -> worker ->
progress event -> consumer, so one tenant operation can be traced across processes.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from uuid import uuid4

_correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)


def new_correlation_id() -> str:
    return uuid4().hex


def get_correlation_id() -> str | None:
    return _correlation_id.get()


@contextmanager
def correlation_scope(correlation_id: str | None) -> Iterator[str]:
    cid = correlation_id or new_correlation_id()
    token = _correlation_id.set(cid)
    try:
        yield cid
    finally:
        _correlation_id.reset(token)
