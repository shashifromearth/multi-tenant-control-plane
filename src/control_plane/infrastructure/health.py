"""Dependency probes used by the readiness endpoint."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import pika
from pika.exceptions import AMQPError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from control_plane.domain.clock import utc_now
from control_plane.infrastructure.repositories.outbox_repository import SqlOutboxRepository


@dataclass(frozen=True, slots=True)
class ComponentHealth:
    ok: bool
    latency_ms: float
    details: dict[str, Any] = field(default_factory=dict)


def check_database(session_factory: sessionmaker[Session]) -> ComponentHealth:
    started = time.perf_counter()
    try:
        with session_factory() as session:
            session.execute(text("SELECT 1"))
            pending, oldest = SqlOutboxRepository(session).pending_stats()
    except SQLAlchemyError as exc:
        return ComponentHealth(False, _ms(started), {"error": type(exc).__name__})
    lag = (utc_now() - oldest).total_seconds() if oldest else 0.0
    return ComponentHealth(
        True,
        _ms(started),
        # Outbox lag is the key signal that the relay (or the broker) is unhealthy.
        {"outbox_pending": pending, "outbox_oldest_pending_age_s": round(lag, 3)},
    )


def check_broker(amqp_url: str) -> ComponentHealth:
    started = time.perf_counter()
    params = pika.URLParameters(amqp_url)
    params.socket_timeout = 2
    params.connection_attempts = 1
    params.blocked_connection_timeout = 2
    try:
        connection = pika.BlockingConnection(params)
        connection.close()
    except AMQPError as exc:
        return ComponentHealth(False, _ms(started), {"error": type(exc).__name__})
    return ComponentHealth(True, _ms(started))


def _ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000, 2)
