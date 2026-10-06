"""RabbitMQ connection helpers shared by the relay and the consumer."""

from __future__ import annotations

import random
import time
from pathlib import Path

import pika
from pika.adapters.blocking_connection import BlockingConnection


def connect(amqp_url: str, connection_name: str) -> BlockingConnection:
    params = pika.URLParameters(amqp_url)
    params.heartbeat = 30
    params.blocked_connection_timeout = 60
    params.connection_attempts = 1  # retries are handled by our own backoff loop
    params.client_properties = {"connection_name": connection_name}
    return pika.BlockingConnection(params)


class Backoff:
    """Exponential backoff with full jitter (AWS architecture blog style)."""

    def __init__(self, base_s: float = 0.5, cap_s: float = 30.0) -> None:
        self._base = base_s
        self._cap = cap_s
        self._attempt = 0

    def next_delay(self) -> float:
        ceiling = min(self._cap, self._base * (2**self._attempt))
        self._attempt += 1
        return random.uniform(0, ceiling)  # noqa: S311 - jitter, not crypto  # nosec B311

    def reset(self) -> None:
        self._attempt = 0


class Heartbeat:
    """Touches a file so container health checks can detect a stuck loop."""

    def __init__(self, path: str | None) -> None:
        self._path = Path(path) if path else None
        self._last = 0.0

    def beat(self) -> None:
        now = time.monotonic()
        if self._path is not None and now - self._last >= 5:
            self._path.touch()
            self._last = now
