from __future__ import annotations

import json
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import pika


@contextmanager
def running(*loops: Callable[[threading.Event], None]) -> Iterator[None]:
    """Run ``run_forever(stop)``-style loops in daemon threads for the block's duration."""
    stop = threading.Event()
    threads = [threading.Thread(target=loop, args=(stop,), daemon=True) for loop in loops]
    for t in threads:
        t.start()
    try:
        yield
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=10)


def eventually(predicate: Callable[[], Any], timeout: float = 15.0, interval: float = 0.05) -> Any:
    deadline = time.monotonic() + timeout
    while True:
        result = predicate()
        if result:
            return result
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(interval)


def progress_body(
    task_id: uuid.UUID | str, status: str, event_id: uuid.UUID | None = None
) -> bytes:
    return json.dumps(
        {
            "event_id": str(event_id or uuid.uuid4()),
            "task_id": str(task_id),
            "status": status,
            "timestamp": datetime.now(UTC).isoformat(),
        }
    ).encode()


def publish(channel: Any, exchange: str, routing_key: str, body: bytes) -> None:
    channel.basic_publish(
        exchange, routing_key, body, pika.BasicProperties(content_type="application/json")
    )


def queue_depth(channel: Any, queue: str) -> int:
    return int(channel.queue_declare(queue, passive=True).method.message_count)
