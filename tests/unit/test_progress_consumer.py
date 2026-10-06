"""Consumer decision logic and ack/retry/dead-letter routing, without a broker."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy.exc import OperationalError

from contracts.topology import Topology
from control_plane.application.services.task_progress_service import ProgressHandling
from control_plane.domain.enums import TaskStatus
from control_plane.domain.exceptions import (
    InvalidStateTransition,
    TaskNotFound,
    TenantVersionConflict,
)
from control_plane.infrastructure.consumers.progress_consumer import (
    DEAD_LETTER_REASON_HEADER,
    RETRY_COUNT_HEADER,
    Action,
    ProgressConsumer,
    ProgressMessageHandler,
)


class StubService:
    def __init__(self, outcome: Any) -> None:
        self.outcome = outcome
        self.calls = 0

    def apply(self, cmd: Any) -> ProgressHandling:
        self.calls += 1
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome  # type: ignore[no-any-return]


def body(**overrides: Any) -> bytes:
    payload = {
        "event_id": str(uuid.uuid4()),
        "task_id": str(uuid.uuid4()),
        "status": "done",
        "timestamp": datetime.now(UTC).isoformat(),
    }
    payload.update(overrides)
    return json.dumps(payload).encode()


@pytest.mark.parametrize(
    ("outcome", "action"),
    [
        (ProgressHandling.APPLIED, Action.ACK),
        (ProgressHandling.DUPLICATE, Action.ACK),
        (ProgressHandling.STALE, Action.ACK),
        (TenantVersionConflict(uuid.uuid4(), 1), Action.RETRY),
        (OperationalError("SELECT 1", {}, Exception("db down")), Action.RETRY),
        (RuntimeError("boom"), Action.RETRY),
        (TaskNotFound(uuid.uuid4()), Action.DEAD_LETTER),
        (
            InvalidStateTransition("tenant", TaskStatus.DONE, TaskStatus.ACCEPTED),
            Action.DEAD_LETTER,
        ),
    ],
)
def test_classification(outcome: Any, action: Action) -> None:
    disposition = ProgressMessageHandler(StubService(outcome)).handle(body())  # type: ignore[arg-type]
    assert disposition.action is action


@pytest.mark.parametrize(
    "raw",
    [b"not json", b"{}", body(status="exploded"), body(task_id="nope"), body(timestamp="x")],
)
def test_malformed_messages_are_dead_lettered_without_touching_the_service(raw: bytes) -> None:
    service = StubService(ProgressHandling.APPLIED)
    assert ProgressMessageHandler(service).handle(raw).action is Action.DEAD_LETTER  # type: ignore[arg-type]
    assert service.calls == 0


class FakeChannel:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []
        self.acked: list[int] = []

    def basic_publish(
        self, exchange: str, routing_key: str, body: bytes, properties: Any, mandatory: bool
    ) -> None:
        assert exchange == ""
        self.published.append((routing_key, properties.headers))

    def basic_ack(self, delivery_tag: int) -> None:
        self.acked.append(delivery_tag)


def _deliver(outcome: Any, headers: dict[str, Any] | None = None) -> FakeChannel:
    topology = Topology(prefix="u", retry_delays_ms=(10, 20))
    consumer = ProgressConsumer(
        ProgressMessageHandler(StubService(outcome)),
        "amqp://unused",
        topology,  # type: ignore[arg-type]
    )
    channel = FakeChannel()
    method = SimpleNamespace(delivery_tag=7, routing_key="task.progress.done")
    props = SimpleNamespace(
        correlation_id="cid",
        headers=headers,
        content_type="application/json",
        message_id="m",
        type="t",
        timestamp=None,
    )
    consumer.on_message(channel, method, props, body())  # type: ignore[arg-type]
    return channel


def test_success_is_acked_without_republish() -> None:
    channel = _deliver(ProgressHandling.APPLIED)
    assert channel.acked == [7]
    assert channel.published == []


def test_transient_failure_goes_to_first_retry_tier_then_acked() -> None:
    channel = _deliver(RuntimeError("db down"))
    [(queue, headers)] = channel.published
    assert queue.endswith("retry.10ms")
    assert headers[RETRY_COUNT_HEADER] == 1
    assert channel.acked == [7]


def test_retry_tiers_escalate() -> None:
    channel = _deliver(RuntimeError("db down"), {RETRY_COUNT_HEADER: 1})
    assert channel.published[0][0].endswith("retry.20ms")


def test_exhausted_retries_are_dead_lettered() -> None:
    channel = _deliver(RuntimeError("db down"), {RETRY_COUNT_HEADER: 2})
    [(queue, headers)] = channel.published
    assert queue.endswith(".dlq")
    assert "retries exhausted" in headers[DEAD_LETTER_REASON_HEADER]
    assert channel.acked == [7]


def test_poison_is_dead_lettered_immediately() -> None:
    channel = _deliver(TaskNotFound(uuid.uuid4()))
    [(queue, headers)] = channel.published
    assert queue.endswith(".dlq")
    assert headers[DEAD_LETTER_REASON_HEADER].startswith("task_not_found")


def test_garbage_retry_header_is_treated_as_zero() -> None:
    channel = _deliver(RuntimeError("x"), {RETRY_COUNT_HEADER: "garbage"})
    assert channel.published[0][1][RETRY_COUNT_HEADER] == 1
