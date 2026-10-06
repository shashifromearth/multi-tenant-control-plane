"""Progress consumer against real RabbitMQ + PostgreSQL."""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from control_plane.application.commands import CreateTenant
from control_plane.application.services.task_progress_service import TaskProgressService
from control_plane.config import Settings
from control_plane.container import Container
from control_plane.domain.enums import TaskStatus, TenantStatus
from control_plane.infrastructure.consumers.progress_consumer import (
    DEAD_LETTER_REASON_HEADER,
    ProgressConsumer,
    ProgressMessageHandler,
)
from tests.messaging.helpers import eventually, progress_body, publish, queue_depth, running

pytestmark = [pytest.mark.integration, pytest.mark.messaging]


def consumer(settings: Settings, service: Any) -> ProgressConsumer:
    return ProgressConsumer(
        ProgressMessageHandler(service), settings.amqp_url, settings.topology, prefetch=5
    )


def send(channel: Any, settings: Settings, body: bytes, status: str = "done") -> None:
    publish(channel, settings.topology.progress_exchange, f"task.progress.{status}", body)


def test_duplicates_and_out_of_order_through_the_broker(
    container: Container, settings: Settings, channel: Any
) -> None:
    created = container.tenant_service.create(CreateTenant(slug="acme", name="A"))
    task_id = created.task.id
    done_id = uuid.uuid4()
    with running(consumer(settings, container.task_progress_service).run_forever):
        # terminal first, then three duplicates, then a stale in_progress
        for _ in range(4):
            send(channel, settings, progress_body(task_id, "done", done_id))
        send(channel, settings, progress_body(task_id, "in_progress"), "in_progress")

        eventually(lambda: queue_depth(channel, settings.topology.progress_queue) == 0)
        eventually(
            lambda: container.tenant_service.get(created.tenant.id).status is TenantStatus.ACTIVE
        )
    tenant = container.tenant_service.get(created.tenant.id)
    assert tenant.version == 2  # exactly one effective mutation
    assert container.task_service.get(task_id).status is TaskStatus.DONE
    assert queue_depth(channel, settings.topology.progress_dlq) == 0


def test_poison_messages_go_to_dlq_and_consumer_keeps_working(
    container: Container, settings: Settings, channel: Any
) -> None:
    created = container.tenant_service.create(CreateTenant(slug="acme", name="A"))
    with running(consumer(settings, container.task_progress_service).run_forever):
        send(channel, settings, b"{not json")
        send(channel, settings, progress_body(uuid.uuid4(), "done"))  # unknown task
        send(channel, settings, progress_body(created.task.id, "done"))  # valid
        eventually(
            lambda: container.tenant_service.get(created.tenant.id).status is TenantStatus.ACTIVE
        )
        eventually(lambda: queue_depth(channel, settings.topology.progress_dlq) == 2)

    reasons = set()
    for _ in range(2):
        _, props, _ = channel.basic_get(settings.topology.progress_dlq, auto_ack=True)
        reasons.add(props.headers[DEAD_LETTER_REASON_HEADER].split(":")[0])
    assert reasons == {"invalid message", "task_not_found"}


class FlakyService:
    """Fails the first N calls (e.g. database outage), then delegates."""

    def __init__(self, inner: TaskProgressService, failures: int) -> None:
        self.inner = inner
        self.failures = failures
        self.calls = 0

    def apply(self, cmd: Any) -> Any:
        self.calls += 1
        if self.calls <= self.failures:
            raise ConnectionError("database unavailable")
        return self.inner.apply(cmd)


def test_transient_failures_are_retried_with_backoff(
    container: Container, settings: Settings, channel: Any
) -> None:
    created = container.tenant_service.create(CreateTenant(slug="acme", name="A"))
    flaky = FlakyService(container.task_progress_service, failures=2)
    with running(consumer(settings, flaky).run_forever):
        send(channel, settings, progress_body(created.task.id, "done"))
        eventually(
            lambda: container.tenant_service.get(created.tenant.id).status is TenantStatus.ACTIVE
        )
    assert flaky.calls == 3  # 1 attempt + 2 retries via the delay queues
    assert queue_depth(channel, settings.topology.progress_dlq) == 0


def test_permanent_failure_ends_in_dlq_after_max_retries(
    container: Container, settings: Settings, channel: Any
) -> None:
    created = container.tenant_service.create(CreateTenant(slug="acme", name="A"))
    flaky = FlakyService(container.task_progress_service, failures=10**6)
    with running(consumer(settings, flaky).run_forever):
        send(channel, settings, progress_body(created.task.id, "done"))
        eventually(lambda: queue_depth(channel, settings.topology.progress_dlq) == 1)
    assert flaky.calls == settings.topology.max_retries + 1
    _, props, _ = channel.basic_get(settings.topology.progress_dlq, auto_ack=True)
    assert props.headers[DEAD_LETTER_REASON_HEADER].startswith("retries exhausted")
    assert container.tenant_service.get(created.tenant.id).status is TenantStatus.PROVISIONING


def test_consumer_survives_broker_outage(container: Container, settings: Settings) -> None:
    import time

    broken = Settings(
        database_url=settings.database_url, amqp_url="amqp://guest:guest@127.0.0.1:1/%2F"
    )
    with running(consumer(broken, container.task_progress_service).run_forever):
        time.sleep(0.5)  # reconnect loop with backoff; must not raise
