"""Transactional outbox: publish strictly after commit, never on rollback."""

from __future__ import annotations

import dataclasses
import json
import time

import pytest
from sqlalchemy import select

from control_plane.application.commands import CreateTenant
from control_plane.application.services.events import build_task_event
from control_plane.config import Settings
from control_plane.container import Container
from control_plane.domain.services.tenant_lifecycle import TenantLifecycleService
from control_plane.infrastructure.database.models import OutboxMessageModel
from control_plane.infrastructure.database.unit_of_work import SqlAlchemyUnitOfWork
from control_plane.infrastructure.outbox.relay import OutboxRelay
from tests.messaging.helpers import eventually, queue_depth, running

pytestmark = [pytest.mark.integration, pytest.mark.messaging]


def relay(container: Container, settings: Settings) -> OutboxRelay:
    return OutboxRelay(
        container.session_factory,
        settings.amqp_url,
        settings.topology,
        batch_size=10,
        poll_interval_s=0.05,
    )


def test_committed_task_is_published_with_task_as_event(
    container: Container, settings: Settings, channel
) -> None:  # type: ignore[no-untyped-def]
    created = container.tenant_service.create(CreateTenant(slug="acme", name="A"))
    channel.confirm_delivery()
    assert relay(container, settings).publish_pending(channel) == 1

    method, props, body = channel.basic_get(settings.topology.worker_queue, auto_ack=True)
    assert method is not None
    payload = json.loads(body)
    assert props.message_id == str(created.task.id)
    assert props.delivery_mode == 2
    assert method.routing_key == "task.deploy"
    assert (payload["id"], payload["type"], payload["tenant_id"], payload["status"]) == (
        str(created.task.id),
        "deploy",
        str(created.tenant.id),
        "accepted",
    )
    with container.session_factory() as s:
        row = s.get(OutboxMessageModel, created.task.id)
        assert row is not None
        assert row.published_at is not None
        assert row.attempts == 1

    # Already published -> nothing left to send.
    assert relay(container, settings).publish_pending(channel) == 0


def test_uncommitted_and_rolled_back_writes_are_never_published(
    container: Container, settings: Settings, channel
) -> None:  # type: ignore[no-untyped-def]
    channel.confirm_delivery()
    lifecycle = TenantLifecycleService()
    with SqlAlchemyUnitOfWork(container.session_factory) as uow:
        tenant, task = lifecycle.provision(slug="ghost", name="Ghost")
        uow.tenants.add(tenant)
        uow.tasks.add(task)
        uow.outbox.add(build_task_event(task, tenant, settings.topology))
        # Transaction still open: the relay must not see the row.
        assert relay(container, settings).publish_pending(channel) == 0
        # ... and we roll back instead of committing.
    assert relay(container, settings).publish_pending(channel) == 0
    assert queue_depth(channel, settings.topology.worker_queue) == 0


def test_unroutable_message_stays_pending_with_error(
    container: Container, settings: Settings, channel
) -> None:  # type: ignore[no-untyped-def]
    channel.confirm_delivery()
    tenant, task = TenantLifecycleService().provision(slug="lost", name="Lost")
    event = build_task_event(task, tenant, settings.topology)
    with SqlAlchemyUnitOfWork(container.session_factory) as uow:
        uow.tenants.add(tenant)
        uow.tasks.add(task)
        uow.outbox.add(dataclasses.replace(event, routing_key="nowhere.bound"))
        uow.commit()
    assert relay(container, settings).publish_pending(channel) == 0
    with container.session_factory() as s:
        row = s.scalars(select(OutboxMessageModel)).one()
    assert row.published_at is None
    assert row.attempts == 1
    assert "Unroutable" in (row.last_error or "")


def test_relay_loop_publishes_continuously(
    container: Container, settings: Settings, channel
) -> None:  # type: ignore[no-untyped-def]
    with running(relay(container, settings).run_forever):
        for i in range(3):
            container.tenant_service.create(CreateTenant(slug=f"t-{i}-x", name="n"))
        eventually(lambda: queue_depth(channel, settings.topology.worker_queue) == 3)


def test_relay_survives_broker_outage(container: Container, settings: Settings) -> None:
    broken = Settings(
        database_url=settings.database_url, amqp_url="amqp://guest:guest@127.0.0.1:1/%2F"
    )
    container.tenant_service.create(CreateTenant(slug="acme", name="A"))
    r = relay(container, broken)
    with running(r.run_forever):
        time.sleep(0.5)  # several failed connection attempts, no crash
    with container.session_factory() as s:
        assert s.scalars(select(OutboxMessageModel)).one().published_at is None
