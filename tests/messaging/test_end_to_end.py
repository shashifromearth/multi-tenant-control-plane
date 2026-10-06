"""Whole pipeline in-process: API -> outbox -> relay -> RabbitMQ -> worker -> RabbitMQ ->
consumer -> DB, with the real worker simulator and fault injection."""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from control_plane.config import Settings
from control_plane.container import Container
from control_plane.infrastructure.consumers.progress_consumer import (
    ProgressConsumer,
    ProgressMessageHandler,
)
from control_plane.infrastructure.outbox.relay import OutboxRelay
from tests.messaging.helpers import eventually, running
from worker.simulator import Worker, WorkerConfig

pytestmark = [pytest.mark.integration, pytest.mark.messaging]


def pipeline(container: Container, settings: Settings, **worker_kw: Any) -> list[Any]:
    worker = Worker(
        WorkerConfig(
            amqp_url=settings.amqp_url,
            topology=settings.topology,
            min_delay_ms=0,
            max_delay_ms=20,
            seed=7,
            **worker_kw,
        )
    )
    relay = OutboxRelay(
        container.session_factory, settings.amqp_url, settings.topology, poll_interval_s=0.05
    )
    consumer = ProgressConsumer(
        ProgressMessageHandler(container.task_progress_service),
        settings.amqp_url,
        settings.topology,
    )
    return [relay.run_forever, consumer.run_forever, worker.run_forever]


def status_of(client: TestClient, tenant_id: str) -> str:
    return str(client.get(f"/tenants/{tenant_id}").json()["status"])


def test_full_lifecycle_without_manual_steps(
    client: TestClient, container: Container, settings: Settings, channel: Any
) -> None:
    with running(*pipeline(container, settings)):
        created = client.post("/tenants", json={"slug": "acme", "name": "ACME"}).json()
        tid = created["tenant"]["id"]
        eventually(lambda: status_of(client, tid) == "active")
        task = client.get(f"/tasks/{created['task']['id']}").json()
        assert task["status"] == "done"

        version = client.get(f"/tenants/{tid}").json()["version"]
        client.patch(f"/tenants/{tid}", json={"version": version, "name": "ACME 2"})
        eventually(lambda: status_of(client, tid) == "active")

        client.delete(f"/tenants/{tid}")
        eventually(lambda: status_of(client, tid) == "destroyed")

    tasks = client.get("/tasks", params={"tenant_id": tid}).json()
    assert sorted(t["type"] for t in tasks["items"]) == ["deploy", "destroy", "update"]
    assert {t["status"] for t in tasks["items"]} == {"done"}
    # 3 API mutations + 3 worker-applied terminal outcomes
    assert client.get(f"/tenants/{tid}").json()["version"] == 6


def test_worker_failures_drive_tenant_to_failed(
    client: TestClient, container: Container, settings: Settings, channel: Any
) -> None:
    with running(*pipeline(container, settings, fail_rate=1.0)):
        tid = client.post("/tenants", json={"slug": "acme", "name": "A"}).json()["tenant"]["id"]
        eventually(lambda: status_of(client, tid) == "failed")
        # failed -> destroying is allowed; with fail_rate=1 the destroy fails too.
        assert client.delete(f"/tenants/{tid}").status_code == 202
        eventually(lambda: status_of(client, tid) == "failed")
    tasks = client.get("/tasks", params={"tenant_id": tid}).json()["items"]
    assert {t["status"] for t in tasks} == {"failed"}


def test_duplicate_and_reordered_worker_output_converges(
    client: TestClient, container: Container, settings: Settings, channel: Any
) -> None:
    with running(*pipeline(container, settings, duplicate_rate=1.0, out_of_order_rate=1.0)):
        ids = [
            client.post("/tenants", json={"slug": f"t-{i}-x", "name": "n"}).json()["tenant"]["id"]
            for i in range(5)
        ]
        for tid in ids:
            eventually(lambda tid=tid: status_of(client, tid) == "active")
    for tid in ids:
        assert client.get(f"/tenants/{tid}").json()["version"] == 2
