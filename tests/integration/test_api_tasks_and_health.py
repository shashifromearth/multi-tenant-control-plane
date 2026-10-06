from __future__ import annotations

import dataclasses
import uuid

import pytest
from fastapi.testclient import TestClient

from control_plane.config import Settings
from control_plane.container import Container

pytestmark = pytest.mark.integration


def test_tasks_list_get_and_filters(client: TestClient) -> None:
    a = client.post("/tenants", json={"slug": "aaa", "name": "A"}).json()
    client.post("/tenants", json={"slug": "bbb", "name": "B"})

    all_tasks = client.get("/tasks").json()
    assert all_tasks["total"] == 2

    mine = client.get("/tasks", params={"tenant_id": a["tenant"]["id"]}).json()
    assert [t["id"] for t in mine["items"]] == [a["task"]["id"]]
    assert (
        client.get("/tasks", params={"status": "accepted", "type": "deploy"}).json()["total"] == 2
    )
    assert client.get("/tasks", params={"status": "done"}).json()["total"] == 0

    task = client.get(f"/tasks/{a['task']['id']}")
    assert task.status_code == 200
    assert task.json() == a["task"]

    missing = client.get(f"/tasks/{uuid.uuid4()}")
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "task_not_found"


def test_tasks_are_read_only(client: TestClient) -> None:
    assert client.post("/tasks", json={}).status_code == 405
    assert client.delete(f"/tasks/{uuid.uuid4()}").status_code == 405


@pytest.mark.messaging
def test_readiness_reports_dependencies(client: TestClient, amqp_connection: object) -> None:
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert body["checks"]["database"]["ok"]
    assert body["checks"]["database"]["outbox_pending"] == 0
    assert body["checks"]["broker"]["ok"]
    assert client.get("/health/live").json() == {"status": "ok"}


def test_readiness_reports_unavailable_broker(client: TestClient, container: Container) -> None:
    broken = Settings(
        database_url=container.settings.database_url, amqp_url="amqp://guest:guest@127.0.0.1:1/%2F"
    )
    client.app.state.container = dataclasses.replace(container, settings=broken)  # type: ignore[attr-defined]
    response = client.get("/health")
    assert response.status_code == 503
    assert response.json()["checks"]["broker"]["ok"] is False
