"""HTTP API integration tests against a real PostgreSQL."""

from __future__ import annotations

import re
import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, update

from control_plane.container import Container
from control_plane.domain.enums import TenantStatus
from control_plane.infrastructure.database.models import OutboxMessageModel, TaskModel, TenantModel

pytestmark = pytest.mark.integration
OPEN = ("accepted", "in_progress")
ISO_MS_Z = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


def create(client: TestClient, slug: str = "acme", name: str = "ACME") -> dict:  # type: ignore[type-arg]
    response = client.post("/tenants", json={"slug": slug, "name": name})
    assert response.status_code == 201, response.text
    return response.json()  # type: ignore[no-any-return]


def force_status(container: Container, tenant_id: str, status: TenantStatus) -> None:
    """Test helper: put a tenant in a given status as if workers had driven it there.
    Tenants at rest have no open task, so open tasks are closed to keep the DB invariant
    (at most one open task per tenant, enforced by a partial unique index)."""
    with container.session_factory() as s:
        row = s.get(TenantModel, uuid.UUID(tenant_id))
        assert row is not None
        row.status = status.value
        if status in (TenantStatus.ACTIVE, TenantStatus.FAILED, TenantStatus.DESTROYED):
            s.execute(
                update(TaskModel)
                .where(TaskModel.tenant_id == row.id, TaskModel.status.in_(OPEN))
                .values(status="done")
            )
        s.commit()


def assert_error(response, status: int, code: str) -> None:  # type: ignore[no-untyped-def]
    assert response.status_code == status, response.text
    body = response.json()
    assert body["error"]["code"] == code
    assert body["error"]["message"]
    assert body["error"]["correlation_id"]


# ------------------------------------------------------------------------------ create
def test_create_tenant(client: TestClient, container: Container) -> None:
    response = client.post(
        "/tenants", json={"slug": "acme", "name": "ACME"}, headers={"X-Correlation-ID": "abc-123"}
    )
    assert response.status_code == 201
    body = response.json()
    tenant, task = body["tenant"], body["task"]
    assert response.headers["location"] == f"/tenants/{tenant['id']}"
    assert response.headers["etag"] == '"1"'
    assert response.headers["x-correlation-id"] == "abc-123"
    assert (tenant["slug"], tenant["status"], tenant["version"]) == ("acme", "provisioning", 1)
    assert (task["type"], task["status"], task["tenant_id"]) == ("deploy", "accepted", tenant["id"])
    for ts in (tenant["created_at"], tenant["updated_at"], task["created_at"]):
        assert ISO_MS_Z.match(ts), ts

    with container.session_factory() as s:
        [outbox] = s.scalars(select(OutboxMessageModel)).all()
    assert str(outbox.id) == task["id"]
    assert outbox.published_at is None  # published later by the relay, never in-request
    assert outbox.correlation_id == "abc-123"
    assert outbox.payload["id"] == task["id"]
    assert outbox.payload["type"] == "deploy"


def test_duplicate_slug_conflicts_even_against_destroyed(
    client: TestClient, container: Container
) -> None:
    tenant = create(client)["tenant"]
    force_status(container, tenant["id"], TenantStatus.DESTROYED)
    assert_error(
        client.post("/tenants", json={"slug": "acme", "name": "x"}), 409, "tenant_already_exists"
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"slug": "ab", "name": "x"},
        {"slug": "1abc", "name": "x"},
        {"slug": "abc-", "name": "x"},
        {"slug": "acme", "name": ""},
        {"slug": "acme"},
        {"slug": "acme", "name": "x", "unexpected": 1},
    ],
)
def test_create_validation(client: TestClient, payload: dict) -> None:  # type: ignore[type-arg]
    response = client.post("/tenants", json=payload)
    assert_error(response, 422, "validation_error")
    assert response.json()["error"]["details"]["errors"]


def test_whitespace_only_name_is_rejected_by_domain(client: TestClient) -> None:
    assert_error(
        client.post("/tenants", json={"slug": "acme", "name": "   "}), 422, "validation_error"
    )


# ------------------------------------------------------------------------------ read
def test_get_and_not_found(client: TestClient) -> None:
    tenant = create(client)["tenant"]
    response = client.get(f"/tenants/{tenant['id']}")
    assert response.status_code == 200
    assert response.json() == tenant
    assert response.headers["etag"] == '"1"'
    assert_error(client.get(f"/tenants/{uuid.uuid4()}"), 404, "tenant_not_found")
    assert_error(client.get("/tenants/not-a-uuid"), 422, "validation_error")


def test_list_pagination_and_filters(client: TestClient, container: Container) -> None:
    ids = [create(client, f"tenant-{i}")["tenant"]["id"] for i in range(5)]
    force_status(container, ids[0], TenantStatus.ACTIVE)

    page = client.get("/tenants", params={"limit": 2, "offset": 0}).json()
    assert (page["total"], page["limit"], page["offset"], len(page["items"])) == (5, 2, 0, 2)
    assert [t["id"] for t in page["items"]] == ids[::-1][:2]  # newest first

    last = client.get("/tenants", params={"limit": 2, "offset": 4}).json()
    assert [t["id"] for t in last["items"]] == [ids[0]]

    active = client.get("/tenants", params={"status": "active"}).json()
    assert [t["id"] for t in active["items"]] == [ids[0]]
    by_slug = client.get("/tenants", params={"slug": "tenant-3"}).json()
    assert by_slug["total"] == 1

    assert_error(client.get("/tenants", params={"limit": 0}), 422, "validation_error")
    assert_error(client.get("/tenants", params={"limit": 101}), 422, "validation_error")
    assert_error(client.get("/tenants", params={"status": "bogus"}), 422, "validation_error")


# ------------------------------------------------------------------------------ update
def test_patch_happy_path(client: TestClient, container: Container) -> None:
    tenant = create(client)["tenant"]
    force_status(container, tenant["id"], TenantStatus.ACTIVE)
    response = client.patch(f"/tenants/{tenant['id']}", json={"version": 1, "name": "ACME 2"})
    assert response.status_code == 202, response.text
    body = response.json()
    assert body["tenant"]["status"] == "updating"
    assert body["tenant"]["name"] == "ACME 2"
    assert body["tenant"]["version"] == 2
    assert body["task"]["type"] == "update"
    assert response.headers["location"] == f"/tasks/{body['task']['id']}"
    assert response.headers["etag"] == '"2"'


def test_patch_stale_version(client: TestClient, container: Container) -> None:
    tenant = create(client)["tenant"]
    force_status(container, tenant["id"], TenantStatus.ACTIVE)
    assert_error(
        client.patch(f"/tenants/{tenant['id']}", json={"version": 5, "name": "x"}),
        409,
        "tenant_version_conflict",
    )


@pytest.mark.parametrize(
    "status",
    [
        TenantStatus.PROVISIONING,
        TenantStatus.UPDATING,
        TenantStatus.DESTROYING,
        TenantStatus.FAILED,
        TenantStatus.DESTROYED,
    ],
)
def test_patch_not_allowed(client: TestClient, container: Container, status: TenantStatus) -> None:
    tenant = create(client)["tenant"]
    force_status(container, tenant["id"], status)
    assert_error(
        client.patch(f"/tenants/{tenant['id']}", json={"version": 1, "name": "x"}),
        409,
        "tenant_update_not_allowed",
    )


def test_patch_validation_and_not_found(client: TestClient) -> None:
    assert_error(
        client.patch(f"/tenants/{uuid.uuid4()}", json={"version": 1, "name": "x"}),
        404,
        "tenant_not_found",
    )
    tenant = create(client)["tenant"]
    assert_error(
        client.patch(f"/tenants/{tenant['id']}", json={"version": 1}), 422, "validation_error"
    )
    assert_error(
        client.patch(f"/tenants/{tenant['id']}", json={"name": "x"}), 422, "validation_error"
    )


# ------------------------------------------------------------------------------ delete
@pytest.mark.parametrize("status", [TenantStatus.ACTIVE, TenantStatus.FAILED])
def test_delete_allowed(client: TestClient, container: Container, status: TenantStatus) -> None:
    tenant = create(client)["tenant"]
    force_status(container, tenant["id"], status)
    response = client.delete(f"/tenants/{tenant['id']}")
    assert response.status_code == 202
    assert response.json()["tenant"]["status"] == "destroying"
    assert response.json()["task"]["type"] == "destroy"
    # A destroyed tenant is still readable (soft lifecycle; slug stays reserved).
    assert client.get(f"/tenants/{tenant['id']}").status_code == 200


@pytest.mark.parametrize(
    "status",
    [
        TenantStatus.PROVISIONING,
        TenantStatus.UPDATING,
        TenantStatus.DESTROYING,
        TenantStatus.DESTROYED,
    ],
)
def test_delete_not_allowed(client: TestClient, container: Container, status: TenantStatus) -> None:
    tenant = create(client)["tenant"]
    force_status(container, tenant["id"], status)
    assert_error(client.delete(f"/tenants/{tenant['id']}"), 409, "tenant_update_not_allowed")


def test_conditional_delete(client: TestClient, container: Container) -> None:
    tenant = create(client)["tenant"]
    force_status(container, tenant["id"], TenantStatus.ACTIVE)
    url = f"/tenants/{tenant['id']}"
    assert_error(client.delete(url, headers={"If-Match": '"9"'}), 409, "tenant_version_conflict")
    assert_error(client.delete(url, headers={"If-Match": "nope"}), 400, "precondition_invalid")
    assert client.delete(url, headers={"If-Match": '"1"'}).status_code == 202
    assert_error(client.delete(f"/tenants/{uuid.uuid4()}"), 404, "tenant_not_found")


# ------------------------------------------------------------------------------ misc
def test_unknown_route_and_method_use_error_envelope(client: TestClient) -> None:
    assert_error(client.get("/nope"), 404, "not_found")
    assert_error(client.put("/tenants"), 405, "method_not_allowed")


def test_unhandled_error_is_rendered_as_internal_error(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, container: Container
) -> None:
    def boom(*_: object) -> None:
        raise RuntimeError("kaboom")

    monkeypatch.setattr(container.tenant_service, "get", boom)
    response = client.get(f"/tenants/{uuid.uuid4()}", headers={"X-Correlation-ID": "trace-me"})
    assert_error(response, 500, "internal_error")
    assert "kaboom" not in response.text  # no internals leak
    assert response.json()["error"]["correlation_id"] == "trace-me"


def test_invalid_correlation_header_is_replaced(client: TestClient) -> None:
    response = client.get("/health/live", headers={"X-Correlation-ID": "bad id\twith spaces"})
    assert response.headers["x-correlation-id"] != "bad id\twith spaces"
    assert len(response.headers["x-correlation-id"]) == 32


def test_openapi_documents_error_codes(client: TestClient) -> None:
    spec = client.get("/openapi.json").json()
    assert "/tenants/{tenant_id}" in spec["paths"]
    patch = spec["paths"]["/tenants/{tenant_id}"]["patch"]["responses"]
    assert {"202", "404", "409", "422"} <= set(patch)
