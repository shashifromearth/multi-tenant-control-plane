"""Application services against in-memory fakes (fast, no infrastructure)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

from contracts.topology import Topology
from control_plane.application.commands import (
    ApplyTaskProgress,
    CreateTenant,
    DeleteTenant,
    UpdateTenant,
)
from control_plane.application.dto import PageRequest, TaskFilter, TenantFilter
from control_plane.application.services.task_progress_service import (
    ProgressHandling,
    TaskProgressService,
)
from control_plane.application.services.task_service import TaskService
from control_plane.application.services.tenant_service import TenantService
from control_plane.correlation import correlation_scope
from control_plane.domain.enums import TaskStatus, TenantStatus
from control_plane.domain.exceptions import (
    TaskNotFound,
    TenantAlreadyExists,
    TenantNotFound,
    TenantUpdateNotAllowed,
    TenantVersionConflict,
)
from control_plane.domain.services.tenant_lifecycle import TenantLifecycleService
from tests.unit.fakes import FakeStore, FakeUnitOfWork


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


@pytest.fixture
def tenants(store: FakeStore) -> TenantService:
    return TenantService(lambda: FakeUnitOfWork(store), TenantLifecycleService(), Topology())


@pytest.fixture
def progress(store: FakeStore) -> TaskProgressService:
    return TaskProgressService(lambda: FakeUnitOfWork(store), TenantLifecycleService())


def _progress(task_id: uuid.UUID, status: TaskStatus, event_id: uuid.UUID | None = None):  # type: ignore[no-untyped-def]
    return ApplyTaskProgress(
        event_id=event_id or uuid.uuid4(),
        task_id=task_id,
        status=status,
        occurred_at=datetime.now(UTC),
    )


def test_create_writes_tenant_task_and_outbox_atomically(
    tenants: TenantService, store: FakeStore
) -> None:
    with correlation_scope("cid-1"):
        result = tenants.create(CreateTenant(slug="acme", name="ACME"))
    assert store.tenants[result.tenant.id].status is TenantStatus.PROVISIONING
    assert store.tasks[result.task.id].status is TaskStatus.ACCEPTED
    [event] = store.outbox
    assert event.id == result.task.id  # the task is the event
    assert event.routing_key == "task.deploy"
    assert event.correlation_id == "cid-1"
    assert event.payload["tenant_id"] == str(result.tenant.id)
    assert event.payload["status"] == "accepted"


def test_duplicate_slug(tenants: TenantService) -> None:
    tenants.create(CreateTenant(slug="acme", name="ACME"))
    with pytest.raises(TenantAlreadyExists):
        tenants.create(CreateTenant(slug="acme", name="Other"))


def test_update_requires_current_version(tenants: TenantService, store: FakeStore) -> None:
    tenant = tenants.create(CreateTenant(slug="acme", name="ACME")).tenant
    store.tenants[tenant.id].transition_to(TenantStatus.ACTIVE)
    with pytest.raises(TenantVersionConflict):
        tenants.update(UpdateTenant(tenant_id=tenant.id, expected_version=99, name="X"))
    result = tenants.update(UpdateTenant(tenant_id=tenant.id, expected_version=1, name="X"))
    assert result.tenant.version == 2
    assert result.tenant.status is TenantStatus.UPDATING
    assert len(store.outbox) == 2


def test_update_rejected_while_provisioning_writes_nothing(
    tenants: TenantService, store: FakeStore
) -> None:
    tenant = tenants.create(CreateTenant(slug="acme", name="ACME")).tenant
    with pytest.raises(TenantUpdateNotAllowed):
        tenants.update(UpdateTenant(tenant_id=tenant.id, expected_version=1, name="X"))
    assert len(store.outbox) == 1
    assert store.tenants[tenant.id].name == "ACME"


def test_not_found(tenants: TenantService) -> None:
    missing = uuid.uuid4()
    with pytest.raises(TenantNotFound):
        tenants.get(missing)
    with pytest.raises(TenantNotFound):
        tenants.delete(DeleteTenant(tenant_id=missing))
    with pytest.raises(TaskNotFound):
        TaskService(lambda: FakeUnitOfWork(FakeStore())).get(missing)


def test_delete_with_stale_if_match(tenants: TenantService, store: FakeStore) -> None:
    tenant = tenants.create(CreateTenant(slug="acme", name="ACME")).tenant
    store.tenants[tenant.id].transition_to(TenantStatus.ACTIVE)
    with pytest.raises(TenantVersionConflict):
        tenants.delete(DeleteTenant(tenant_id=tenant.id, expected_version=7))
    assert tenants.delete(DeleteTenant(tenant_id=tenant.id, expected_version=1)).task


def test_progress_lifecycle_and_idempotency(
    tenants: TenantService, progress: TaskProgressService, store: FakeStore
) -> None:
    created = tenants.create(CreateTenant(slug="acme", name="ACME"))
    task_id = created.task.id
    done_id = uuid.uuid4()
    assert progress.apply(_progress(task_id, TaskStatus.IN_PROGRESS)) is ProgressHandling.APPLIED
    assert progress.apply(_progress(task_id, TaskStatus.DONE, done_id)) is ProgressHandling.APPLIED
    assert (
        progress.apply(_progress(task_id, TaskStatus.DONE, done_id)) is ProgressHandling.DUPLICATE
    )
    assert progress.apply(_progress(task_id, TaskStatus.IN_PROGRESS)) is ProgressHandling.STALE
    tenant = store.tenants[created.tenant.id]
    assert tenant.status is TenantStatus.ACTIVE
    assert tenant.version == 2  # one worker-applied tenant mutation
    assert store.events[done_id] == "applied"


def test_progress_for_unknown_task(progress: TaskProgressService) -> None:
    with pytest.raises(TaskNotFound):
        progress.apply(_progress(uuid.uuid4(), TaskStatus.DONE))


def test_list_pages(tenants: TenantService) -> None:
    for i in range(3):
        tenants.create(CreateTenant(slug=f"t{i}-x", name="n"))
    page = tenants.list(TenantFilter(), PageRequest(limit=2, offset=0))
    assert (len(page.items), page.total, page.limit) == (2, 3, 2)
    assert tenants.list(TenantFilter(slug="t1-x"), PageRequest()).total == 1
    tasks = TaskService(tenants._uow_factory)
    assert tasks.list(TaskFilter(), PageRequest()).total == 3
