"""Inbound progress processing against real PostgreSQL: idempotency, ordering, effects."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from control_plane.application.commands import ApplyTaskProgress, CreateTenant, UpdateTenant
from control_plane.application.services.task_progress_service import ProgressHandling
from control_plane.container import Container
from control_plane.domain.enums import TaskStatus, TenantStatus
from control_plane.domain.exceptions import TaskNotFound
from control_plane.infrastructure.database.models import ProcessedEventModel

pytestmark = pytest.mark.integration
S = TaskStatus


def apply(
    container: Container, task_id: uuid.UUID, status: TaskStatus, event_id: uuid.UUID | None = None
) -> ProgressHandling:
    return container.task_progress_service.apply(
        ApplyTaskProgress(
            event_id=event_id or uuid.uuid4(),
            task_id=task_id,
            status=status,
            occurred_at=datetime.now(UTC),
        )
    )


def test_full_lifecycle_deploy_update_destroy(container: Container) -> None:
    svc = container.tenant_service
    created = svc.create(CreateTenant(slug="acme", name="A"))
    tid = created.tenant.id

    assert apply(container, created.task.id, S.IN_PROGRESS) is ProgressHandling.APPLIED
    assert svc.get(tid).status is TenantStatus.PROVISIONING
    assert apply(container, created.task.id, S.DONE) is ProgressHandling.APPLIED
    tenant = svc.get(tid)
    assert (tenant.status, tenant.version) == (TenantStatus.ACTIVE, 2)  # worker bumps version

    updated = svc.update(UpdateTenant(tenant_id=tid, expected_version=2, name="B"))
    apply(container, updated.task.id, S.IN_PROGRESS)
    apply(container, updated.task.id, S.DONE)
    assert svc.get(tid).status is TenantStatus.ACTIVE

    from control_plane.application.commands import DeleteTenant

    destroyed = svc.delete(DeleteTenant(tenant_id=tid))
    apply(container, destroyed.task.id, S.IN_PROGRESS)
    apply(container, destroyed.task.id, S.DONE)
    tenant = svc.get(tid)
    assert tenant.status is TenantStatus.DESTROYED
    assert tenant.version == 6
    assert container.task_service.get(destroyed.task.id).status is S.DONE


def test_failure_moves_tenant_to_failed(container: Container) -> None:
    created = container.tenant_service.create(CreateTenant(slug="acme", name="A"))
    apply(container, created.task.id, S.IN_PROGRESS)
    apply(container, created.task.id, S.FAILED)
    assert container.tenant_service.get(created.tenant.id).status is TenantStatus.FAILED
    assert container.task_service.get(created.task.id).status is S.FAILED


def test_duplicate_event_has_no_side_effects(container: Container) -> None:
    created = container.tenant_service.create(CreateTenant(slug="acme", name="A"))
    event_id = uuid.uuid4()
    assert apply(container, created.task.id, S.DONE, event_id) is ProgressHandling.APPLIED
    before = container.tenant_service.get(created.tenant.id)
    for _ in range(3):
        assert apply(container, created.task.id, S.DONE, event_id) is ProgressHandling.DUPLICATE
    after = container.tenant_service.get(created.tenant.id)
    assert (after.version, after.updated_at) == (before.version, before.updated_at)

    with container.session_factory() as s:
        rows = s.scalars(select(ProcessedEventModel)).all()
    assert [(r.event_id, r.outcome) for r in rows] == [(event_id, "applied")]


def test_out_of_order_in_progress_after_done_does_not_regress(container: Container) -> None:
    created = container.tenant_service.create(CreateTenant(slug="acme", name="A"))
    apply(container, created.task.id, S.DONE)  # terminal overtakes in_progress
    tenant_before = container.tenant_service.get(created.tenant.id)

    assert apply(container, created.task.id, S.IN_PROGRESS) is ProgressHandling.STALE
    assert (
        apply(container, created.task.id, S.FAILED) is ProgressHandling.STALE
    )  # terminal is final

    assert container.task_service.get(created.task.id).status is S.DONE
    tenant_after = container.tenant_service.get(created.tenant.id)
    assert tenant_after.status is TenantStatus.ACTIVE
    assert tenant_after.version == tenant_before.version


def test_unknown_task_rolls_back_event_record(container: Container) -> None:
    event_id = uuid.uuid4()
    with pytest.raises(TaskNotFound):
        apply(container, uuid.uuid4(), S.DONE, event_id)
    with container.session_factory() as s:
        assert s.get(ProcessedEventModel, event_id) is None
