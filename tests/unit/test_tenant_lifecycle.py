from __future__ import annotations

import pytest

from control_plane.domain.entities import Task, Tenant
from control_plane.domain.enums import TaskStatus, TaskType, TenantStatus
from control_plane.domain.exceptions import InvalidStateTransition, TenantUpdateNotAllowed
from control_plane.domain.services.tenant_lifecycle import ProgressResult, TenantLifecycleService

S = TaskStatus
lifecycle = TenantLifecycleService()


def make(status: TenantStatus, task_type: TaskType, task_status: TaskStatus) -> tuple[Tenant, Task]:
    tenant = Tenant.register(slug="acme", name="ACME")
    tenant = Tenant.restore(
        id=tenant.id,
        slug=tenant.slug,
        name=tenant.name,
        status=status,
        version=1,
        created_at=tenant.created_at,
        updated_at=tenant.updated_at,
    )
    task = Task.accept(type=task_type, tenant_id=tenant.id)
    task = Task.restore(
        id=task.id,
        type=task_type,
        tenant_id=tenant.id,
        status=task_status,
        created_at=task.created_at,
        updated_at=task.updated_at,
    )
    return tenant, task


def test_provision_creates_deploy_task() -> None:
    tenant, task = lifecycle.provision(slug="acme", name="ACME")
    assert tenant.status is TenantStatus.PROVISIONING
    assert (task.type, task.status, task.tenant_id) == (TaskType.DEPLOY, S.ACCEPTED, tenant.id)


def test_update_only_from_active() -> None:
    tenant, _ = make(TenantStatus.ACTIVE, TaskType.DEPLOY, S.DONE)
    task = lifecycle.request_update(tenant, name="New")
    assert tenant.status is TenantStatus.UPDATING
    assert tenant.name == "New"
    assert task.type is TaskType.UPDATE


@pytest.mark.parametrize("status", [s for s in TenantStatus if s is not TenantStatus.ACTIVE])
def test_update_rejected_outside_active(status: TenantStatus) -> None:
    tenant, _ = make(status, TaskType.DEPLOY, S.DONE)
    with pytest.raises(TenantUpdateNotAllowed) as exc:
        lifecycle.request_update(tenant, name="New")
    assert exc.value.code == "tenant_update_not_allowed"
    assert tenant.name == "ACME"  # guard runs before any mutation


@pytest.mark.parametrize("status", [TenantStatus.ACTIVE, TenantStatus.FAILED])
def test_destroy_allowed(status: TenantStatus) -> None:
    tenant, _ = make(status, TaskType.DEPLOY, S.DONE)
    task = lifecycle.request_destroy(tenant)
    assert tenant.status is TenantStatus.DESTROYING
    assert task.type is TaskType.DESTROY


@pytest.mark.parametrize(
    "status",
    [
        TenantStatus.PROVISIONING,
        TenantStatus.UPDATING,
        TenantStatus.DESTROYING,
        TenantStatus.DESTROYED,
    ],
)
def test_destroy_rejected(status: TenantStatus) -> None:
    tenant, _ = make(status, TaskType.DEPLOY, S.DONE)
    with pytest.raises(TenantUpdateNotAllowed):
        lifecycle.request_destroy(tenant)


@pytest.mark.parametrize(
    ("tenant_status", "task_type", "outcome", "expected"),
    [
        (TenantStatus.PROVISIONING, TaskType.DEPLOY, S.DONE, TenantStatus.ACTIVE),
        (TenantStatus.PROVISIONING, TaskType.DEPLOY, S.FAILED, TenantStatus.FAILED),
        (TenantStatus.UPDATING, TaskType.UPDATE, S.DONE, TenantStatus.ACTIVE),
        (TenantStatus.UPDATING, TaskType.UPDATE, S.FAILED, TenantStatus.FAILED),
        (TenantStatus.DESTROYING, TaskType.DESTROY, S.DONE, TenantStatus.DESTROYED),
        (TenantStatus.DESTROYING, TaskType.DESTROY, S.FAILED, TenantStatus.FAILED),
    ],
)
def test_terminal_outcome_moves_tenant(
    tenant_status: TenantStatus, task_type: TaskType, outcome: TaskStatus, expected: TenantStatus
) -> None:
    tenant, task = make(tenant_status, task_type, S.IN_PROGRESS)
    result = lifecycle.apply_progress(task=task, tenant=tenant, reported=outcome)
    assert result.applied
    assert result.tenant_changed
    assert task.status is outcome
    assert tenant.status is expected


def test_in_progress_does_not_touch_tenant() -> None:
    tenant, task = make(TenantStatus.PROVISIONING, TaskType.DEPLOY, S.ACCEPTED)
    result = lifecycle.apply_progress(task=task, tenant=tenant, reported=S.IN_PROGRESS)
    assert result.result is ProgressResult.APPLIED
    assert not result.tenant_changed
    assert tenant.status is TenantStatus.PROVISIONING


def test_terminal_fast_forwards_through_in_progress() -> None:
    tenant, task = make(TenantStatus.PROVISIONING, TaskType.DEPLOY, S.ACCEPTED)
    result = lifecycle.apply_progress(task=task, tenant=tenant, reported=S.DONE)
    assert result.applied
    assert task.status is S.DONE
    assert tenant.status is TenantStatus.ACTIVE


@pytest.mark.parametrize(
    ("current", "reported"),
    [
        (S.IN_PROGRESS, S.IN_PROGRESS),  # duplicate semantic
        (S.DONE, S.IN_PROGRESS),  # stale after terminal
        (S.DONE, S.DONE),
        (S.DONE, S.FAILED),  # terminal states are immutable
        (S.FAILED, S.DONE),
        (S.FAILED, S.IN_PROGRESS),
    ],
)
def test_stale_updates_are_noops(current: TaskStatus, reported: TaskStatus) -> None:
    tenant_status = TenantStatus.ACTIVE if current.is_terminal else TenantStatus.PROVISIONING
    tenant, task = make(tenant_status, TaskType.DEPLOY, current)
    before = (task.status, task.updated_at, tenant.status, tenant.updated_at)
    result = lifecycle.apply_progress(task=task, tenant=tenant, reported=reported)
    assert result.result is ProgressResult.STALE
    assert not result.applied
    assert (task.status, task.updated_at, tenant.status, tenant.updated_at) == before


def test_inconsistent_tenant_state_is_rejected() -> None:
    # A deploy finishing for a tenant that is not provisioning is an invariant breach.
    tenant, task = make(TenantStatus.DESTROYED, TaskType.DEPLOY, S.IN_PROGRESS)
    with pytest.raises(InvalidStateTransition):
        lifecycle.apply_progress(task=task, tenant=tenant, reported=S.DONE)
