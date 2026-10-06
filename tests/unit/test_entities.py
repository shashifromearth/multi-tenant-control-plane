from __future__ import annotations

import pytest

from control_plane.domain.entities import Task, Tenant
from control_plane.domain.enums import TaskStatus, TaskType, TenantStatus
from control_plane.domain.exceptions import InvalidStateTransition, ValidationFailed


@pytest.mark.parametrize(
    "slug",
    ["abc", "a-b", "acme-corp-1", "a" + "b" * 26 + "c", "x1y"],
)
def test_valid_slugs(slug: str) -> None:
    assert Tenant.register(slug=slug, name="n").slug == slug


@pytest.mark.parametrize(
    "slug",
    [
        "ab",  # too short
        "a" * 29,  # too long
        "1abc",  # must start with a letter
        "abc-",  # must not end with a hyphen
        "Abc",  # lowercase only
        "ab_c",  # no underscores
        "ab c",
        "",
    ],
)
def test_invalid_slugs(slug: str) -> None:
    with pytest.raises(ValidationFailed):
        Tenant.register(slug=slug, name="n")


@pytest.mark.parametrize("name", ["", "   ", "x" * 256])
def test_invalid_names(name: str) -> None:
    with pytest.raises(ValidationFailed):
        Tenant.register(slug="acme", name=name)


def test_register_defaults() -> None:
    tenant = Tenant.register(slug="acme", name="  ACME  ")
    assert tenant.status is TenantStatus.PROVISIONING
    assert tenant.version == 1
    assert tenant.name == "ACME"
    assert tenant.created_at == tenant.updated_at
    assert tenant.created_at.microsecond % 1000 == 0  # millisecond precision


def test_status_is_read_only_and_transitions_are_validated() -> None:
    tenant = Tenant.register(slug="acme", name="ACME")
    with pytest.raises(AttributeError):
        tenant.status = TenantStatus.ACTIVE  # type: ignore[misc]
    with pytest.raises(InvalidStateTransition):
        tenant.transition_to(TenantStatus.DESTROYED)
    tenant.transition_to(TenantStatus.ACTIVE)
    assert tenant.status is TenantStatus.ACTIVE


def test_task_transitions() -> None:
    task = Task.accept(type=TaskType.DEPLOY, tenant_id=Tenant.register(slug="abc", name="n").id)
    assert task.status is TaskStatus.ACCEPTED
    with pytest.raises(InvalidStateTransition):
        task.transition_to(TaskStatus.DONE)
    task.transition_to(TaskStatus.IN_PROGRESS)
    task.transition_to(TaskStatus.DONE)
    with pytest.raises(InvalidStateTransition):
        task.transition_to(TaskStatus.FAILED)


def test_identity_equality() -> None:
    a = Tenant.register(slug="acme", name="A")
    b = Tenant.restore(
        id=a.id,
        slug="acme",
        name="other",
        status=TenantStatus.ACTIVE,
        version=9,
        created_at=a.created_at,
        updated_at=a.updated_at,
    )
    assert a == b
    assert hash(a) == hash(b)
    assert a != object()
    task = Task.accept(type=TaskType.DEPLOY, tenant_id=a.id)
    assert task == task
    assert task != a
    assert len({task, task}) == 1
