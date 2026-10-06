"""Real races against PostgreSQL: N threads released simultaneously by a barrier.

The assertions are on *outcomes*, which are deterministic regardless of interleaving:
whatever order the database serialises the writers in, exactly one may win.
"""

from __future__ import annotations

import threading
import uuid
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

import pytest

from control_plane.application.commands import (
    ApplyTaskProgress,
    CreateTenant,
    DeleteTenant,
    UpdateTenant,
)
from control_plane.application.dto import PageRequest, TaskFilter
from control_plane.container import Container
from control_plane.domain.enums import TaskStatus, TenantStatus
from control_plane.domain.exceptions import DomainError

pytestmark = pytest.mark.integration
N = 16


def race(n: int, fn: Callable[[int], Any]) -> Counter[str]:
    """Run ``fn(i)`` in ``n`` threads released at the same instant; tally outcomes."""
    barrier = threading.Barrier(n)

    def run(i: int) -> str:
        barrier.wait()
        try:
            result = fn(i)
        except DomainError as exc:
            return exc.code
        return "ok" if not isinstance(result, str) else result

    with ThreadPoolExecutor(max_workers=n) as pool:
        return Counter(pool.map(run, range(n)))


def activate(container: Container, slug: str = "acme") -> uuid.UUID:
    created = container.tenant_service.create(CreateTenant(slug=slug, name="A"))
    container.task_progress_service.apply(
        ApplyTaskProgress(uuid.uuid4(), created.task.id, TaskStatus.DONE, datetime.now(UTC))
    )
    return created.tenant.id


def test_concurrent_creates_with_same_slug_exactly_one_wins(container: Container) -> None:
    outcomes = race(
        N,
        lambda i: container.tenant_service.create(
            CreateTenant(slug="same-slug", name=f"client {i}")
        ),
    )
    assert outcomes == Counter({"ok": 1, "tenant_already_exists": N - 1})
    page = container.task_service.list(TaskFilter(), PageRequest())
    assert page.total == 1  # exactly one deploy task, i.e. exactly one event


def test_concurrent_patches_with_same_version_exactly_one_wins(container: Container) -> None:
    tenant_id = activate(container)
    version = container.tenant_service.get(tenant_id).version

    outcomes = race(
        N,
        lambda i: container.tenant_service.update(
            UpdateTenant(tenant_id=tenant_id, expected_version=version, name=f"writer {i}")
        ),
    )
    assert outcomes == Counter({"ok": 1, "tenant_version_conflict": N - 1})
    tenant = container.tenant_service.get(tenant_id)
    assert (tenant.status, tenant.version) == (TenantStatus.UPDATING, version + 1)


def test_concurrent_patch_and_delete_exactly_one_wins(container: Container) -> None:
    tenant_id = activate(container)
    version = container.tenant_service.get(tenant_id).version

    def op(i: int) -> Any:
        if i % 2:
            return container.tenant_service.delete(DeleteTenant(tenant_id=tenant_id))
        return container.tenant_service.update(
            UpdateTenant(tenant_id=tenant_id, expected_version=version, name="x")
        )

    outcomes = race(N, op)
    assert outcomes["ok"] == 1
    # Losers either lost the CAS or observed the winner's new status.
    assert set(outcomes) <= {"ok", "tenant_version_conflict", "tenant_update_not_allowed"}
    open_tasks = [
        t
        for t in container.task_service.list(TaskFilter(tenant_id=tenant_id), PageRequest()).items
        if t.status.is_open
    ]
    assert len(open_tasks) == 1  # never two concurrent operations on one tenant


def test_same_event_delivered_concurrently_is_applied_once(container: Container) -> None:
    created = container.tenant_service.create(CreateTenant(slug="acme", name="A"))
    event_id = uuid.uuid4()
    cmd = ApplyTaskProgress(event_id, created.task.id, TaskStatus.DONE, datetime.now(UTC))

    outcomes = race(N, lambda _: container.task_progress_service.apply(cmd).value)
    assert outcomes == Counter({"applied": 1, "duplicate": N - 1})
    assert container.tenant_service.get(created.tenant.id).version == 2


def test_conflicting_events_for_one_task_resolve_to_a_single_terminal_state(
    container: Container,
) -> None:
    created = container.tenant_service.create(CreateTenant(slug="acme", name="A"))
    statuses = [TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.IN_PROGRESS]

    outcomes = race(
        N,
        lambda i: (
            container.task_progress_service.apply(
                ApplyTaskProgress(uuid.uuid4(), created.task.id, statuses[i % 3], datetime.now(UTC))
            ).value
        ),
    )
    assert outcomes["applied"] in (1, 2)  # terminal (+ maybe in_progress first)
    task = container.task_service.get(created.task.id)
    tenant = container.tenant_service.get(created.tenant.id)
    assert task.status.is_terminal
    expected = TenantStatus.ACTIVE if task.status is TaskStatus.DONE else TenantStatus.FAILED
    assert tenant.status is expected
    assert tenant.version == 2  # tenant moved exactly once
