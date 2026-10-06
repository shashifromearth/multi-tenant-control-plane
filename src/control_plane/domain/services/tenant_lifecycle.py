"""Tenant lifecycle domain service.

Owns the business rules that span a tenant *and* its tasks:

* which API operation is allowed in which tenant status (guards),
* which task an operation spawns,
* how a worker-reported task outcome moves the task and, on a terminal outcome, the
  tenant -- including order tolerance (no regressions) and idempotency of effects.

It is pure (no I/O), which makes every rule unit-testable without a database.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from control_plane.domain.entities import Task, Tenant
from control_plane.domain.enums import TaskStatus, TaskType, TenantStatus
from control_plane.domain.exceptions import TenantUpdateNotAllowed
from control_plane.domain.services.state_machine import (
    TASK_PROGRESS_RANK,
    TENANT_STATE_MACHINE,
)

# Terminal task outcome -> resulting tenant status.
_TENANT_STATUS_ON_DONE: dict[TaskType, TenantStatus] = {
    TaskType.DEPLOY: TenantStatus.ACTIVE,
    TaskType.UPDATE: TenantStatus.ACTIVE,
    TaskType.DESTROY: TenantStatus.DESTROYED,
}


class ProgressResult(StrEnum):
    APPLIED = "applied"
    STALE = "stale"  # duplicate, out-of-order or targets a terminal task -> no-op


@dataclass(frozen=True, slots=True)
class ProgressOutcome:
    result: ProgressResult
    task_status: TaskStatus
    tenant_changed: bool = False

    @property
    def applied(self) -> bool:
        return self.result is ProgressResult.APPLIED


class TenantLifecycleService:
    """Stateless; a module-level instance is fine but it is injected for testability."""

    def provision(self, *, slug: str, name: str) -> tuple[Tenant, Task]:
        tenant = Tenant.register(slug=slug, name=name)
        return tenant, Task.accept(type=TaskType.DEPLOY, tenant_id=tenant.id)

    def request_update(self, tenant: Tenant, *, name: str | None) -> Task:
        self._guard(tenant, TenantStatus.UPDATING, operation="update")
        if name is not None:
            tenant.rename(name)
        tenant.transition_to(TenantStatus.UPDATING)
        return Task.accept(type=TaskType.UPDATE, tenant_id=tenant.id)

    def request_destroy(self, tenant: Tenant) -> Task:
        self._guard(tenant, TenantStatus.DESTROYING, operation="delete")
        tenant.transition_to(TenantStatus.DESTROYING)
        return Task.accept(type=TaskType.DESTROY, tenant_id=tenant.id)

    def apply_progress(
        self, *, task: Task, tenant: Tenant, reported: TaskStatus
    ) -> ProgressOutcome:
        """Apply a worker-reported status.

        Order tolerance: an update is applied only if it moves the task strictly forward
        (by rank). Anything else -- a duplicate, a stale ``in_progress`` after ``done``, or
        any update for a terminal task -- is a no-op, never an error, so redelivery is safe.

        If a terminal outcome overtakes ``in_progress`` (e.g. ``in_progress`` delayed),
        the task is walked through ``in_progress`` so every hop is still validated by the
        task state machine; the late ``in_progress`` then arrives as stale.
        """
        current = task.status
        if current.is_terminal or TASK_PROGRESS_RANK[reported] <= TASK_PROGRESS_RANK[current]:
            return ProgressOutcome(ProgressResult.STALE, current)

        if current is TaskStatus.ACCEPTED and reported.is_terminal:
            task.transition_to(TaskStatus.IN_PROGRESS)
        task.transition_to(reported)

        tenant_changed = False
        if reported.is_terminal:
            tenant.transition_to(self._tenant_status_for(task.type, reported))
            tenant_changed = True
        return ProgressOutcome(ProgressResult.APPLIED, task.status, tenant_changed)

    @staticmethod
    def _tenant_status_for(task_type: TaskType, outcome: TaskStatus) -> TenantStatus:
        if outcome is TaskStatus.FAILED:
            return TenantStatus.FAILED
        return _TENANT_STATUS_ON_DONE[task_type]

    @staticmethod
    def _guard(tenant: Tenant, target: TenantStatus, *, operation: str) -> None:
        # Guards reuse the transition table -- no status checks scattered elsewhere.
        if not TENANT_STATE_MACHINE.can_transition(tenant.status, target):
            raise TenantUpdateNotAllowed(tenant.id, tenant.status, operation)
