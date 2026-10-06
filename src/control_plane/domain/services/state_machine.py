"""Explicit, table-driven state machines.

The transition tables below are the single source of truth for which status changes
are legal. Entities route every status change through :meth:`StateMachine.ensure`,
so there is no code path that can set a status without being validated here.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum

from control_plane.domain.enums import TaskStatus, TenantStatus
from control_plane.domain.exceptions import InvalidStateTransition


class StateMachine[S: StrEnum]:
    def __init__(self, entity: str, transitions: Mapping[S, frozenset[S]]) -> None:
        self._entity = entity
        self._transitions = transitions

    def can_transition(self, current: S, target: S) -> bool:
        return target in self._transitions.get(current, frozenset())

    def ensure(self, current: S, target: S) -> None:
        if not self.can_transition(current, target):
            raise InvalidStateTransition(self._entity, current, target)

    def allowed_from(self, current: S) -> frozenset[S]:
        return self._transitions.get(current, frozenset())

    def is_terminal(self, state: S) -> bool:
        return not self._transitions.get(state)


TENANT_STATE_MACHINE: StateMachine[TenantStatus] = StateMachine(
    "tenant",
    {
        TenantStatus.PROVISIONING: frozenset({TenantStatus.ACTIVE, TenantStatus.FAILED}),
        TenantStatus.ACTIVE: frozenset({TenantStatus.UPDATING, TenantStatus.DESTROYING}),
        TenantStatus.UPDATING: frozenset({TenantStatus.ACTIVE, TenantStatus.FAILED}),
        TenantStatus.DESTROYING: frozenset({TenantStatus.DESTROYED, TenantStatus.FAILED}),
        TenantStatus.FAILED: frozenset({TenantStatus.DESTROYING}),
        TenantStatus.DESTROYED: frozenset(),
    },
)

TASK_STATE_MACHINE: StateMachine[TaskStatus] = StateMachine(
    "task",
    {
        TaskStatus.ACCEPTED: frozenset({TaskStatus.IN_PROGRESS}),
        TaskStatus.IN_PROGRESS: frozenset({TaskStatus.DONE, TaskStatus.FAILED}),
        TaskStatus.DONE: frozenset(),
        TaskStatus.FAILED: frozenset(),
    },
)

# Monotonic progress rank used for order tolerance: an update is only ever applied if
# it moves the task *forward*. Equal or lower rank means duplicate/stale -> ignored.
TASK_PROGRESS_RANK: Mapping[TaskStatus, int] = {
    TaskStatus.ACCEPTED: 0,
    TaskStatus.IN_PROGRESS: 1,
    TaskStatus.DONE: 2,
    TaskStatus.FAILED: 2,
}
