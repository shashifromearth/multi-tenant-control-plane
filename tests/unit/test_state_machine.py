"""Exhaustive state-machine tests: every (from, to) pair is checked against the spec."""

from __future__ import annotations

import itertools

import pytest

from control_plane.domain.enums import TaskStatus, TenantStatus
from control_plane.domain.exceptions import InvalidStateTransition
from control_plane.domain.services.state_machine import (
    TASK_STATE_MACHINE,
    TENANT_STATE_MACHINE,
)

T = TenantStatus
TENANT_ALLOWED = {
    (T.PROVISIONING, T.ACTIVE),
    (T.PROVISIONING, T.FAILED),
    (T.ACTIVE, T.UPDATING),
    (T.UPDATING, T.ACTIVE),
    (T.UPDATING, T.FAILED),
    (T.ACTIVE, T.DESTROYING),
    (T.DESTROYING, T.DESTROYED),
    (T.DESTROYING, T.FAILED),
    (T.FAILED, T.DESTROYING),
}
K = TaskStatus
TASK_ALLOWED = {(K.ACCEPTED, K.IN_PROGRESS), (K.IN_PROGRESS, K.DONE), (K.IN_PROGRESS, K.FAILED)}


@pytest.mark.parametrize(("current", "target"), list(itertools.product(T, T)))
def test_tenant_transitions_match_spec(current: TenantStatus, target: TenantStatus) -> None:
    allowed = (current, target) in TENANT_ALLOWED
    assert TENANT_STATE_MACHINE.can_transition(current, target) is allowed
    if allowed:
        TENANT_STATE_MACHINE.ensure(current, target)
    else:
        with pytest.raises(InvalidStateTransition) as exc:
            TENANT_STATE_MACHINE.ensure(current, target)
        assert exc.value.current == current
        assert exc.value.target == target


@pytest.mark.parametrize(("current", "target"), list(itertools.product(K, K)))
def test_task_transitions_match_spec(current: TaskStatus, target: TaskStatus) -> None:
    allowed = (current, target) in TASK_ALLOWED
    assert TASK_STATE_MACHINE.can_transition(current, target) is allowed


def test_terminal_states() -> None:
    assert TENANT_STATE_MACHINE.is_terminal(T.DESTROYED)
    assert not TENANT_STATE_MACHINE.is_terminal(T.FAILED)  # failed -> destroying is allowed
    assert {s for s in K if TASK_STATE_MACHINE.is_terminal(s)} == {K.DONE, K.FAILED}
    assert {s for s in K if s.is_terminal} == {K.DONE, K.FAILED}


def test_allowed_from() -> None:
    assert TENANT_STATE_MACHINE.allowed_from(T.ACTIVE) == {T.UPDATING, T.DESTROYING}
