"""Domain entities.

Plain Python objects with no persistence concerns. ``status`` is read-only: the only way
to change it is ``transition_to``, which is validated by the corresponding state machine.

``version`` is owned by persistence (the optimistic-locking UPDATE increments it and the
repository writes the new value back), so the domain never bumps it by hand.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID, uuid4

from control_plane.domain.clock import utc_now
from control_plane.domain.enums import TaskStatus, TaskType, TenantStatus
from control_plane.domain.services.state_machine import (
    TASK_STATE_MACHINE,
    TENANT_STATE_MACHINE,
)
from control_plane.domain.validation import validate_name, validate_slug


@dataclass(eq=False, kw_only=True)
class Tenant:
    id: UUID
    slug: str
    name: str
    _status: TenantStatus = field(repr=False)
    version: int
    created_at: datetime
    updated_at: datetime

    @classmethod
    def register(cls, *, slug: str, name: str) -> Tenant:
        now = utc_now()
        return cls(
            id=uuid4(),
            slug=validate_slug(slug),
            name=validate_name(name),
            _status=TenantStatus.PROVISIONING,
            version=1,
            created_at=now,
            updated_at=now,
        )

    @classmethod
    def restore(
        cls,
        *,
        id: UUID,
        slug: str,
        name: str,
        status: TenantStatus,
        version: int,
        created_at: datetime,
        updated_at: datetime,
    ) -> Tenant:
        """Rehydrate from storage (no validation: stored data is already valid)."""
        return cls(
            id=id,
            slug=slug,
            name=name,
            _status=status,
            version=version,
            created_at=created_at,
            updated_at=updated_at,
        )

    @property
    def status(self) -> TenantStatus:
        return self._status

    def transition_to(self, target: TenantStatus) -> None:
        TENANT_STATE_MACHINE.ensure(self._status, target)
        self._status = target
        self.updated_at = utc_now()

    def rename(self, name: str) -> None:
        self.name = validate_name(name)
        self.updated_at = utc_now()

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Tenant) and other.id == self.id

    def __hash__(self) -> int:
        return hash(self.id)


@dataclass(eq=False, kw_only=True)
class Task:
    id: UUID
    type: TaskType
    tenant_id: UUID
    _status: TaskStatus = field(repr=False)
    created_at: datetime
    updated_at: datetime

    @classmethod
    def accept(cls, *, type: TaskType, tenant_id: UUID) -> Task:
        now = utc_now()
        return cls(
            id=uuid4(),
            type=type,
            tenant_id=tenant_id,
            _status=TaskStatus.ACCEPTED,
            created_at=now,
            updated_at=now,
        )

    @classmethod
    def restore(
        cls,
        *,
        id: UUID,
        type: TaskType,
        tenant_id: UUID,
        status: TaskStatus,
        created_at: datetime,
        updated_at: datetime,
    ) -> Task:
        return cls(
            id=id,
            type=type,
            tenant_id=tenant_id,
            _status=status,
            created_at=created_at,
            updated_at=updated_at,
        )

    @property
    def status(self) -> TaskStatus:
        return self._status

    def transition_to(self, target: TaskStatus) -> None:
        TASK_STATE_MACHINE.ensure(self._status, target)
        self._status = target
        self.updated_at = utc_now()

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Task) and other.id == self.id

    def __hash__(self) -> int:
        return hash(self.id)


__all__ = ["Task", "Tenant"]
