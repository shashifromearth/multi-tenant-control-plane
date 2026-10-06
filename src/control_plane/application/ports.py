"""Abstractions the application layer depends on (Dependency Inversion).

Infrastructure provides SQLAlchemy implementations; tests may provide fakes. Protocols
keep this structural -- no inheritance ceremony required from implementations.
"""

from __future__ import annotations

from datetime import datetime
from types import TracebackType
from typing import Protocol, Self
from uuid import UUID

from control_plane.application.dto import OutboxMessage, PageRequest, TaskFilter, TenantFilter
from control_plane.domain.entities import Task, Tenant
from control_plane.domain.enums import TaskStatus


class TenantRepository(Protocol):
    def add(self, tenant: Tenant) -> None:
        """Insert; raises ``TenantAlreadyExists`` on a slug collision."""

    def get(self, tenant_id: UUID) -> Tenant | None: ...

    def update(self, tenant: Tenant, *, expected_version: int) -> None:
        """Compare-and-swap on ``version``; raises ``TenantVersionConflict`` if stale.
        On success ``tenant.version`` is set to the new (incremented) value."""

    def list(self, filters: TenantFilter, page: PageRequest) -> tuple[list[Tenant], int]: ...


class TaskRepository(Protocol):
    def add(self, task: Task) -> None: ...

    def get(self, task_id: UUID) -> Task | None: ...

    def get_for_update(self, task_id: UUID) -> Task | None:
        """Load and row-lock the task until the transaction ends."""

    def update(self, task: Task) -> None: ...

    def list(self, filters: TaskFilter, page: PageRequest) -> tuple[list[Task], int]: ...


class OutboxRepository(Protocol):
    def add(self, message: OutboxMessage) -> None: ...


class ProcessedEventRepository(Protocol):
    def try_record(
        self, *, event_id: UUID, task_id: UUID, status: TaskStatus, occurred_at: datetime
    ) -> bool:
        """Record an inbound event id. Returns False if it was already recorded."""

    def set_outcome(self, event_id: UUID, outcome: str) -> None: ...


class UnitOfWork(Protocol):
    # Read-only members so implementations may expose more specific repository types.
    @property
    def tenants(self) -> TenantRepository: ...

    @property
    def tasks(self) -> TaskRepository: ...

    @property
    def outbox(self) -> OutboxRepository: ...

    @property
    def processed_events(self) -> ProcessedEventRepository: ...

    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

    def commit(self) -> None: ...


class UnitOfWorkFactory(Protocol):
    def __call__(self) -> UnitOfWork: ...
