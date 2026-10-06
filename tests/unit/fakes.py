"""In-memory implementations of the application ports (proves the layering: the
application layer runs without SQLAlchemy at all)."""

from __future__ import annotations

import copy
from datetime import datetime
from types import TracebackType
from typing import Self
from uuid import UUID

from control_plane.application.dto import OutboxMessage, PageRequest, TaskFilter, TenantFilter
from control_plane.domain.entities import Task, Tenant
from control_plane.domain.enums import TaskStatus
from control_plane.domain.exceptions import TenantAlreadyExists, TenantVersionConflict


class FakeStore:
    def __init__(self) -> None:
        self.tenants: dict[UUID, Tenant] = {}
        self.tasks: dict[UUID, Task] = {}
        self.outbox: list[OutboxMessage] = []
        self.events: dict[UUID, str | None] = {}


class _Tenants:
    def __init__(self, staged: FakeStore) -> None:
        self.s = staged

    def add(self, tenant: Tenant) -> None:
        if any(t.slug == tenant.slug for t in self.s.tenants.values()):
            raise TenantAlreadyExists(tenant.slug)
        self.s.tenants[tenant.id] = copy.deepcopy(tenant)

    def get(self, tenant_id: UUID) -> Tenant | None:
        t = self.s.tenants.get(tenant_id)
        return copy.deepcopy(t) if t else None

    def update(self, tenant: Tenant, *, expected_version: int) -> None:
        current = self.s.tenants.get(tenant.id)
        if current is None or current.version != expected_version:
            raise TenantVersionConflict(tenant.id, expected_version)
        tenant.version = expected_version + 1
        self.s.tenants[tenant.id] = copy.deepcopy(tenant)

    def list(self, filters: TenantFilter, page: PageRequest) -> tuple[list[Tenant], int]:
        items = [
            t
            for t in self.s.tenants.values()
            if filters.status in (None, t.status) and filters.slug in (None, t.slug)
        ]
        return items[page.offset : page.offset + page.limit], len(items)


class _Tasks:
    def __init__(self, staged: FakeStore) -> None:
        self.s = staged

    def add(self, task: Task) -> None:
        self.s.tasks[task.id] = copy.deepcopy(task)

    def get(self, task_id: UUID) -> Task | None:
        t = self.s.tasks.get(task_id)
        return copy.deepcopy(t) if t else None

    get_for_update = get

    def update(self, task: Task) -> None:
        self.s.tasks[task.id] = copy.deepcopy(task)

    def list(self, filters: TaskFilter, page: PageRequest) -> tuple[list[Task], int]:
        items = [t for t in self.s.tasks.values() if filters.tenant_id in (None, t.tenant_id)]
        return items[page.offset : page.offset + page.limit], len(items)


class _Outbox:
    def __init__(self, staged: FakeStore) -> None:
        self.s = staged

    def add(self, message: OutboxMessage) -> None:
        self.s.outbox.append(message)


class _Events:
    def __init__(self, staged: FakeStore) -> None:
        self.s = staged

    def try_record(
        self, *, event_id: UUID, task_id: UUID, status: TaskStatus, occurred_at: datetime
    ) -> bool:
        if event_id in self.s.events:
            return False
        self.s.events[event_id] = None
        return True

    def set_outcome(self, event_id: UUID, outcome: str) -> None:
        self.s.events[event_id] = outcome


class FakeUnitOfWork:
    """Transactional semantics: work on a copy, swap it in on commit."""

    def __init__(self, store: FakeStore) -> None:
        self.store = store
        self.committed = False

    def __enter__(self) -> Self:
        self._staged = copy.deepcopy(self.store)
        self.tenants = _Tenants(self._staged)
        self.tasks = _Tasks(self._staged)
        self.outbox = _Outbox(self._staged)
        self.processed_events = _Events(self._staged)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    def commit(self) -> None:
        self.store.__dict__.update(self._staged.__dict__)
        self.committed = True
