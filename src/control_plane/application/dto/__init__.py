"""Plain data carriers between layers (no behaviour)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from control_plane.domain.entities import Task, Tenant
from control_plane.domain.enums import TaskStatus, TaskType, TenantStatus


@dataclass(frozen=True, slots=True)
class PageRequest:
    limit: int = 20
    offset: int = 0


@dataclass(frozen=True, slots=True)
class Page[T]:
    items: list[T]
    total: int
    limit: int
    offset: int


@dataclass(frozen=True, slots=True)
class TenantFilter:
    status: TenantStatus | None = None
    slug: str | None = None


@dataclass(frozen=True, slots=True)
class TaskFilter:
    tenant_id: UUID | None = None
    status: TaskStatus | None = None
    type: TaskType | None = None


@dataclass(frozen=True, slots=True)
class TenantOperationResult:
    """A mutating tenant call returns the tenant and the task it spawned."""

    tenant: Tenant
    task: Task


@dataclass(frozen=True, slots=True)
class OutboxMessage:
    id: UUID  # == task id: "the task is the event"
    event_type: str
    exchange: str
    routing_key: str
    payload: dict[str, Any]
    correlation_id: str | None = None
    headers: dict[str, Any] = field(default_factory=dict)
