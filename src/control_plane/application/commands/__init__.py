"""Intent objects for state-changing use cases."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from control_plane.domain.enums import TaskStatus


@dataclass(frozen=True, slots=True)
class CreateTenant:
    slug: str
    name: str


@dataclass(frozen=True, slots=True)
class UpdateTenant:
    tenant_id: UUID
    expected_version: int
    name: str | None = None


@dataclass(frozen=True, slots=True)
class DeleteTenant:
    tenant_id: UUID
    expected_version: int | None = None


@dataclass(frozen=True, slots=True)
class ApplyTaskProgress:
    event_id: UUID
    task_id: UUID
    status: TaskStatus
    occurred_at: datetime
