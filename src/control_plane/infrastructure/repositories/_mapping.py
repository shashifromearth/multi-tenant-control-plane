"""Domain <-> ORM mapping (kept in one place so repositories stay declarative)."""

from __future__ import annotations

from control_plane.domain.entities import Task, Tenant
from control_plane.domain.enums import TaskStatus, TaskType, TenantStatus
from control_plane.infrastructure.database.models import TaskModel, TenantModel


def tenant_to_domain(row: TenantModel) -> Tenant:
    return Tenant.restore(
        id=row.id,
        slug=row.slug,
        name=row.name,
        status=TenantStatus(row.status),
        version=row.version,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def task_to_domain(row: TaskModel) -> Task:
    return Task.restore(
        id=row.id,
        type=TaskType(row.type),
        tenant_id=row.tenant_id,
        status=TaskStatus(row.status),
        created_at=row.created_at,
        updated_at=row.updated_at,
    )
