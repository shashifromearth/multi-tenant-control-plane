from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel

from control_plane.domain.entities import Task
from control_plane.domain.enums import TaskStatus, TaskType
from control_plane.presentation.schemas.common import Timestamp


class TaskResponse(BaseModel):
    id: UUID
    type: TaskType
    tenant_id: UUID
    status: TaskStatus
    created_at: Timestamp
    updated_at: Timestamp

    @classmethod
    def from_domain(cls, task: Task) -> TaskResponse:
        return cls(
            id=task.id,
            type=task.type,
            tenant_id=task.tenant_id,
            status=task.status,
            created_at=task.created_at,
            updated_at=task.updated_at,
        )
