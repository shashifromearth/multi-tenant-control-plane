"""Task resource: read-only over HTTP."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from control_plane.application.dto import TaskFilter
from control_plane.domain.enums import TaskStatus, TaskType
from control_plane.presentation.api.dependencies import PageDep, TaskServiceDep
from control_plane.presentation.schemas.common import ErrorResponse, PageResponse
from control_plane.presentation.schemas.task import TaskResponse

router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.get("", summary="List tasks")
def list_tasks(
    service: TaskServiceDep,
    page: PageDep,
    tenant_id: UUID | None = None,
    status_: Annotated[TaskStatus | None, Query(alias="status")] = None,
    type_: Annotated[TaskType | None, Query(alias="type")] = None,
) -> PageResponse[TaskResponse]:
    result = service.list(TaskFilter(tenant_id=tenant_id, status=status_, type=type_), page)
    return PageResponse[TaskResponse](
        items=[TaskResponse.from_domain(t) for t in result.items],
        total=result.total,
        limit=result.limit,
        offset=result.offset,
    )


@router.get(
    "/{task_id}",
    summary="Get a task",
    responses={404: {"model": ErrorResponse, "description": "task_not_found"}},
)
def get_task(task_id: UUID, service: TaskServiceDep) -> TaskResponse:
    return TaskResponse.from_domain(service.get(task_id))
