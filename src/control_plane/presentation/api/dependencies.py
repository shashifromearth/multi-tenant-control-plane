"""Lightweight dependency providers: the container lives on ``app.state``."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Query, Request

from control_plane.application.dto import PageRequest
from control_plane.application.services.task_service import TaskService
from control_plane.application.services.tenant_service import TenantService
from control_plane.container import Container

MAX_PAGE_SIZE = 100


def get_container(request: Request) -> Container:
    container: Container = request.app.state.container
    return container


def get_tenant_service(container: Annotated[Container, Depends(get_container)]) -> TenantService:
    return container.tenant_service


def get_task_service(container: Annotated[Container, Depends(get_container)]) -> TaskService:
    return container.task_service


def get_page(
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE, description="Page size")] = 20,
    offset: Annotated[int, Query(ge=0, description="Items to skip")] = 0,
) -> PageRequest:
    return PageRequest(limit=limit, offset=offset)


TenantServiceDep = Annotated[TenantService, Depends(get_tenant_service)]
TaskServiceDep = Annotated[TaskService, Depends(get_task_service)]
PageDep = Annotated[PageRequest, Depends(get_page)]
