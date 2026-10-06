"""Read-only task queries (tasks are never mutated over HTTP)."""

from __future__ import annotations

from uuid import UUID

from control_plane.application.dto import Page, PageRequest, TaskFilter
from control_plane.application.ports import UnitOfWorkFactory
from control_plane.domain.entities import Task
from control_plane.domain.exceptions import TaskNotFound


class TaskService:
    def __init__(self, uow_factory: UnitOfWorkFactory) -> None:
        self._uow_factory = uow_factory

    def get(self, task_id: UUID) -> Task:
        with self._uow_factory() as uow:
            task = uow.tasks.get(task_id)
        if task is None:
            raise TaskNotFound(task_id)
        return task

    def list(self, filters: TaskFilter, page: PageRequest) -> Page[Task]:
        with self._uow_factory() as uow:
            items, total = uow.tasks.list(filters, page)
        return Page(items=items, total=total, limit=page.limit, offset=page.offset)
