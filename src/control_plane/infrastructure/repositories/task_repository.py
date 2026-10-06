from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import Select, func, select, update
from sqlalchemy.orm import Session

from control_plane.application.dto import PageRequest, TaskFilter
from control_plane.domain.entities import Task
from control_plane.infrastructure.database.models import TaskModel
from control_plane.infrastructure.repositories._mapping import task_to_domain


class SqlTaskRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, task: Task) -> None:
        self._session.add(
            TaskModel(
                id=task.id,
                type=task.type.value,
                tenant_id=task.tenant_id,
                status=task.status.value,
                created_at=task.created_at,
                updated_at=task.updated_at,
            )
        )
        self._session.flush()

    def get(self, task_id: UUID) -> Task | None:
        row = self._session.get(TaskModel, task_id)
        return task_to_domain(row) if row else None

    def get_for_update(self, task_id: UUID) -> Task | None:
        row = self._session.scalars(
            select(TaskModel).where(TaskModel.id == task_id).with_for_update()
        ).one_or_none()
        return task_to_domain(row) if row else None

    def update(self, task: Task) -> None:
        self._session.execute(
            update(TaskModel)
            .where(TaskModel.id == task.id)
            .values(status=task.status.value, updated_at=task.updated_at)
            .execution_options(synchronize_session=False)
        )

    def list(self, filters: TaskFilter, page: PageRequest) -> tuple[list[Task], int]:
        stmt: Select[Any] = select(TaskModel)
        if filters.tenant_id is not None:
            stmt = stmt.where(TaskModel.tenant_id == filters.tenant_id)
        if filters.status is not None:
            stmt = stmt.where(TaskModel.status == filters.status.value)
        if filters.type is not None:
            stmt = stmt.where(TaskModel.type == filters.type.value)
        total = self._session.execute(
            select(func.count()).select_from(stmt.subquery())
        ).scalar_one()
        rows = self._session.scalars(
            stmt.order_by(TaskModel.created_at.desc(), TaskModel.id.desc())
            .limit(page.limit)
            .offset(page.offset)
        ).all()
        return [task_to_domain(r) for r in rows], total
