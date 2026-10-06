"""Composition root: the one place where concrete implementations are wired together.

No DI framework -- a plain object built once per process. FastAPI endpoints reach it
through tiny ``Depends`` providers, and tests build their own container.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from control_plane.application.services.task_progress_service import TaskProgressService
from control_plane.application.services.task_service import TaskService
from control_plane.application.services.tenant_service import TenantService
from control_plane.config import Settings
from control_plane.domain.services.tenant_lifecycle import TenantLifecycleService
from control_plane.infrastructure.database.session import build_engine, build_session_factory
from control_plane.infrastructure.database.unit_of_work import SqlAlchemyUnitOfWorkFactory


@dataclass(frozen=True, slots=True)
class Container:
    settings: Settings
    engine: Engine
    session_factory: sessionmaker[Session]
    tenant_service: TenantService
    task_service: TaskService
    task_progress_service: TaskProgressService

    def close(self) -> None:
        self.engine.dispose()


def build_container(settings: Settings) -> Container:
    engine = build_engine(settings)
    session_factory = build_session_factory(engine)
    uow_factory = SqlAlchemyUnitOfWorkFactory(session_factory)
    lifecycle = TenantLifecycleService()
    return Container(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
        tenant_service=TenantService(uow_factory, lifecycle, settings.topology),
        task_service=TaskService(uow_factory),
        task_progress_service=TaskProgressService(uow_factory, lifecycle),
    )
