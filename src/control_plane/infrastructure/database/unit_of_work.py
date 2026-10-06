"""SQLAlchemy Unit of Work: one session == one transaction == one use case.

Nothing is committed unless ``commit()`` is called explicitly; leaving the ``with`` block
(normally or via an exception) without committing rolls everything back.
"""

from __future__ import annotations

from types import TracebackType
from typing import Self

from sqlalchemy.orm import Session, sessionmaker

from control_plane.infrastructure.repositories.outbox_repository import SqlOutboxRepository
from control_plane.infrastructure.repositories.processed_event_repository import (
    SqlProcessedEventRepository,
)
from control_plane.infrastructure.repositories.task_repository import SqlTaskRepository
from control_plane.infrastructure.repositories.tenant_repository import SqlTenantRepository


class SqlAlchemyUnitOfWork:
    tenants: SqlTenantRepository
    tasks: SqlTaskRepository
    outbox: SqlOutboxRepository
    processed_events: SqlProcessedEventRepository

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory
        self._session: Session | None = None

    def __enter__(self) -> Self:
        session = self._session_factory()
        self._session = session
        self.tenants = SqlTenantRepository(session)
        self.tasks = SqlTaskRepository(session)
        self.outbox = SqlOutboxRepository(session)
        self.processed_events = SqlProcessedEventRepository(session)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        session = self._require_session()
        try:
            session.rollback()  # no-op after a successful commit
        finally:
            session.close()
            self._session = None

    def commit(self) -> None:
        self._require_session().commit()

    def _require_session(self) -> Session:
        if self._session is None:
            raise RuntimeError("UnitOfWork used outside of its context manager")
        return self._session


class SqlAlchemyUnitOfWorkFactory:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def __call__(self) -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(self._session_factory)
