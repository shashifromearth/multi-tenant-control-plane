from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from control_plane.domain.clock import utc_now
from control_plane.domain.enums import TaskStatus
from control_plane.infrastructure.database.models import ProcessedEventModel


class SqlProcessedEventRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def try_record(
        self, *, event_id: UUID, task_id: UUID, status: TaskStatus, occurred_at: datetime
    ) -> bool:
        # Atomic check-and-set: a concurrent insert of the same id blocks on the primary
        # key until the other transaction finishes, then becomes a no-op.
        stmt = (
            insert(ProcessedEventModel)
            .values(
                event_id=event_id,
                task_id=task_id,
                status=status.value,
                occurred_at=occurred_at,
                processed_at=utc_now(),
            )
            .on_conflict_do_nothing(index_elements=[ProcessedEventModel.event_id])
            .returning(ProcessedEventModel.event_id)
        )
        return self._session.execute(stmt).scalar_one_or_none() is not None

    def set_outcome(self, event_id: UUID, outcome: str) -> None:
        self._session.execute(
            update(ProcessedEventModel)
            .where(ProcessedEventModel.event_id == event_id)
            .values(outcome=outcome)
            .execution_options(synchronize_session=False)
        )
