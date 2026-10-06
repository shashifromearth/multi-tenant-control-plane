from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from control_plane.application.dto import OutboxMessage
from control_plane.domain.clock import utc_now
from control_plane.infrastructure.database.models import OutboxMessageModel


class SqlOutboxRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    # --- write side (used inside business transactions)
    def add(self, message: OutboxMessage) -> None:
        self._session.add(
            OutboxMessageModel(
                id=message.id,
                event_type=message.event_type,
                exchange=message.exchange,
                routing_key=message.routing_key,
                payload=message.payload,
                headers=message.headers,
                correlation_id=message.correlation_id,
                created_at=utc_now(),
                attempts=0,
            )
        )
        self._session.flush()

    # --- relay side
    def claim_unpublished(self, limit: int) -> Sequence[OutboxMessageModel]:
        """Lock a batch of pending rows. SKIP LOCKED lets several relays run in parallel
        without double-claiming; rows stay locked until the relay's transaction ends."""
        return self._session.scalars(
            select(OutboxMessageModel)
            .where(OutboxMessageModel.published_at.is_(None))
            .order_by(OutboxMessageModel.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        ).all()

    def mark_published(self, message_id: UUID, at: datetime) -> None:
        self._session.execute(
            update(OutboxMessageModel)
            .where(OutboxMessageModel.id == message_id)
            .values(published_at=at, attempts=OutboxMessageModel.attempts + 1, last_error=None)
            .execution_options(synchronize_session=False)
        )

    def record_failure(self, message_id: UUID, error: str) -> None:
        self._session.execute(
            update(OutboxMessageModel)
            .where(OutboxMessageModel.id == message_id)
            .values(attempts=OutboxMessageModel.attempts + 1, last_error=error[:2000])
            .execution_options(synchronize_session=False)
        )

    def pending_stats(self) -> tuple[int, datetime | None]:
        count, oldest = self._session.execute(
            select(func.count(), func.min(OutboxMessageModel.created_at)).where(
                OutboxMessageModel.published_at.is_(None)
            )
        ).one()
        return int(count), oldest
