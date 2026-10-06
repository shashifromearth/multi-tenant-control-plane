"""Transactional-outbox relay.

Polls committed-but-unpublished outbox rows and publishes them with publisher confirms.
A row is marked published only after the broker confirmed it, inside the same DB
transaction that claimed it. Consequences:

* An event can never be published for a rolled-back API transaction (the row would not
  exist / not be visible).
* If the relay crashes between broker confirm and DB commit, the row is published again
  on restart -> at-least-once delivery. Consumers are idempotent by design.
"""

from __future__ import annotations

import contextlib
import json
import logging
import threading

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPError, NackError, UnroutableError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from contracts.topology import PERSISTENT_DELIVERY_MODE, TASK_CONTENT_TYPE, Topology
from control_plane.correlation import correlation_scope
from control_plane.domain.clock import utc_now
from control_plane.infrastructure.database.models import OutboxMessageModel
from control_plane.infrastructure.messaging.connection import Backoff, Heartbeat, connect
from control_plane.infrastructure.repositories.outbox_repository import SqlOutboxRepository

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        amqp_url: str,
        topology: Topology,
        *,
        batch_size: int = 50,
        poll_interval_s: float = 0.5,
        heartbeat: Heartbeat | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._amqp_url = amqp_url
        self._topology = topology
        self._batch_size = batch_size
        self._poll_interval_s = poll_interval_s
        self._heartbeat = heartbeat or Heartbeat(None)

    def publish_pending(self, channel: BlockingChannel) -> int:
        """Publish one batch. Returns how many rows were published."""
        published = 0
        with self._session_factory() as session:
            repo = SqlOutboxRepository(session)
            for row in repo.claim_unpublished(self._batch_size):
                try:
                    self._publish(channel, row)
                except (UnroutableError, NackError) as exc:
                    # Broker is up but refused this message: keep it pending, record why.
                    logger.error(
                        "outbox message rejected by broker",
                        extra={"outbox_id": str(row.id), "error": repr(exc)},
                    )
                    repo.record_failure(row.id, repr(exc))
                    continue
                repo.mark_published(row.id, utc_now())
                published += 1
            session.commit()
        return published

    def _publish(self, channel: BlockingChannel, row: OutboxMessageModel) -> None:
        with correlation_scope(row.correlation_id):
            channel.basic_publish(
                exchange=row.exchange,
                routing_key=row.routing_key,
                body=json.dumps(row.payload).encode(),
                properties=pika.BasicProperties(
                    content_type=TASK_CONTENT_TYPE,
                    delivery_mode=PERSISTENT_DELIVERY_MODE,
                    message_id=str(row.id),
                    correlation_id=row.correlation_id,
                    type=row.event_type,
                    timestamp=int(row.created_at.timestamp()),
                    headers=row.headers or None,
                ),
                mandatory=True,  # unroutable -> UnroutableError instead of silent drop
            )
            logger.info(
                "outbox message published",
                extra={"outbox_id": str(row.id), "routing_key": row.routing_key},
            )

    def run_forever(self, stop: threading.Event) -> None:
        backoff = Backoff()
        while not stop.is_set():
            connection = None
            try:
                connection = connect(self._amqp_url, "control-plane-outbox-relay")
                channel = connection.channel()
                channel.confirm_delivery()
                self._topology.declare(channel)
                logger.info("outbox relay connected")
                backoff.reset()
                while not stop.is_set():
                    self._heartbeat.beat()
                    if self.publish_pending(channel) < self._batch_size:
                        # Idle (or partially full batch): wait, while servicing heartbeats.
                        connection.sleep(self._poll_interval_s)
            except (AMQPError, SQLAlchemyError) as exc:
                delay = backoff.next_delay()
                logger.warning(
                    "outbox relay error, retrying",
                    extra={"error": repr(exc), "retry_in_s": round(delay, 2)},
                )
                stop.wait(delay)
            finally:
                if connection is not None and connection.is_open:
                    with contextlib.suppress(AMQPError):  # best effort
                        connection.close()
        logger.info("outbox relay stopped")
