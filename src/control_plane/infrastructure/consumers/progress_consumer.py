"""Inbound consumer for worker progress events.

Delivery guarantees and failure handling:

* **At-least-once**: manual acks, sent only after the DB transaction committed (or after
  the message was safely re-routed to a retry/dead-letter queue).
* **Idempotent / order tolerant**: delegated to ``TaskProgressService``.
* **Transient failures** (DB down, optimistic-lock conflict, unexpected errors) are retried
  with increasing delays via per-tier TTL retry queues; after the last tier the message
  is parked in the DLQ.
* **Poison messages** (unparseable, schema-invalid, unknown task, impossible transition)
  go straight to the DLQ with the reason in a header. They never crash the consumer
  and never block the queue.
* **Broker failures**: the outer loop reconnects with exponential backoff; unacked
  messages are redelivered by RabbitMQ.
"""

from __future__ import annotations

import contextlib
import logging
import threading
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPError
from pika.spec import Basic, BasicProperties
from pydantic import ValidationError

from contracts.messages import TaskProgressEvent
from contracts.topology import PERSISTENT_DELIVERY_MODE, Topology
from control_plane.application.commands import ApplyTaskProgress
from control_plane.application.services.task_progress_service import TaskProgressService
from control_plane.correlation import correlation_scope
from control_plane.domain.enums import TaskStatus
from control_plane.domain.exceptions import DomainError, TenantVersionConflict
from control_plane.infrastructure.messaging.connection import Backoff, Heartbeat, connect

logger = logging.getLogger(__name__)

RETRY_COUNT_HEADER = "x-retry-count"
ERROR_HEADER = "x-last-error"
DEAD_LETTER_REASON_HEADER = "x-dead-letter-reason"


class Action(StrEnum):
    ACK = "ack"
    RETRY = "retry"
    DEAD_LETTER = "dead_letter"


@dataclass(frozen=True, slots=True)
class Disposition:
    action: Action
    reason: str = ""


class ProgressMessageHandler:
    """Broker-agnostic decision logic: bytes in, disposition out. Unit-testable."""

    def __init__(self, service: TaskProgressService) -> None:
        self._service = service

    def handle(self, body: bytes) -> Disposition:
        try:
            event = TaskProgressEvent.model_validate_json(body)
        except ValidationError as exc:
            return Disposition(Action.DEAD_LETTER, f"invalid message: {exc.errors()[:3]}")

        command = ApplyTaskProgress(
            event_id=event.event_id,
            task_id=event.task_id,
            status=TaskStatus(event.status.value),
            occurred_at=event.timestamp,
        )
        try:
            handling = self._service.apply(command)
        except TenantVersionConflict as exc:
            # A concurrent writer touched the tenant; re-reading on retry resolves it.
            return Disposition(Action.RETRY, exc.code)
        except DomainError as exc:
            # Deterministic business rejection: retrying cannot help.
            return Disposition(Action.DEAD_LETTER, f"{exc.code}: {exc.message}")
        except Exception as exc:
            logger.exception("progress event processing failed")
            return Disposition(Action.RETRY, repr(exc)[:500])
        return Disposition(Action.ACK, handling.value)


class ProgressConsumer:
    def __init__(
        self,
        handler: ProgressMessageHandler,
        amqp_url: str,
        topology: Topology,
        *,
        prefetch: int = 20,
        heartbeat: Heartbeat | None = None,
    ) -> None:
        self._handler = handler
        self._amqp_url = amqp_url
        self._topology = topology
        self._prefetch = prefetch
        self._heartbeat = heartbeat or Heartbeat(None)

    # ------------------------------------------------------------- message callback
    def on_message(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:
        with correlation_scope(properties.correlation_id):
            disposition = self._handler.handle(body)
            if disposition.action is Action.RETRY:
                attempt = _retry_count(properties) + 1
                if attempt > self._topology.max_retries:
                    disposition = Disposition(
                        Action.DEAD_LETTER, f"retries exhausted: {disposition.reason}"
                    )
                else:
                    self._republish(
                        channel,
                        self._topology.retry_queue(attempt),
                        properties,
                        body,
                        {RETRY_COUNT_HEADER: attempt, ERROR_HEADER: disposition.reason},
                    )
                    logger.warning(
                        "progress event scheduled for retry",
                        extra={"attempt": attempt, "reason": disposition.reason},
                    )
            if disposition.action is Action.DEAD_LETTER:
                self._republish(
                    channel,
                    self._topology.progress_dlq,
                    properties,
                    body,
                    {
                        DEAD_LETTER_REASON_HEADER: disposition.reason,
                        "x-original-routing-key": method.routing_key,
                    },
                )
                logger.error("progress event dead-lettered", extra={"reason": disposition.reason})
            # Ack last: if anything above raised, the message stays unacked and is
            # redelivered after reconnect (at-least-once).
            channel.basic_ack(delivery_tag=method.delivery_tag)

    @staticmethod
    def _republish(
        channel: BlockingChannel,
        queue: str,
        properties: BasicProperties,
        body: bytes,
        extra_headers: dict[str, Any],
    ) -> None:
        headers = dict(properties.headers or {})
        headers.update(extra_headers)
        channel.basic_publish(
            exchange="",  # default exchange routes by queue name
            routing_key=queue,
            body=body,
            properties=pika.BasicProperties(
                content_type=properties.content_type,
                delivery_mode=PERSISTENT_DELIVERY_MODE,
                message_id=properties.message_id,
                correlation_id=properties.correlation_id,
                type=properties.type,
                timestamp=properties.timestamp,
                headers=headers,
            ),
            mandatory=True,
        )

    # ------------------------------------------------------------- run loop
    def run_forever(self, stop: threading.Event) -> None:
        backoff = Backoff()
        while not stop.is_set():
            connection = None
            try:
                connection = connect(self._amqp_url, "control-plane-progress-consumer")
                channel = connection.channel()
                channel.confirm_delivery()
                self._topology.declare(channel)
                channel.basic_qos(prefetch_count=self._prefetch)
                channel.basic_consume(self._topology.progress_queue, self.on_message)
                logger.info(
                    "progress consumer connected", extra={"queue": self._topology.progress_queue}
                )
                backoff.reset()
                while not stop.is_set():
                    self._heartbeat.beat()
                    connection.process_data_events(time_limit=1)
            except AMQPError as exc:
                delay = backoff.next_delay()
                logger.warning(
                    "progress consumer broker error, reconnecting",
                    extra={"error": repr(exc), "retry_in_s": round(delay, 2)},
                )
                stop.wait(delay)
            finally:
                if connection is not None and connection.is_open:
                    with contextlib.suppress(AMQPError):  # best effort
                        connection.close()
        logger.info("progress consumer stopped")


def _retry_count(properties: BasicProperties) -> int:
    value = (properties.headers or {}).get(RETRY_COUNT_HEADER, 0)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
