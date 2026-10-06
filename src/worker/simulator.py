"""Provisioning worker simulator.

Consumes task messages, pretends to provision, and reports progress
(``in_progress`` then ``done``/``failed``) back to the control plane.

Event ids are *deterministic* -- ``uuid5(task_id, status)`` -- so if a task message is
redelivered (at-least-once) the worker re-emits the very same event ids and the control
plane deduplicates them exactly. Fault-injection knobs make negative scenarios
reproducible: failures, duplicate deliveries and out-of-order deliveries.
"""

from __future__ import annotations

import contextlib
import logging
import random
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, UUID, uuid5

import pika
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.exceptions import AMQPError
from pika.spec import Basic, BasicProperties
from pydantic import ValidationError

from contracts.messages import ProgressStatus, TaskMessage, TaskProgressEvent
from contracts.topology import (
    PERSISTENT_DELIVERY_MODE,
    TASK_CONTENT_TYPE,
    TASK_PROGRESS_EVENT,
    Topology,
)

logger = logging.getLogger("worker")

_EVENT_NAMESPACE = uuid5(NAMESPACE_URL, "urn:control-plane:task-progress")


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    amqp_url: str
    topology: Topology
    fail_rate: float = 0.0
    min_delay_ms: int = 500
    max_delay_ms: int = 2_000
    duplicate_rate: float = 0.0
    out_of_order_rate: float = 0.0
    prefetch: int = 4
    seed: int | None = None

    def __post_init__(self) -> None:
        for name in ("fail_rate", "duplicate_rate", "out_of_order_rate"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be within [0, 1], got {value}")
        if not 0 <= self.min_delay_ms <= self.max_delay_ms:
            raise ValueError("require 0 <= min_delay_ms <= max_delay_ms")


def progress_event_id(task_id: UUID, status: ProgressStatus) -> UUID:
    return uuid5(_EVENT_NAMESPACE, f"{task_id}:{status.value}")


def build_progress_event(
    task_id: UUID, status: ProgressStatus, reason: str | None = None
) -> TaskProgressEvent:
    return TaskProgressEvent(
        event_id=progress_event_id(task_id, status),
        task_id=task_id,
        status=status,
        timestamp=datetime.now(UTC),
        reason=reason,
    )


@dataclass(frozen=True, slots=True)
class Plan:
    """What the simulator will emit for one task, in order."""

    events: list[TaskProgressEvent]
    delays_s: list[float]


class Simulator:
    """Pure decision logic (no I/O) so behaviour is unit-testable with a fixed seed."""

    def __init__(self, config: WorkerConfig) -> None:
        self._config = config
        self._rng = random.Random(config.seed)  # simulation, not crypto  # nosec B311

    def plan(self, task: TaskMessage) -> Plan:
        cfg = self._config
        in_progress = build_progress_event(task.id, ProgressStatus.IN_PROGRESS)
        failed = self._rng.random() < cfg.fail_rate
        terminal = build_progress_event(
            task.id,
            ProgressStatus.FAILED if failed else ProgressStatus.DONE,
            reason="simulated provisioning failure" if failed else None,
        )
        events = [in_progress, terminal]
        if self._rng.random() < cfg.out_of_order_rate:
            events.reverse()  # terminal overtakes in_progress
        if self._rng.random() < cfg.duplicate_rate:
            events = [e for e in events for _ in range(2)]  # every event delivered twice
        delays = [self._delay() for _ in events]
        return Plan(events, delays)

    def _delay(self) -> float:
        return self._rng.uniform(self._config.min_delay_ms, self._config.max_delay_ms) / 1000


class Worker:
    def __init__(self, config: WorkerConfig) -> None:
        self._config = config
        self._simulator = Simulator(config)

    def run_forever(self, stop: threading.Event) -> None:
        delay = 0.5
        while not stop.is_set():
            connection: BlockingConnection | None = None
            try:
                params = pika.URLParameters(self._config.amqp_url)
                params.heartbeat = 30
                params.client_properties = {"connection_name": "worker-simulator"}
                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                channel.confirm_delivery()
                self._config.topology.declare(channel)
                channel.basic_qos(prefetch_count=self._config.prefetch)
                channel.basic_consume(
                    self._config.topology.worker_queue, self._on_message_factory(connection)
                )
                logger.info("worker connected", extra={"config": repr(self._config)})
                delay = 0.5
                while not stop.is_set():
                    connection.process_data_events(time_limit=1)
            except AMQPError as exc:
                logger.warning("worker broker error, reconnecting", extra={"error": repr(exc)})
                stop.wait(delay)
                delay = min(delay * 2, 30.0)
            finally:
                if connection is not None and connection.is_open:
                    with contextlib.suppress(AMQPError):  # best effort
                        connection.close()

    def _on_message_factory(
        self, connection: BlockingConnection
    ) -> Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]:
        def on_message(
            channel: BlockingChannel,
            method: Basic.Deliver,
            properties: BasicProperties,
            body: bytes,
        ) -> None:
            try:
                task = TaskMessage.model_validate_json(body)
            except ValidationError as exc:
                logger.error("poison task message rejected", extra={"error": str(exc)[:500]})
                channel.basic_reject(method.delivery_tag, requeue=False)  # -> worker DLQ
                return

            plan = self._simulator.plan(task)
            log = {"task_id": str(task.id), "type": task.type, "tenant": task.tenant.slug}
            logger.info("task received", extra={**log, "correlation_id": properties.correlation_id})
            for event, pause in zip(plan.events, plan.delays_s, strict=True):
                # connection.sleep keeps heartbeats flowing while we "work".
                connection.sleep(pause)
                self.publish(channel, event, properties.correlation_id)
                logger.info(
                    "progress published",
                    extra={**log, "status": event.status.value, "event_id": str(event.event_id)},
                )
            # Ack only after all progress is confirmed by the broker: a crash before this
            # point redelivers the task and we re-emit identical (deduplicated) events.
            channel.basic_ack(method.delivery_tag)

        return on_message

    def publish(
        self, channel: BlockingChannel, event: TaskProgressEvent, correlation_id: str | None
    ) -> None:
        channel.basic_publish(
            exchange=self._config.topology.progress_exchange,
            routing_key=self._config.topology.progress_routing_key(event.status.value),
            body=event.model_dump_json().encode(),
            properties=pika.BasicProperties(
                content_type=TASK_CONTENT_TYPE,
                delivery_mode=PERSISTENT_DELIVERY_MODE,
                message_id=str(event.event_id),
                correlation_id=correlation_id,
                type=TASK_PROGRESS_EVENT,
            ),
            mandatory=True,
        )
