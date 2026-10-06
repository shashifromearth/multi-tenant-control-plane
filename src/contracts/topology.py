"""RabbitMQ topology: names and an idempotent ``declare`` used by both sides.

Declaring from one place guarantees both processes use identical queue arguments
(mismatched arguments make RabbitMQ refuse the declaration with PRECONDITION_FAILED).

    tasks exchange    --task.<type>-->          worker queue   --(reject)-->    worker DLQ
    progress exchange --task.progress.<s>-->    progress queue --(poison)-->    progress DLQ
                                                  ^   |
                             (TTL expires) -------+   +--(transient)-->  retry.<n>ms queues
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

TASK_CONTENT_TYPE = "application/json"
PERSISTENT_DELIVERY_MODE = 2  # survive broker restarts (with durable queues)
TASK_CREATED_EVENT = "task.created"
TASK_PROGRESS_EVENT = "task.progress"


@dataclass(frozen=True, slots=True)
class Topology:
    prefix: str = "cp"
    retry_delays_ms: tuple[int, ...] = (1_000, 5_000, 30_000)

    # --- exchanges
    @property
    def tasks_exchange(self) -> str:
        return f"{self.prefix}.tasks"

    @property
    def progress_exchange(self) -> str:
        return f"{self.prefix}.task-progress"

    # --- queues
    @property
    def worker_queue(self) -> str:
        return f"{self.prefix}.worker.tasks"

    @property
    def worker_dlq(self) -> str:
        return f"{self.worker_queue}.dlq"

    @property
    def progress_queue(self) -> str:
        return f"{self.prefix}.control-plane.task-progress"

    @property
    def progress_dlq(self) -> str:
        return f"{self.progress_queue}.dlq"

    def retry_queue(self, attempt: int) -> str:
        """Retry tier for the given 1-based attempt (capped at the last tier)."""
        tier = min(attempt, len(self.retry_delays_ms)) - 1
        return f"{self.progress_queue}.retry.{self.retry_delays_ms[tier]}ms"

    @property
    def max_retries(self) -> int:
        return len(self.retry_delays_ms)

    # --- routing keys
    @staticmethod
    def task_routing_key(task_type: str) -> str:
        return f"task.{task_type}"

    @staticmethod
    def progress_routing_key(status: str) -> str:
        return f"task.progress.{status}"

    def all_queues(self) -> Sequence[str]:
        return (
            self.worker_queue,
            self.worker_dlq,
            self.progress_queue,
            self.progress_dlq,
            *(self.retry_queue(i + 1) for i in range(len(self.retry_delays_ms))),
        )

    def declare(self, channel: Any) -> None:
        """Idempotently declare every exchange, queue and binding."""
        channel.exchange_declare(self.tasks_exchange, exchange_type="topic", durable=True)
        channel.exchange_declare(self.progress_exchange, exchange_type="topic", durable=True)

        # Worker side: rejected (poison) task messages are dead-lettered to its DLQ.
        channel.queue_declare(self.worker_dlq, durable=True)
        channel.queue_declare(
            self.worker_queue,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": self.worker_dlq,
            },
        )
        channel.queue_bind(self.worker_queue, self.tasks_exchange, routing_key="task.#")

        # Control-plane side.
        channel.queue_declare(self.progress_dlq, durable=True)
        channel.queue_declare(self.progress_queue, durable=True)
        channel.queue_bind(
            self.progress_queue, self.progress_exchange, routing_key="task.progress.#"
        )
        # One queue per backoff tier with a fixed TTL avoids the head-of-line blocking a
        # single queue with per-message TTLs would cause. Expired messages are
        # dead-lettered straight back to the progress queue via the default exchange.
        for delay in self.retry_delays_ms:
            channel.queue_declare(
                f"{self.progress_queue}.retry.{delay}ms",
                durable=True,
                arguments={
                    "x-message-ttl": delay,
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": self.progress_queue,
                },
            )
