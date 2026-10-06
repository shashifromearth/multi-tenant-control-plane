"""Message schemas (JSON bodies) exchanged over RabbitMQ.

Outbound (control plane -> worker): ``TaskMessage``. The task *is* the event: the body is
the persisted task itself, so the AMQP ``message_id`` equals the task id.

Inbound (worker -> control plane): ``TaskProgressEvent``. ``event_id`` is the idempotency
key; ``task_id`` references the task being updated.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

SCHEMA_VERSION: Literal[1] = 1


class _Message(BaseModel):
    # Forward compatible: unknown fields are ignored, never fatal.
    model_config = ConfigDict(frozen=True, extra="ignore")


class TenantRef(_Message):
    id: UUID
    slug: str
    name: str


class TaskMessage(_Message):
    schema_version: Literal[1] = 1
    id: UUID
    type: Literal["deploy", "update", "destroy"]
    tenant_id: UUID
    status: Literal["accepted", "in_progress", "done", "failed"]
    created_at: datetime
    updated_at: datetime
    tenant: TenantRef


class ProgressStatus(StrEnum):
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        return self is not ProgressStatus.IN_PROGRESS


class TaskProgressEvent(_Message):
    schema_version: Literal[1] = 1
    event_id: UUID = Field(description="Idempotency key; stable across redeliveries")
    task_id: UUID
    status: ProgressStatus
    timestamp: AwareDatetime = Field(description="When the worker observed the status")
    reason: str | None = Field(default=None, max_length=1000)
