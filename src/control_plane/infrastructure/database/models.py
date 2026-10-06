"""SQLAlchemy ORM models (persistence representation, separate from domain entities).

Constraint/index names are explicit so the migration, the models and the exception
translation in the repositories all refer to the same identifiers.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

TENANT_SLUG_UNIQUE = "uq_tenants_slug"
ONE_OPEN_TASK_PER_TENANT = "uq_tasks_one_open_per_tenant"

_TENANT_STATUSES = "'provisioning','active','updating','destroying','failed','destroyed'"
_TASK_STATUSES = "'accepted','in_progress','done','failed'"
_TASK_TYPES = "'deploy','update','destroy'"


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class TenantModel(Base):
    __tablename__ = "tenants"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    slug: Mapped[str] = mapped_column(String(28), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        # Unique across *all* tenants, including destroyed ones (slugs are never reused).
        Index(TENANT_SLUG_UNIQUE, "slug", unique=True),
        Index("ix_tenants_status", "status"),
        Index("ix_tenants_created_at_id", "created_at", "id"),
        CheckConstraint(f"status IN ({_TENANT_STATUSES})", name="status_valid"),
        CheckConstraint("version > 0", name="version_positive"),
    )


class TaskModel(Base):
    __tablename__ = "tasks"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    type: Mapped[str] = mapped_column(String(16), nullable=False)
    tenant_id: Mapped[UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_tasks_tenant_id_created_at", "tenant_id", "created_at"),
        Index("ix_tasks_status", "status"),
        Index("ix_tasks_created_at_id", "created_at", "id"),
        # Defence in depth: the tenant status guards already guarantee at most one open
        # task per tenant; the database refuses to ever store a violation of it.
        Index(
            ONE_OPEN_TASK_PER_TENANT,
            "tenant_id",
            unique=True,
            postgresql_where=text("status IN ('accepted','in_progress')"),
        ),
        CheckConstraint(f"status IN ({_TASK_STATUSES})", name="status_valid"),
        CheckConstraint(f"type IN ({_TASK_TYPES})", name="type_valid"),
    )


class OutboxMessageModel(Base):
    """Transactional outbox: written in the same transaction as the task it announces."""

    __tablename__ = "outbox_messages"

    id: Mapped[UUID] = mapped_column(primary_key=True)  # == task id
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    exchange: Mapped[str] = mapped_column(String(255), nullable=False)
    routing_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    headers: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    correlation_id: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    last_error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        # Small partial index: only the rows the relay still has to publish.
        Index(
            "ix_outbox_messages_unpublished",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )


class ProcessedEventModel(Base):
    """Inbox / idempotency store for worker progress events."""

    __tablename__ = "processed_events"

    event_id: Mapped[UUID] = mapped_column(primary_key=True)
    # No FK on purpose: an event for an unknown task must be rejected by the service
    # (as a poison message), not fail on a constraint before we can classify it.
    task_id: Mapped[UUID] = mapped_column(nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    outcome: Mapped[str | None] = mapped_column(String(16))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_processed_events_task_id", "task_id"),)
