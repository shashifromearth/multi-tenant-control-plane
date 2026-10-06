"""Initial schema: tenants, tasks, transactional outbox, processed events.

Revision ID: 0001
Revises:
Create Date: 2026-10-06
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TS = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "tenants",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("slug", sa.String(28), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("updated_at", TS, nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_tenants"),
        sa.CheckConstraint(
            "status IN ('provisioning','active','updating','destroying','failed','destroyed')",
            name="ck_tenants_status_valid",
        ),
        sa.CheckConstraint("version > 0", name="ck_tenants_version_positive"),
    )
    op.create_index("uq_tenants_slug", "tenants", ["slug"], unique=True)
    op.create_index("ix_tenants_status", "tenants", ["status"])
    op.create_index("ix_tenants_created_at_id", "tenants", ["created_at", "id"])

    op.create_table(
        "tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("type", sa.String(16), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("updated_at", TS, nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_tasks"),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_tasks_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "status IN ('accepted','in_progress','done','failed')", name="ck_tasks_status_valid"
        ),
        sa.CheckConstraint("type IN ('deploy','update','destroy')", name="ck_tasks_type_valid"),
    )
    op.create_index("ix_tasks_tenant_id_created_at", "tasks", ["tenant_id", "created_at"])
    op.create_index("ix_tasks_status", "tasks", ["status"])
    op.create_index("ix_tasks_created_at_id", "tasks", ["created_at", "id"])
    op.create_index(
        "uq_tasks_one_open_per_tenant",
        "tasks",
        ["tenant_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('accepted','in_progress')"),
    )

    op.create_table(
        "outbox_messages",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("exchange", sa.String(255), nullable=False),
        sa.Column("routing_key", sa.String(255), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "headers", postgresql.JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        sa.Column("correlation_id", sa.String(64), nullable=True),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("published_at", TS, nullable=True),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_outbox_messages"),
    )
    op.create_index(
        "ix_outbox_messages_unpublished",
        "outbox_messages",
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )

    op.create_table(
        "processed_events",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("outcome", sa.String(16), nullable=True),
        sa.Column("occurred_at", TS, nullable=False),
        sa.Column("processed_at", TS, nullable=False),
        sa.PrimaryKeyConstraint("event_id", name="pk_processed_events"),
    )
    op.create_index("ix_processed_events_task_id", "processed_events", ["task_id"])


def downgrade() -> None:
    op.drop_table("processed_events")
    op.drop_table("outbox_messages")
    op.drop_table("tasks")
    op.drop_table("tenants")
