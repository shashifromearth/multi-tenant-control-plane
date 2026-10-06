"""Applies worker progress events. Idempotent and order tolerant.

One transaction per event:

1. ``INSERT .. ON CONFLICT DO NOTHING`` the event id -> a duplicate is detected
   atomically (even across concurrent consumers) and short-circuits with no effects.
2. ``SELECT .. FOR UPDATE`` the task -> progress for one task is serialised.
3. The domain service decides (forward-only) and the tenant is written with the same
   optimistic-locking UPDATE the API uses, so a worker-applied change bumps ``version``.

If anything fails the transaction rolls back *including* the event-id record, so a
retried delivery is processed from scratch.
"""

from __future__ import annotations

import logging
from enum import StrEnum

from control_plane.application.commands import ApplyTaskProgress
from control_plane.application.ports import UnitOfWorkFactory
from control_plane.domain.exceptions import TaskNotFound
from control_plane.domain.services.tenant_lifecycle import TenantLifecycleService

logger = logging.getLogger(__name__)


class ProgressHandling(StrEnum):
    APPLIED = "applied"
    STALE = "stale"
    DUPLICATE = "duplicate"


class TaskProgressService:
    def __init__(self, uow_factory: UnitOfWorkFactory, lifecycle: TenantLifecycleService) -> None:
        self._uow_factory = uow_factory
        self._lifecycle = lifecycle

    def apply(self, cmd: ApplyTaskProgress) -> ProgressHandling:
        log_ctx = {
            "event_id": str(cmd.event_id),
            "task_id": str(cmd.task_id),
            "reported_status": cmd.status.value,
        }
        with self._uow_factory() as uow:
            is_new = uow.processed_events.try_record(
                event_id=cmd.event_id,
                task_id=cmd.task_id,
                status=cmd.status,
                occurred_at=cmd.occurred_at,
            )
            if not is_new:
                logger.info("duplicate progress event ignored", extra=log_ctx)
                return ProgressHandling.DUPLICATE

            task = uow.tasks.get_for_update(cmd.task_id)
            if task is None:
                raise TaskNotFound(cmd.task_id)
            tenant = uow.tenants.get(task.tenant_id)
            if tenant is None:  # pragma: no cover - guaranteed by the FK
                raise RuntimeError(f"task {task.id} references a missing tenant")

            loaded_version = tenant.version
            outcome = self._lifecycle.apply_progress(task=task, tenant=tenant, reported=cmd.status)
            if outcome.applied:
                uow.tasks.update(task)
                if outcome.tenant_changed:
                    uow.tenants.update(tenant, expected_version=loaded_version)
            uow.processed_events.set_outcome(cmd.event_id, outcome.result.value)
            uow.commit()

        handling = ProgressHandling(outcome.result.value)
        logger.info(
            "progress event processed",
            extra={
                **log_ctx,
                "handling": handling.value,
                "task_status": task.status.value,
                "tenant_status": tenant.status.value,
            },
        )
        return handling
