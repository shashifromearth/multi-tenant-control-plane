"""Tenant use cases. Each mutating method is exactly one transaction that writes the
tenant, its new task and the outbox row together -- the event is published only after
this commit, by the outbox relay, never from inside the request."""

from __future__ import annotations

import logging
from uuid import UUID

from contracts.topology import Topology
from control_plane.application.commands import CreateTenant, DeleteTenant, UpdateTenant
from control_plane.application.dto import Page, PageRequest, TenantFilter, TenantOperationResult
from control_plane.application.ports import UnitOfWork, UnitOfWorkFactory
from control_plane.application.services.events import build_task_event
from control_plane.domain.entities import Task, Tenant
from control_plane.domain.exceptions import TenantNotFound, TenantVersionConflict
from control_plane.domain.services.tenant_lifecycle import TenantLifecycleService

logger = logging.getLogger(__name__)


class TenantService:
    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        lifecycle: TenantLifecycleService,
        topology: Topology,
    ) -> None:
        self._uow_factory = uow_factory
        self._lifecycle = lifecycle
        self._topology = topology

    # ------------------------------------------------------------------ commands
    def create(self, cmd: CreateTenant) -> TenantOperationResult:
        tenant, task = self._lifecycle.provision(slug=cmd.slug, name=cmd.name)
        with self._uow_factory() as uow:
            uow.tenants.add(tenant)  # unique constraint decides slug races
            self._record_task(uow, task, tenant)
            uow.commit()
        logger.info(
            "tenant created",
            extra={"tenant_id": str(tenant.id), "slug": tenant.slug, "task_id": str(task.id)},
        )
        return TenantOperationResult(tenant, task)

    def update(self, cmd: UpdateTenant) -> TenantOperationResult:
        with self._uow_factory() as uow:
            tenant = self._load(uow, cmd.tenant_id)
            # Fail fast on a stale client view; the versioned UPDATE below closes the
            # race window between this read and the write.
            self._check_version(tenant, cmd.expected_version)
            task = self._lifecycle.request_update(tenant, name=cmd.name)
            uow.tenants.update(tenant, expected_version=cmd.expected_version)
            self._record_task(uow, task, tenant)
            uow.commit()
        logger.info("tenant update requested", extra={"tenant_id": str(tenant.id)})
        return TenantOperationResult(tenant, task)

    def delete(self, cmd: DeleteTenant) -> TenantOperationResult:
        with self._uow_factory() as uow:
            tenant = self._load(uow, cmd.tenant_id)
            if cmd.expected_version is not None:
                self._check_version(tenant, cmd.expected_version)
            loaded_version = tenant.version
            task = self._lifecycle.request_destroy(tenant)
            uow.tenants.update(tenant, expected_version=loaded_version)
            self._record_task(uow, task, tenant)
            uow.commit()
        logger.info("tenant destroy requested", extra={"tenant_id": str(tenant.id)})
        return TenantOperationResult(tenant, task)

    # ------------------------------------------------------------------ queries
    def get(self, tenant_id: UUID) -> Tenant:
        with self._uow_factory() as uow:
            return self._load(uow, tenant_id)

    def list(self, filters: TenantFilter, page: PageRequest) -> Page[Tenant]:
        with self._uow_factory() as uow:
            items, total = uow.tenants.list(filters, page)
        return Page(items=items, total=total, limit=page.limit, offset=page.offset)

    # ------------------------------------------------------------------ helpers
    def _record_task(self, uow: UnitOfWork, task: Task, tenant: Tenant) -> None:
        uow.tasks.add(task)
        uow.outbox.add(build_task_event(task, tenant, self._topology))

    @staticmethod
    def _load(uow: UnitOfWork, tenant_id: UUID) -> Tenant:
        tenant = uow.tenants.get(tenant_id)
        if tenant is None:
            raise TenantNotFound(tenant_id)
        return tenant

    @staticmethod
    def _check_version(tenant: Tenant, expected_version: int) -> None:
        if tenant.version != expected_version:
            raise TenantVersionConflict(tenant.id, expected_version)
