"""Transaction boundaries, optimistic locking and schema integrity on real PostgreSQL."""

from __future__ import annotations

import uuid

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from control_plane.application.commands import CreateTenant
from control_plane.container import Container
from control_plane.domain.entities import Task, Tenant
from control_plane.domain.enums import TaskType, TenantStatus
from control_plane.domain.exceptions import TenantVersionConflict
from control_plane.infrastructure.database.models import (
    Base,
    OutboxMessageModel,
    TaskModel,
    TenantModel,
)
from control_plane.infrastructure.database.unit_of_work import SqlAlchemyUnitOfWork

pytestmark = pytest.mark.integration


def counts(container: Container) -> tuple[int, int, int]:
    with container.session_factory() as s:
        return (
            s.scalar(select(func.count()).select_from(TenantModel)) or 0,
            s.scalar(select(func.count()).select_from(TaskModel)) or 0,
            s.scalar(select(func.count()).select_from(OutboxMessageModel)) or 0,
        )


def test_failure_after_writes_rolls_back_tenant_task_and_outbox(
    container: Container, monkeypatch: pytest.MonkeyPatch
) -> None:
    from control_plane.infrastructure.repositories import outbox_repository

    def explode(self: object, message: object) -> None:
        raise RuntimeError("crash between task insert and outbox insert")

    monkeypatch.setattr(outbox_repository.SqlOutboxRepository, "add", explode)
    with pytest.raises(RuntimeError):
        container.tenant_service.create(CreateTenant(slug="acme", name="ACME"))
    # Nothing half-written: in particular no event exists for a tenant that doesn't.
    assert counts(container) == (0, 0, 0)


def test_unit_of_work_without_commit_persists_nothing(container: Container) -> None:
    with SqlAlchemyUnitOfWork(container.session_factory) as uow:
        uow.tenants.add(Tenant.register(slug="ghost", name="Ghost"))
    assert counts(container) == (0, 0, 0)


def test_unit_of_work_outside_context_is_an_error(container: Container) -> None:
    with pytest.raises(RuntimeError):
        SqlAlchemyUnitOfWork(container.session_factory).commit()


def test_versioned_update_is_compare_and_swap(container: Container) -> None:
    """Two writers that both read version 1: the second write must fail."""
    tenant_id = container.tenant_service.create(CreateTenant(slug="acme", name="A")).tenant.id

    with (
        SqlAlchemyUnitOfWork(container.session_factory) as first,
        SqlAlchemyUnitOfWork(container.session_factory) as second,
    ):
        a = first.tenants.get(tenant_id)
        b = second.tenants.get(tenant_id)
        assert a is not None
        assert b is not None
        assert a.version == b.version == 1

        a.rename("from A")
        first.tenants.update(a, expected_version=1)
        first.commit()
        assert a.version == 2

        b.rename("from B")
        with pytest.raises(TenantVersionConflict):
            second.tenants.update(b, expected_version=1)

    assert container.tenant_service.get(tenant_id).name == "from A"


def test_database_refuses_second_open_task_per_tenant(container: Container) -> None:
    tenant_id = container.tenant_service.create(CreateTenant(slug="acme", name="A")).tenant.id
    with SqlAlchemyUnitOfWork(container.session_factory) as uow, pytest.raises(IntegrityError):
        uow.tasks.add(Task.accept(type=TaskType.UPDATE, tenant_id=tenant_id))


def test_database_refuses_invalid_status(container: Container) -> None:
    with container.session_factory() as s:
        tenant = Tenant.register(slug="acme", name="A")
        s.add(
            TenantModel(
                id=uuid.uuid4(),
                slug="x-y-z",
                name="x",
                status="bogus",
                version=1,
                created_at=tenant.created_at,
                updated_at=tenant.updated_at,
            )
        )
        with pytest.raises(IntegrityError):
            s.commit()


def test_migrations_match_models(container: Container) -> None:
    """Alembic migration and ORM models must describe the same schema."""
    with container.engine.connect() as conn:
        diff = compare_metadata(MigrationContext.configure(conn), Base.metadata)
    assert diff == []


def test_restore_round_trip(container: Container) -> None:
    created = container.tenant_service.create(CreateTenant(slug="acme", name="A")).tenant
    loaded = container.tenant_service.get(created.id)
    assert (loaded.slug, loaded.name, loaded.status, loaded.version) == (
        "acme",
        "A",
        TenantStatus.PROVISIONING,
        1,
    )
    assert loaded.created_at == created.created_at
