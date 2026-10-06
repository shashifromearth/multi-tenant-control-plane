from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import Select, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from control_plane.application.dto import PageRequest, TenantFilter
from control_plane.domain.entities import Tenant
from control_plane.domain.exceptions import TenantAlreadyExists, TenantVersionConflict
from control_plane.infrastructure.database.models import TENANT_SLUG_UNIQUE, TenantModel
from control_plane.infrastructure.repositories._mapping import tenant_to_domain


def _violated_constraint(exc: IntegrityError) -> str | None:
    diag = getattr(exc.orig, "diag", None)
    return getattr(diag, "constraint_name", None)


class SqlTenantRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, tenant: Tenant) -> None:
        self._session.add(
            TenantModel(
                id=tenant.id,
                slug=tenant.slug,
                name=tenant.name,
                status=tenant.status.value,
                version=tenant.version,
                created_at=tenant.created_at,
                updated_at=tenant.updated_at,
            )
        )
        try:
            # Flush now so a slug collision surfaces here, as a business error. Under a
            # concurrent create the second INSERT blocks on the unique index until the
            # first commits, then fails -- exactly one winner, no pre-check race.
            self._session.flush()
        except IntegrityError as exc:
            if _violated_constraint(exc) == TENANT_SLUG_UNIQUE:
                raise TenantAlreadyExists(tenant.slug) from exc
            raise

    def get(self, tenant_id: UUID) -> Tenant | None:
        row = self._session.get(TenantModel, tenant_id)
        return tenant_to_domain(row) if row else None

    def update(self, tenant: Tenant, *, expected_version: int) -> None:
        """Optimistic lock: compare-and-swap on ``version`` in a single statement.

        UPDATE tenants SET ..., version = version + 1
         WHERE id = :id AND version = :expected
        RETURNING version
        """
        stmt = (
            update(TenantModel)
            .where(TenantModel.id == tenant.id, TenantModel.version == expected_version)
            .values(
                name=tenant.name,
                status=tenant.status.value,
                updated_at=tenant.updated_at,
                version=TenantModel.version + 1,
            )
            .returning(TenantModel.version)
            .execution_options(synchronize_session=False)
        )
        new_version = self._session.execute(stmt).scalar_one_or_none()
        if new_version is None:
            raise TenantVersionConflict(tenant.id, expected_version)
        tenant.version = new_version

    def list(self, filters: TenantFilter, page: PageRequest) -> tuple[list[Tenant], int]:
        stmt: Select[Any] = select(TenantModel)
        if filters.status is not None:
            stmt = stmt.where(TenantModel.status == filters.status.value)
        if filters.slug is not None:
            stmt = stmt.where(TenantModel.slug == filters.slug)
        total = self._session.execute(
            select(func.count()).select_from(stmt.subquery())
        ).scalar_one()
        rows = self._session.scalars(
            stmt.order_by(TenantModel.created_at.desc(), TenantModel.id.desc())
            .limit(page.limit)
            .offset(page.offset)
        ).all()
        return [tenant_to_domain(r) for r in rows], total
