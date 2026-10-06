from __future__ import annotations

from typing import Self
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from control_plane.domain.entities import Tenant
from control_plane.domain.enums import TenantStatus
from control_plane.domain.validation import NAME_MAX_LENGTH, SLUG_PATTERN
from control_plane.presentation.schemas.common import ApiModel, Timestamp
from control_plane.presentation.schemas.task import TaskResponse


class CreateTenantRequest(ApiModel):
    slug: str = Field(pattern=SLUG_PATTERN, min_length=3, max_length=28, examples=["acme-corp"])
    name: str = Field(min_length=1, max_length=NAME_MAX_LENGTH, examples=["ACME Corporation"])


class UpdateTenantRequest(ApiModel):
    version: int = Field(ge=1, description="Version the client last saw (optimistic lock)")
    name: str | None = Field(default=None, min_length=1, max_length=NAME_MAX_LENGTH)

    @model_validator(mode="after")
    def _has_changes(self) -> Self:
        if self.name is None:
            raise ValueError("at least one updatable field (name) must be provided")
        return self


class TenantResponse(BaseModel):
    id: UUID
    slug: str
    name: str
    status: TenantStatus
    version: int
    created_at: Timestamp
    updated_at: Timestamp

    @classmethod
    def from_domain(cls, tenant: Tenant) -> TenantResponse:
        return cls(
            id=tenant.id,
            slug=tenant.slug,
            name=tenant.name,
            status=tenant.status,
            version=tenant.version,
            created_at=tenant.created_at,
            updated_at=tenant.updated_at,
        )


class TenantOperationResponse(BaseModel):
    """Returned by mutating calls: the tenant and the asynchronous task it spawned."""

    tenant: TenantResponse
    task: TaskResponse
