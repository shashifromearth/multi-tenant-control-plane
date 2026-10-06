"""Tenant resource. Controllers are thin: parse -> call a use case -> render."""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Header, Query, Response, status

from control_plane.application.commands import CreateTenant, DeleteTenant, UpdateTenant
from control_plane.application.dto import TenantFilter, TenantOperationResult
from control_plane.domain.enums import TenantStatus
from control_plane.presentation.api.dependencies import PageDep, TenantServiceDep
from control_plane.presentation.api.errors import PreconditionInvalid
from control_plane.presentation.schemas.common import ErrorResponse, PageResponse
from control_plane.presentation.schemas.task import TaskResponse
from control_plane.presentation.schemas.tenant import (
    CreateTenantRequest,
    TenantOperationResponse,
    TenantResponse,
    UpdateTenantRequest,
)

router = APIRouter(prefix="/tenants", tags=["tenants"])

_Responses = dict[int | str, dict[str, Any]]
_E404: _Responses = {404: {"model": ErrorResponse, "description": "tenant_not_found"}}
_E409: _Responses = {
    409: {
        "model": ErrorResponse,
        "description": "tenant_update_not_allowed | tenant_version_conflict",
    }
}
_E422: _Responses = {422: {"model": ErrorResponse, "description": "validation_error"}}


def _operation_response(
    response: Response, result: TenantOperationResult
) -> TenantOperationResponse:
    response.headers["ETag"] = _etag(result.tenant.version)
    return TenantOperationResponse(
        tenant=TenantResponse.from_domain(result.tenant),
        task=TaskResponse.from_domain(result.task),
    )


def _etag(version: int) -> str:
    return f'"{version}"'


def _parse_if_match(value: str | None) -> int | None:
    if value is None:
        return None
    raw = value.strip().removeprefix("W/").strip('"')
    if not raw.isdigit():
        raise PreconditionInvalid("If-Match must carry the tenant version, e.g. '\"3\"'")
    return int(raw)


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    summary="Create a tenant (starts asynchronous provisioning)",
    responses={409: {"model": ErrorResponse, "description": "tenant_already_exists"}, **_E422},
)
def create_tenant(
    body: CreateTenantRequest, service: TenantServiceDep, response: Response
) -> TenantOperationResponse:
    result = service.create(CreateTenant(slug=body.slug, name=body.name))
    response.headers["Location"] = f"/tenants/{result.tenant.id}"
    return _operation_response(response, result)


@router.get("", summary="List tenants")
def list_tenants(
    service: TenantServiceDep,
    page: PageDep,
    status_: Annotated[TenantStatus | None, Query(alias="status")] = None,
    slug: Annotated[str | None, Query(max_length=28)] = None,
) -> PageResponse[TenantResponse]:
    result = service.list(TenantFilter(status=status_, slug=slug), page)
    return PageResponse[TenantResponse](
        items=[TenantResponse.from_domain(t) for t in result.items],
        total=result.total,
        limit=result.limit,
        offset=result.offset,
    )


@router.get("/{tenant_id}", summary="Get a tenant", responses=_E404)
def get_tenant(tenant_id: UUID, service: TenantServiceDep, response: Response) -> TenantResponse:
    tenant = service.get(tenant_id)
    response.headers["ETag"] = _etag(tenant.version)
    return TenantResponse.from_domain(tenant)


@router.patch(
    "/{tenant_id}",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Partially update a tenant (only from 'active'; optimistic locking via version)",
    responses={**_E404, **_E409, **_E422},
)
def update_tenant(
    tenant_id: UUID, body: UpdateTenantRequest, service: TenantServiceDep, response: Response
) -> TenantOperationResponse:
    result = service.update(
        UpdateTenant(tenant_id=tenant_id, expected_version=body.version, name=body.name)
    )
    response.headers["Location"] = f"/tasks/{result.task.id}"
    return _operation_response(response, result)


@router.delete(
    "/{tenant_id}",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Destroy a tenant (only from 'active' or 'failed'; asynchronous)",
    description=(
        'Optionally send `If-Match: "<version>"` to make the delete conditional. '
        "The tenant record is kept with status `destroyed` (its slug stays reserved)."
    ),
    responses={**_E404, **_E409, 400: {"model": ErrorResponse}},
)
def delete_tenant(
    tenant_id: UUID,
    service: TenantServiceDep,
    response: Response,
    if_match: Annotated[str | None, Header()] = None,
) -> TenantOperationResponse:
    result = service.delete(
        DeleteTenant(tenant_id=tenant_id, expected_version=_parse_if_match(if_match))
    )
    response.headers["Location"] = f"/tasks/{result.task.id}"
    return _operation_response(response, result)
