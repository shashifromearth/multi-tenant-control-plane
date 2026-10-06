"""Centralised exception -> HTTP mapping. Controllers never build error bodies.

| code                       | HTTP |
|----------------------------|------|
| validation_error           | 422  |
| tenant_not_found           | 404  |
| task_not_found             | 404  |
| tenant_already_exists      | 409  |
| tenant_update_not_allowed  | 409  |
| tenant_version_conflict    | 409  |
| precondition_invalid       | 400  |
| not_found / method_not_allowed | 404 / 405 |
| internal_error             | 500  |
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from control_plane.correlation import get_correlation_id
from control_plane.domain.exceptions import (
    DomainError,
    TaskNotFound,
    TenantAlreadyExists,
    TenantNotFound,
    TenantUpdateNotAllowed,
    TenantVersionConflict,
    ValidationFailed,
)

logger = logging.getLogger(__name__)


class PreconditionInvalid(DomainError):
    """Malformed conditional-request header (e.g. a non-integer If-Match)."""

    code = "precondition_invalid"


HTTP_STATUS_BY_ERROR: dict[type[DomainError], int] = {
    PreconditionInvalid: status.HTTP_400_BAD_REQUEST,
    ValidationFailed: status.HTTP_422_UNPROCESSABLE_CONTENT,
    TenantNotFound: status.HTTP_404_NOT_FOUND,
    TaskNotFound: status.HTTP_404_NOT_FOUND,
    TenantAlreadyExists: status.HTTP_409_CONFLICT,
    TenantUpdateNotAllowed: status.HTTP_409_CONFLICT,
    TenantVersionConflict: status.HTTP_409_CONFLICT,
}


def error_response(
    http_status: int, code: str, message: str, details: dict[str, Any] | None = None
) -> JSONResponse:
    return JSONResponse(
        status_code=http_status,
        content={
            "error": {
                "code": code,
                "message": message,
                "details": jsonable_encoder(details or {}),
                "correlation_id": get_correlation_id(),
            }
        },
    )


async def _domain_error(_: Request, exc: DomainError) -> JSONResponse:
    http_status = HTTP_STATUS_BY_ERROR.get(type(exc))
    if http_status is None:
        # e.g. InvalidStateTransition reaching the API would be a bug, not a client error.
        logger.error("unmapped domain error", extra={"code": exc.code, "error": exc.message})
        return error_response(500, "internal_error", "Internal server error")
    return error_response(http_status, exc.code, exc.message, exc.details)


async def _validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    errors = [
        {"loc": list(e.get("loc", ())), "msg": e.get("msg"), "type": e.get("type")}
        for e in exc.errors()
    ]
    return error_response(
        status.HTTP_422_UNPROCESSABLE_CONTENT,
        "validation_error",
        "Request validation failed",
        {"errors": errors},
    )


async def _http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
    codes = {404: "not_found", 405: "method_not_allowed"}
    return error_response(
        exc.status_code, codes.get(exc.status_code, "http_error"), str(exc.detail)
    )


def register_error_handlers(app: FastAPI) -> None:
    app.exception_handler(DomainError)(_domain_error)
    app.exception_handler(RequestValidationError)(_validation_error)
    app.exception_handler(StarletteHTTPException)(_http_error)
    # Truly unexpected exceptions are turned into 500 ``internal_error`` bodies by
    # CorrelationIdMiddleware, so the correlation id is still attached.
