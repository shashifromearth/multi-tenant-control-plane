"""Liveness and readiness."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Response, status
from fastapi.concurrency import run_in_threadpool

from control_plane.container import Container
from control_plane.infrastructure.health import check_broker, check_database
from control_plane.presentation.api.dependencies import get_container

router = APIRouter(tags=["health"])


@router.get("/health/live", summary="Liveness: the process is up and serving")
def live() -> dict[str, str]:
    return {"status": "ok"}


@router.get(
    "/health",
    summary="Readiness: database, broker and outbox backlog",
    responses={503: {"description": "One or more dependencies are unavailable"}},
)
async def ready(
    response: Response, container: Annotated[Container, Depends(get_container)]
) -> dict[str, Any]:
    database = await run_in_threadpool(check_database, container.session_factory)
    broker = await run_in_threadpool(check_broker, container.settings.amqp_url)
    ok = database.ok and broker.ok
    if not ok:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {
        "status": "ok" if ok else "unavailable",
        "checks": {
            "database": {"ok": database.ok, "latency_ms": database.latency_ms, **database.details},
            "broker": {"ok": broker.ok, "latency_ms": broker.latency_ms, **broker.details},
        },
    }
