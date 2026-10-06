"""FastAPI application factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from control_plane.config import get_settings
from control_plane.container import Container, build_container
from control_plane.presentation.api import health, tasks, tenants
from control_plane.presentation.api.errors import register_error_handlers
from control_plane.presentation.api.middleware import CorrelationIdMiddleware

DESCRIPTION = """
Control plane for asynchronous tenant provisioning.

* Mutating tenant calls return the tenant **and** the task they spawned; the task is
  published to the broker only after the database transaction commits (transactional outbox).
* Every error has the shape `{"error": {"code", "message", "details", "correlation_id"}}`.
* Send `X-Correlation-ID` to trace a request across API, broker, worker and consumer.
"""


def create_app(container: Container | None = None) -> FastAPI:
    owns_container = container is None

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.container = container or build_container(get_settings())
        try:
            yield
        finally:
            if owns_container:
                app.state.container.close()

    app = FastAPI(
        title="Tenant Provisioning Control Plane",
        version="1.0.0",
        description=DESCRIPTION,
        lifespan=lifespan,
    )
    app.add_middleware(CorrelationIdMiddleware)
    register_error_handlers(app)
    app.include_router(tenants.router)
    app.include_router(tasks.router)
    app.include_router(health.router)
    return app
