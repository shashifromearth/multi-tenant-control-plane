"""Correlation-id + access-log middleware (pure ASGI, so it also wraps error responses)."""

from __future__ import annotations

import logging
import re
import time

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from control_plane.correlation import correlation_scope
from control_plane.presentation.api.errors import error_response

CORRELATION_HEADER = "x-correlation-id"
_VALID_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")  # never trust/echo arbitrary header input

logger = logging.getLogger("control_plane.access")


class CorrelationIdMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        incoming = dict(scope["headers"]).get(CORRELATION_HEADER.encode(), b"").decode("latin-1")
        started = time.perf_counter()
        status_code = 500

        with correlation_scope(incoming if _VALID_ID.match(incoming) else None) as cid:
            response_started = False

            async def send_wrapper(message: Message) -> None:
                nonlocal status_code, response_started
                if message["type"] == "http.response.start":
                    response_started = True
                    status_code = message["status"]
                    message.setdefault("headers", [])
                    message["headers"].append((CORRELATION_HEADER.encode(), cid.encode()))
                await send(message)

            try:
                await self.app(scope, receive, send_wrapper)
            except Exception:
                logger.exception("unhandled error")
                if response_started:  # pragma: no cover - nothing sensible left to do
                    raise
                await error_response(500, "internal_error", "Internal server error")(
                    scope, receive, send_wrapper
                )
            finally:
                logger.info(
                    "request",
                    extra={
                        "method": scope["method"],
                        "path": scope["path"],
                        "status": status_code,
                        "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                    },
                )
