"""Process entrypoint: ``python -m control_plane {api|relay|consumer}``.

All three roles ship in one image and share one codebase; they run as separate
processes so they can be scaled, restarted and health-checked independently.
"""

from __future__ import annotations

import argparse
import logging
import signal
import threading

from control_plane.config import get_settings
from control_plane.container import build_container
from control_plane.infrastructure.logging import configure_logging
from control_plane.infrastructure.messaging.connection import Heartbeat

logger = logging.getLogger("control_plane")

# Inside a container the API must listen on all interfaces; compose publishes it on
# 127.0.0.1 only. The heartbeat file is a liveness marker, never read for data.
BIND_ALL = "0.0.0.0"  # noqa: S104  # nosec B104
HEARTBEAT_FILE = "/tmp/heartbeat"  # noqa: S108  # nosec B108


def _stop_event() -> threading.Event:
    stop = threading.Event()

    def _handle(signum: int, _frame: object) -> None:
        logger.info("shutdown signal received", extra={"signal": signum})
        stop.set()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)
    return stop


def main() -> None:
    parser = argparse.ArgumentParser(prog="control_plane")
    parser.add_argument("role", choices=["api", "relay", "consumer"])
    parser.add_argument("--host", default=BIND_ALL)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--heartbeat-file", default=HEARTBEAT_FILE)
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(f"control-plane-{args.role}", settings.log_level)

    if args.role == "api":
        import uvicorn

        uvicorn.run(
            "control_plane.presentation.api.app:create_app",
            factory=True,
            host=args.host,
            port=args.port,
            log_config=None,  # keep our JSON logging
            proxy_headers=True,
        )
        return

    container = build_container(settings)
    heartbeat = Heartbeat(args.heartbeat_file)
    stop = _stop_event()
    try:
        if args.role == "relay":
            from control_plane.infrastructure.outbox.relay import OutboxRelay

            OutboxRelay(
                container.session_factory,
                settings.amqp_url,
                settings.topology,
                batch_size=settings.outbox_batch_size,
                poll_interval_s=settings.outbox_poll_interval_s,
                heartbeat=heartbeat,
            ).run_forever(stop)
        else:
            from control_plane.infrastructure.consumers.progress_consumer import (
                ProgressConsumer,
                ProgressMessageHandler,
            )

            ProgressConsumer(
                ProgressMessageHandler(container.task_progress_service),
                settings.amqp_url,
                settings.topology,
                prefetch=settings.consumer_prefetch,
                heartbeat=heartbeat,
            ).run_forever(stop)
    finally:
        container.close()


if __name__ == "__main__":
    main()
