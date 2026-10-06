"""``python -m worker --fail-rate 0.2 --min-delay-ms 200 --max-delay-ms 1500``

Every flag also reads an environment variable (shown in ``--help``), so behaviour can be
changed by restarting the container with different env vars.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
from datetime import UTC, datetime

from contracts.topology import Topology
from worker.simulator import Worker, WorkerConfig

_RESERVED = frozenset(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime"}


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname.lower(),
            "service": "worker",
            "msg": record.getMessage(),
            **{k: v for k, v in vars(record).items() if k not in _RESERVED},
        }
        return json.dumps(payload, default=str)


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def parse_args(argv: list[str] | None = None) -> WorkerConfig:
    p = argparse.ArgumentParser(prog="worker", description="Tenant provisioning simulator")
    p.add_argument(
        "--amqp-url",
        default=_env("AMQP_URL", "amqp://guest:guest@localhost:5672/%2F"),
        help="env AMQP_URL",
    )
    p.add_argument("--broker-prefix", default=_env("BROKER_PREFIX", "cp"), help="env BROKER_PREFIX")
    p.add_argument(
        "--fail-rate",
        type=float,
        default=float(_env("WORKER_FAIL_RATE", "0")),
        help="probability [0-1] that a task fails (env WORKER_FAIL_RATE)",
    )
    p.add_argument(
        "--min-delay-ms",
        type=int,
        default=int(_env("WORKER_MIN_DELAY_MS", "500")),
        help="env WORKER_MIN_DELAY_MS",
    )
    p.add_argument(
        "--max-delay-ms",
        type=int,
        default=int(_env("WORKER_MAX_DELAY_MS", "2000")),
        help="env WORKER_MAX_DELAY_MS",
    )
    p.add_argument(
        "--duplicate-rate",
        type=float,
        default=float(_env("WORKER_DUPLICATE_RATE", "0")),
        help="probability that each progress event is published twice (env WORKER_DUPLICATE_RATE)",
    )
    p.add_argument(
        "--out-of-order-rate",
        type=float,
        default=float(_env("WORKER_OUT_OF_ORDER_RATE", "0")),
        help="probability that the terminal event is published before "
        "in_progress (env WORKER_OUT_OF_ORDER_RATE)",
    )
    p.add_argument("--prefetch", type=int, default=int(_env("WORKER_PREFETCH", "4")))
    p.add_argument("--seed", type=int, default=None, help="RNG seed for reproducible runs")
    a = p.parse_args(argv)
    return WorkerConfig(
        amqp_url=a.amqp_url,
        topology=Topology(prefix=a.broker_prefix),
        fail_rate=a.fail_rate,
        min_delay_ms=a.min_delay_ms,
        max_delay_ms=a.max_delay_ms,
        duplicate_rate=a.duplicate_rate,
        out_of_order_rate=a.out_of_order_rate,
        prefetch=a.prefetch,
        seed=a.seed,
    )


def main() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    logging.basicConfig(level=_env("LOG_LEVEL", "INFO").upper(), handlers=[handler])
    logging.getLogger("pika").setLevel(logging.WARNING)

    config = parse_args()
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    Worker(config).run_forever(stop)


if __name__ == "__main__":
    main()
