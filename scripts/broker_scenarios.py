"""Negative messaging scenarios against a *running* stack (``make scenarios``).

The make target stops the worker first so this script fully controls what the control
plane receives, then restarts it. Each scenario publishes directly to RabbitMQ and then
verifies the outcome through the public API.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import UTC, datetime

import httpx
import pika

PREFIX = os.environ.get("BROKER_PREFIX", "cp")
EXCHANGE = f"{PREFIX}.task-progress"


def publish(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    body: str | bytes,
    status: str = "raw",
) -> None:
    channel.basic_publish(
        EXCHANGE,
        f"task.progress.{status}",
        body.encode() if isinstance(body, str) else body,
        pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )


def event(task_id: str, status: str, event_id: str | None = None) -> str:
    return json.dumps(
        {
            "event_id": event_id or str(uuid.uuid4()),
            "task_id": task_id,
            "status": status,
            "timestamp": datetime.now(UTC).isoformat(),
        }
    )


def wait_for(client: httpx.Client, path: str, field: str, value: str) -> dict:  # type: ignore[type-arg]
    deadline = time.monotonic() + 15
    while True:
        doc = client.get(path).json()
        if doc.get(field) == value or time.monotonic() > deadline:
            return doc
        time.sleep(0.3)


def check(label: str, cond: bool) -> bool:
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    return cond


def wait_for_api(client: httpx.Client, base_url: str, timeout_s: float = 90.0) -> None:
    """Fail with a clear message (not a traceback) if the stack is not up yet."""
    deadline = time.monotonic() + timeout_s
    last_error = ""
    while time.monotonic() < deadline:
        try:
            if client.get("/health/live").status_code == 200:
                return
        except httpx.TransportError as exc:
            last_error = str(exc)
        time.sleep(1)
    print(
        f"\nAPI not reachable at {base_url} ({last_error}).\n"
        "Start the stack first:  docker compose up -d --build\n"
        "then re-run this command."
    )
    sys.exit(2)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", default="http://localhost:8000")
    p.add_argument(
        "--amqp-url",
        default=os.environ.get(
            "TEST_AMQP_URL",
            os.environ.get("AMQP_URL", "amqp://controlplane:controlplane@localhost:5672/%2F"),
        ),
    )
    args = p.parse_args()
    client = httpx.Client(base_url=args.base_url, timeout=10)
    wait_for_api(client, args.base_url)
    conn = pika.BlockingConnection(pika.URLParameters(args.amqp_url))
    ch = conn.channel()
    ok = True

    created = client.post(
        "/tenants", json={"slug": f"scn-{uuid.uuid4().hex[:8]}", "name": "Scenarios"}
    ).json()
    tenant_id, task_id = created["tenant"]["id"], created["task"]["id"]
    print(f"tenant={tenant_id} task={task_id}")

    print("\n1) out-of-order: 'done' arrives before 'in_progress'")
    publish(ch, event(task_id, "done"), "done")
    t = wait_for(client, f"/tenants/{tenant_id}", "status", "active")
    ok &= check("tenant active after done", t["status"] == "active")
    version = t["version"]
    publish(ch, event(task_id, "in_progress"), "in_progress")
    time.sleep(1.5)
    task = client.get(f"/tasks/{task_id}").json()
    t = client.get(f"/tenants/{tenant_id}").json()
    ok &= check("late in_progress ignored: task stays done", task["status"] == "done")
    ok &= check("tenant version unchanged by stale event", t["version"] == version)

    print("\n2) duplicate delivery of the same event_id")
    patched = client.patch(
        f"/tenants/{tenant_id}", json={"version": version, "name": "Renamed"}
    ).json()
    update_task = patched["task"]["id"]
    dup_id = str(uuid.uuid4())
    for _ in range(3):
        publish(ch, event(update_task, "failed", dup_id), "failed")
    t = wait_for(client, f"/tenants/{tenant_id}", "status", "failed")
    time.sleep(1.0)
    t = client.get(f"/tenants/{tenant_id}").json()
    ok &= check(
        "tenant failed exactly once (version bumped once)",
        t["status"] == "failed" and t["version"] == patched["tenant"]["version"] + 1,
    )

    print("\n3) poison messages: not JSON / unknown task -> DLQ, consumer keeps running")
    publish(ch, b"this is not json")
    publish(ch, event(str(uuid.uuid4()), "done"), "done")
    time.sleep(1.5)
    dlq = ch.queue_declare(f"{PREFIX}.control-plane.task-progress.dlq", passive=True)
    ok &= check(
        f"DLQ holds poison messages (depth={dlq.method.message_count})",
        dlq.method.message_count >= 2,
    )
    deleted = client.delete(f"/tenants/{tenant_id}")
    ok &= check(
        "API + consumer still healthy (DELETE from failed accepted)", deleted.status_code == 202
    )

    conn.close()
    print("\nRESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
