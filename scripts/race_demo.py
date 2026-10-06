"""Concurrency demo against a *running* stack (``make race-demo``).

1. N clients POST the same slug at the same instant      -> exactly 1 x 201, N-1 x 409
2. N clients PATCH the same tenant with the same version -> exactly 1 x 202, N-1 x 409
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import httpx


def _fire(n: int, fn) -> list[httpx.Response]:  # type: ignore[no-untyped-def]
    barrier = threading.Barrier(n)

    def call(i: int) -> httpx.Response:
        barrier.wait()  # release all requests at the same moment
        return fn(i)

    with ThreadPoolExecutor(max_workers=n) as pool:
        return list(pool.map(call, range(n)))


def _summary(responses: list[httpx.Response]) -> Counter[str]:
    return Counter(
        f"{r.status_code} {r.json().get('error', {}).get('code', 'ok')}" for r in responses
    )


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
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("-n", type=int, default=20)
    args = parser.parse_args()
    client = httpx.Client(base_url=args.base_url, timeout=30)
    wait_for_api(client, args.base_url)
    ok = True

    slug = f"race-{uuid.uuid4().hex[:8]}"
    print(f"\n[1] {args.n} concurrent POST /tenants slug={slug}")
    responses = _fire(
        args.n, lambda i: client.post("/tenants", json={"slug": slug, "name": f"client {i}"})
    )
    summary = _summary(responses)
    print("   ", dict(summary))
    ok &= summary.get("201 ok") == 1 and summary.get("409 tenant_already_exists") == args.n - 1
    tenant = next(r.json()["tenant"] for r in responses if r.status_code == 201)

    print("    waiting for the worker to activate the tenant ...")
    deadline = time.monotonic() + 60
    while (t := client.get(f"/tenants/{tenant['id']}").json())["status"] != "active":
        if time.monotonic() > deadline:
            print("    tenant never became active -- is the worker running?")
            return 1
        time.sleep(0.5)

    print(f"\n[2] {args.n} concurrent PATCH /tenants/{t['id']} with version={t['version']}")
    responses = _fire(
        args.n,
        lambda i: client.patch(
            f"/tenants/{t['id']}", json={"version": t["version"], "name": f"renamed by {i}"}
        ),
    )
    summary = _summary(responses)
    print("   ", dict(summary))
    ok &= summary.get("202 ok") == 1 and summary.get("409 tenant_version_conflict") == args.n - 1

    print("\nRESULT:", "PASS - exactly one winner each time" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
