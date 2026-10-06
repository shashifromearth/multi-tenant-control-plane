from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

from contracts.messages import ProgressStatus, TaskMessage, TenantRef
from contracts.topology import Topology
from worker.__main__ import parse_args
from worker.simulator import Simulator, WorkerConfig, progress_event_id


def task() -> TaskMessage:
    now = datetime.now(UTC)
    tenant_id = uuid.uuid4()
    return TaskMessage(
        id=uuid.uuid4(),
        type="deploy",
        tenant_id=tenant_id,
        status="accepted",
        created_at=now,
        updated_at=now,
        tenant=TenantRef(id=tenant_id, slug="acme", name="A"),
    )


def config(**kw: float | int) -> WorkerConfig:
    return WorkerConfig(amqp_url="amqp://x", topology=Topology(), seed=42, **kw)  # type: ignore[arg-type]


def statuses(plan) -> list[ProgressStatus]:  # type: ignore[no-untyped-def]
    return [e.status for e in plan.events]


def test_happy_path() -> None:
    plan = Simulator(config(min_delay_ms=10, max_delay_ms=20)).plan(task())
    assert statuses(plan) == [ProgressStatus.IN_PROGRESS, ProgressStatus.DONE]
    assert all(0.01 <= d <= 0.02 for d in plan.delays_s)


def test_fail_rate_one_always_fails() -> None:
    plan = Simulator(config(fail_rate=1.0)).plan(task())
    assert statuses(plan) == [ProgressStatus.IN_PROGRESS, ProgressStatus.FAILED]
    assert plan.events[-1].reason


def test_out_of_order_and_duplicates() -> None:
    plan = Simulator(config(out_of_order_rate=1.0, duplicate_rate=1.0)).plan(task())
    assert statuses(plan) == [ProgressStatus.DONE] * 2 + [ProgressStatus.IN_PROGRESS] * 2
    assert plan.events[0].event_id == plan.events[1].event_id  # true duplicates


def test_event_ids_are_deterministic_per_task_and_status() -> None:
    t = task()
    a, b = Simulator(config()).plan(t), Simulator(config()).plan(t)
    assert [e.event_id for e in a.events] == [e.event_id for e in b.events]
    assert progress_event_id(t.id, ProgressStatus.DONE) != progress_event_id(
        t.id, ProgressStatus.IN_PROGRESS
    )


@pytest.mark.parametrize(
    "kw", [{"fail_rate": 1.5}, {"duplicate_rate": -0.1}, {"min_delay_ms": 5, "max_delay_ms": 1}]
)
def test_invalid_config(kw: dict[str, float]) -> None:
    with pytest.raises(ValueError, match=r"must|require"):
        config(**kw)


def test_cli_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKER_FAIL_RATE", "0.25")
    cfg = parse_args(["--min-delay-ms", "1", "--max-delay-ms", "2", "--broker-prefix", "zz"])
    assert cfg.fail_rate == 0.25
    assert (cfg.min_delay_ms, cfg.max_delay_ms) == (1, 2)
    assert cfg.topology.prefix == "zz"
    assert parse_args(["--fail-rate=1"]).fail_rate == 1.0
