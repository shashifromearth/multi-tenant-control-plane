from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError

from contracts.messages import ProgressStatus, TaskProgressEvent
from contracts.topology import Topology


def test_topology_names_and_retry_tiers() -> None:
    t = Topology(prefix="x", retry_delays_ms=(10, 20))
    assert t.tasks_exchange == "x.tasks"
    assert t.progress_queue == "x.control-plane.task-progress"
    assert t.progress_dlq.endswith(".dlq")
    assert t.retry_queue(1).endswith("retry.10ms")
    assert t.retry_queue(2).endswith("retry.20ms")
    assert t.retry_queue(9).endswith("retry.20ms")  # capped at last tier
    assert t.max_retries == 2
    assert t.task_routing_key("deploy") == "task.deploy"
    assert t.progress_routing_key("done") == "task.progress.done"
    assert len(t.all_queues()) == 6


def test_progress_event_requires_tz_aware_timestamp_and_known_status() -> None:
    base = {"event_id": str(uuid.uuid4()), "task_id": str(uuid.uuid4())}
    ok = TaskProgressEvent.model_validate(
        {**base, "status": "done", "timestamp": "2026-01-01T00:00:00Z", "extra": "ignored"}
    )
    assert ok.status is ProgressStatus.DONE
    assert ok.status.is_terminal
    with pytest.raises(ValidationError):
        TaskProgressEvent.model_validate(
            {**base, "status": "done", "timestamp": "2026-01-01T00:00:00"}
        )
    with pytest.raises(ValidationError):
        TaskProgressEvent.model_validate(
            {**base, "status": "accepted", "timestamp": "2026-01-01T00:00:00Z"}
        )
