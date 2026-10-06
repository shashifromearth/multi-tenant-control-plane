from __future__ import annotations

import json
import logging

import pytest

from control_plane.correlation import correlation_scope, get_correlation_id
from control_plane.infrastructure.logging import JsonFormatter, configure_logging


def test_json_log_line_carries_correlation_id_and_extras() -> None:
    record = logging.makeLogRecord(
        {"name": "x", "levelname": "INFO", "msg": "hello %s", "args": ("world",), "tenant_id": "t1"}
    )
    with correlation_scope("cid-9"):
        line = json.loads(JsonFormatter("svc").format(record))
    assert line["msg"] == "hello world"
    assert line["service"] == "svc"
    assert line["correlation_id"] == "cid-9"
    assert line["tenant_id"] == "t1"
    assert line["ts"].endswith("Z")


def test_exception_is_serialised() -> None:
    try:
        raise ValueError("bad")
    except ValueError:
        import sys

        record = logging.makeLogRecord({"msg": "oops", "exc_info": sys.exc_info()})
    assert "ValueError: bad" in json.loads(JsonFormatter("svc").format(record))["exc"]


def test_correlation_scope_generates_and_resets() -> None:
    assert get_correlation_id() is None
    with correlation_scope(None) as cid:
        assert get_correlation_id() == cid
        assert len(cid) == 32
    assert get_correlation_id() is None


def test_configure_logging(capsys: pytest.CaptureFixture[str]) -> None:
    root = logging.getLogger()
    saved = root.handlers[:], root.level
    try:
        configure_logging("unit", "debug")
        logging.getLogger("t").info("ping")
        assert json.loads(capsys.readouterr().out.strip())["msg"] == "ping"
    finally:
        root.handlers[:], _ = saved
        root.setLevel(saved[1])
