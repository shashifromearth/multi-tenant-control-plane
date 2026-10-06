"""Shared fixtures.

* Unit tests need nothing.
* ``integration`` / ``messaging`` tests use a real PostgreSQL (a throw-away database,
  migrated with Alembic -- so migrations are tested too) and a real RabbitMQ (topology
  isolated under a random prefix). In the Docker test runner both are mandatory; on a
  laptop without them the tests are skipped with a clear reason.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from pathlib import Path

import pika
import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

from control_plane.config import Settings
from control_plane.container import Container, build_container
from control_plane.presentation.api.app import create_app

ROOT = Path(__file__).resolve().parents[1]
ADMIN_URL = os.environ.get(
    "TEST_DATABASE_ADMIN_URL", "postgresql+psycopg://postgres@localhost:5432/postgres"
)
AMQP_URL = os.environ.get("TEST_AMQP_URL", "amqp://guest:guest@localhost:5672/%2F")
REQUIRE_SERVICES = os.environ.get("TEST_REQUIRE_SERVICES", "0") == "1"


def _unavailable(reason: str) -> None:
    if REQUIRE_SERVICES:
        pytest.fail(reason, pytrace=False)
    pytest.skip(reason)


# ---------------------------------------------------------------------------- database
@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    name = f"controlplane_test_{uuid.uuid4().hex[:8]}"
    admin = create_engine(ADMIN_URL, isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            conn.execute(text(f'CREATE DATABASE "{name}"'))
    except OperationalError as exc:
        _unavailable(f"PostgreSQL not reachable at {ADMIN_URL}: {exc.orig}")

    url = make_url(ADMIN_URL).set(database=name).render_as_string(hide_password=False)
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "alembic"))
    cfg.attributes["database_url"] = url
    command.upgrade(cfg, "head")
    try:
        yield url
    finally:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


@pytest.fixture(scope="session")
def broker_prefix() -> str:
    return f"test-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="session")
def settings(database_url: str, broker_prefix: str) -> Settings:
    return Settings(
        database_url=database_url,
        amqp_url=AMQP_URL,
        broker_prefix=broker_prefix,
        db_pool_size=30,
        outbox_poll_interval_s=0.05,
        consumer_retry_delays_ms=(100, 200, 300),
    )


@pytest.fixture(scope="session")
def container(settings: Settings) -> Iterator[Container]:
    c = build_container(settings)
    yield c
    c.close()


@pytest.fixture(autouse=True)
def _clean_tables(request: pytest.FixtureRequest) -> Iterator[None]:
    yield
    if "container" in request.fixturenames:
        c: Container = request.getfixturevalue("container")
        with c.engine.begin() as conn:
            conn.execute(text("TRUNCATE processed_events, outbox_messages, tasks, tenants CASCADE"))


@pytest.fixture
def client(container: Container) -> Iterator[TestClient]:
    with TestClient(create_app(container)) as c:
        yield c


# ---------------------------------------------------------------------------- broker
@pytest.fixture(scope="session")
def amqp_connection(settings: Settings) -> Iterator[pika.BlockingConnection]:
    try:
        conn = pika.BlockingConnection(pika.URLParameters(settings.amqp_url))
    except pika.exceptions.AMQPError as exc:
        _unavailable(f"RabbitMQ not reachable at {settings.amqp_url}: {exc!r}")
    channel = conn.channel()
    settings.topology.declare(channel)
    yield conn
    if conn.is_open:
        channel = conn.channel()
        for queue in settings.topology.all_queues():
            channel.queue_delete(queue)
        channel.exchange_delete(settings.topology.tasks_exchange)
        channel.exchange_delete(settings.topology.progress_exchange)
        conn.close()


@pytest.fixture
def channel(amqp_connection: pika.BlockingConnection, settings: Settings) -> Iterator:  # type: ignore[type-arg]
    ch = amqp_connection.channel()
    for queue in settings.topology.all_queues():
        ch.queue_purge(queue)
    yield ch
    if ch.is_open:
        ch.close()
