"""Typed configuration from environment variables (12-factor). No secrets have defaults
that are meant for anything but a local developer machine."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from contracts.topology import Topology


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", extra="ignore", frozen=True)

    database_url: str = Field(
        default="postgresql+psycopg://controlplane:controlplane@localhost:5432/controlplane"
    )
    amqp_url: str = Field(default="amqp://guest:guest@localhost:5672/%2F")
    broker_prefix: str = "cp"
    log_level: str = "INFO"

    db_pool_size: int = 10
    db_max_overflow: int = 10

    outbox_poll_interval_s: float = 0.5
    outbox_batch_size: int = 50

    consumer_prefetch: int = 20
    consumer_retry_delays_ms: tuple[int, ...] = (1_000, 5_000, 30_000)

    @property
    def topology(self) -> Topology:
        return Topology(prefix=self.broker_prefix, retry_delays_ms=self.consumer_retry_delays_ms)


@lru_cache
def get_settings() -> Settings:
    return Settings()
