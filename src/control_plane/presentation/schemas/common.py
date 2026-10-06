from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer


def to_iso_ms(value: datetime) -> str:
    """ISO 8601, millisecond precision, ``Z`` suffix -- e.g. 2026-01-31T12:00:00.123Z"""
    return (
        value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"
    )


Timestamp = Annotated[
    datetime,
    PlainSerializer(to_iso_ms, return_type=str, when_used="json"),
    Field(json_schema_extra={"example": "2026-01-31T12:00:00.123Z"}),
]


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PageResponse[T](BaseModel):
    items: list[T]
    total: int = Field(description="Total number of items matching the filters")
    limit: int
    offset: int


class ErrorBody(BaseModel):
    code: str = Field(examples=["tenant_version_conflict"])
    message: str = Field(examples=["Version conflict detected"])
    details: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str | None = None


class ErrorResponse(BaseModel):
    error: ErrorBody
