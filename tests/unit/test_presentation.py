from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from control_plane.presentation.api.errors import PreconditionInvalid
from control_plane.presentation.api.tenants import _parse_if_match
from control_plane.presentation.schemas.common import to_iso_ms
from control_plane.presentation.schemas.tenant import CreateTenantRequest, UpdateTenantRequest


def test_iso_ms_with_z_suffix() -> None:
    assert (
        to_iso_ms(datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=UTC)) == "2026-01-02T03:04:05.678Z"
    )
    ist = timezone(timedelta(hours=5, minutes=30))
    assert to_iso_ms(datetime(2026, 1, 2, 8, 34, 5, 0, tzinfo=ist)) == "2026-01-02T03:04:05.000Z"


def test_request_schemas() -> None:
    with pytest.raises(ValidationError):
        CreateTenantRequest(slug="Bad_Slug", name="x")
    with pytest.raises(ValidationError):
        CreateTenantRequest.model_validate({"slug": "acme", "name": "x", "status": "active"})
    with pytest.raises(ValidationError):
        UpdateTenantRequest(version=1)  # no changes
    with pytest.raises(ValidationError):
        UpdateTenantRequest(version=0, name="x")


@pytest.mark.parametrize(("header", "expected"), [(None, None), ('"3"', 3), ("4", 4), ('W/"5"', 5)])
def test_if_match_parsing(header: str | None, expected: int | None) -> None:
    assert _parse_if_match(header) == expected


def test_if_match_rejects_garbage() -> None:
    with pytest.raises(PreconditionInvalid):
        _parse_if_match('"abc"')
