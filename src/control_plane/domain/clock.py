"""Time helpers. All timestamps are UTC and truncated to millisecond precision, which is
the precision exposed by the API, so stored and rendered values never disagree."""

from datetime import UTC, datetime


def utc_now() -> datetime:
    now = datetime.now(UTC)
    return now.replace(microsecond=(now.microsecond // 1000) * 1000)
