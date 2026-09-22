"""Interval: the bucket-width literal the time-series tools accept."""

from __future__ import annotations

import pytest

from lares_mcp_bridge.interval import Interval


@pytest.mark.parametrize(
    ("text", "count", "unit"),
    [
        ("1 hour", 1, "hour"),
        ("15 minutes", 15, "minute"),
        ("  3 Hours ", 3, "hour"),
        ("1 hours", 1, "hour"),
        ("2 week", 2, "week"),
    ],
)
def test_parse_accepts_postgres_literals(text: str, count: int, unit: str) -> None:
    parsed = Interval.parse(text)
    assert parsed.count == count
    assert parsed.unit == unit


@pytest.mark.parametrize("text", ["not-a-bucket", "DROP TABLE knx", "1", "hour", "1.5 hours", ""])
def test_parse_rejects_anything_else(text: str) -> None:
    with pytest.raises(ValueError, match="invalid_bucket_interval"):
        Interval.parse(text)


def test_str_is_a_normalised_literal_for_sql() -> None:
    assert str(Interval.parse("1 hour")) == "1 hour"
    assert str(Interval.parse("1 hours")) == "1 hour"
    assert str(Interval.parse("15 MINUTES")) == "15 minutes"
    assert str(Interval.parse("3 hours")) == "3 hours"


def test_seconds_and_ratio() -> None:
    assert Interval.parse("15 minutes").seconds == 900
    assert Interval.parse("1 day").seconds == 86400
    assert Interval.parse("3 hours") // Interval.parse("1 hour") == 3
    assert Interval.parse("1 hour") // Interval.parse("1 day") == 0


@pytest.mark.parametrize(
    ("text", "hourly"),
    [
        ("1 hour", True),
        ("2 hours", True),
        ("1 day", True),
        ("1 month", True),
        ("120 minutes", True),
        ("59 minutes", False),
        ("3600 seconds", True),
        ("5 minutes", False),
    ],
)
def test_at_least_hourly_is_about_width_not_unit(text: str, hourly: bool) -> None:
    assert Interval.parse(text).at_least_hourly is hourly
