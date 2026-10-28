"""query_timeseries tests: validation, bucketing, row cap."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from lares_mcp_bridge import db
from lares_mcp_bridge.config import Settings
from lares_mcp_bridge.tools import timeseries as ts

pytestmark = pytest.mark.asyncio


def _now_window() -> tuple[str, str]:
    now = datetime.now(tz=UTC)
    return (now - timedelta(hours=4)).isoformat(), (now + timedelta(minutes=1)).isoformat()


@pytest.mark.parametrize("bucket", ["5 minutes", "1 hour", "120 minutes"])
async def test_query_reads_the_named_table_at_any_width(db_pool: None, bucket: str) -> None:
    from_ts, to_ts = _now_window()
    result = await ts.query_timeseries(
        table="knx",
        columns=["value"],
        from_ts=from_ts,
        to_ts=to_ts,
        aggregation="avg",
        bucket=bucket,
    )
    assert result["table"] == "knx"
    assert result["bucket"] == bucket
    assert result["row_count"] > 0


async def test_query_day_bucket_on_an_aggregate_is_one_row_per_day(db_pool: None) -> None:
    # The aggregate's own hourly column is also called `bucket`; the day must win.
    result = await ts.query_timeseries(
        table="solaredge_powerflow_1h",
        columns=["pv_production_avg"],
        from_ts="2026-09-02T00:00:00Z",
        to_ts="2026-09-02T23:59:59Z",
        bucket="1 day",
        filters={"inverter_id": 1},
    )
    assert result["rows"] == [
        {"bucket": "2026-09-02T00:00:00+00:00", "pv_production_avg_avg": 1000.0}
    ]


async def test_query_unknown_table_rejected(db_pool: None) -> None:
    from_ts, to_ts = _now_window()
    with pytest.raises(ValueError, match="unknown_table"):
        await ts.query_timeseries(
            table="not_a_table",
            columns=["value"],
            from_ts=from_ts,
            to_ts=to_ts,
        )


async def test_query_unknown_column_rejected(db_pool: None) -> None:
    from_ts, to_ts = _now_window()
    with pytest.raises(ValueError, match=r"unknown_columns: \['does_not_exist'\]; valid: \["):
        await ts.query_timeseries(
            table="knx",
            columns=["does_not_exist"],
            from_ts=from_ts,
            to_ts=to_ts,
        )


async def test_query_invalid_bucket_rejected(db_pool: None) -> None:
    from_ts, to_ts = _now_window()
    with pytest.raises(ValueError, match="invalid_bucket_interval"):
        await ts.query_timeseries(
            table="knx",
            columns=["value"],
            from_ts=from_ts,
            to_ts=to_ts,
            bucket="DROP TABLE knx",
        )


async def test_query_invalid_aggregation_rejected(db_pool: None) -> None:
    from_ts, to_ts = _now_window()
    with pytest.raises(ValueError, match="invalid_aggregation"):
        await ts.query_timeseries(
            table="knx",
            columns=["value"],
            from_ts=from_ts,
            to_ts=to_ts,
            aggregation="median",  # type: ignore[arg-type]
        )


async def test_query_row_limit_enforced(settings: Settings) -> None:
    await db.init_pool(settings.model_copy(update={"query_row_limit": 2}))
    try:
        from_ts, to_ts = _now_window()
        with pytest.raises(ValueError, match="row_limit_exceeded"):
            await ts.query_timeseries(
                table="knx",
                columns=["value"],
                from_ts=from_ts,
                to_ts=to_ts,
                bucket="1 minute",
            )
    finally:
        await db.close_pool()
