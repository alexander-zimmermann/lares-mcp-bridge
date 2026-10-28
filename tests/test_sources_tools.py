"""Data-source tests against the seeded TimescaleDB container: listing, describing, resolving."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from lares_mcp_bridge import db
from lares_mcp_bridge.tools import sources

pytestmark = pytest.mark.asyncio


async def test_list_data_sources_returns_hypertables_and_caggs(db_pool: None) -> None:
    listed = await sources.list_data_sources()
    names = {s["name"]: s for s in listed}

    assert "knx" in names
    assert names["knx"]["kind"] == sources.KIND_HYPERTABLE
    assert names["knx"]["time_column"] == "time"
    assert names["knx"]["time_range"]["min"] is not None

    assert "ems_esp" in names
    assert names["ems_esp"]["kind"] == sources.KIND_HYPERTABLE

    assert "knx_1h" in names
    assert names["knx_1h"]["kind"] == sources.KIND_CONTINUOUS_AGGREGATE
    assert names["knx_1h"]["time_column"] == "bucket"


async def test_a_range_starts_with_the_oldest_chunk_and_ends_with_the_newest_value(
    db_pool: None,
) -> None:
    """Every query routes through this catalog, so it reads TimescaleDB's chunk
    metadata instead of scanning a year of compressed data for its minimum: the
    start is exact to the chunk, the end is the newest value itself."""
    (truth,) = await db.lookup(
        "test", "knx", "SELECT min(time) AS oldest, max(time) AS newest FROM knx"
    )
    chunk = timedelta(days=7)  # the seed's hypertable keeps the default chunk interval

    knx = {s["name"]: s for s in await sources.list_data_sources()}["knx"]["time_range"]

    oldest = datetime.fromisoformat(truth["oldest"])
    assert oldest - chunk < datetime.fromisoformat(knx["min"]) <= oldest
    assert knx["max"] == truth["newest"]


async def test_get_schema_returns_columns_and_jsonb_keys(db_pool: None) -> None:
    schema = await sources.get_schema("ems_esp")

    assert schema["name"] == "ems_esp"
    assert schema["time_column"] == "time"
    col_names = {c["name"] for c in schema["columns"]}
    assert {"time", "topic", "raw"} <= col_names

    raw_keys = {entry["key"] for entry in schema["jsonb_top_keys"]["raw"]}
    assert {"flow_temp", "return_temp", "burner_power"} <= raw_keys


async def test_get_schema_unknown_table_raises(db_pool: None) -> None:
    with pytest.raises(ValueError, match="unknown_table"):
        await sources.get_schema("does_not_exist")


async def test_get_schema_knx_hint_present(db_pool: None) -> None:
    schema = await sources.get_schema("knx")
    assert schema["hint"] is not None
    assert "ga_catalog_view" in schema["hint"]


async def test_resolve_describes_the_named_source(db_pool: None) -> None:
    raw = await sources.resolve("knx")
    assert (raw.name, raw.kind, raw.time_column) == ("knx", sources.KIND_HYPERTABLE, "time")

    cagg = await sources.resolve("knx_1h")
    assert (cagg.name, cagg.kind) == ("knx_1h", sources.KIND_CONTINUOUS_AGGREGATE)
    assert cagg.time_column == "bucket"


async def test_resolve_unknown_table_raises(db_pool: None) -> None:
    with pytest.raises(ValueError, match="unknown_table"):
        await sources.resolve("does_not_exist")
