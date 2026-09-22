"""Data-source tests against the seeded TimescaleDB container: listing, describing, routing."""

from __future__ import annotations

import pytest

from lares_mcp_bridge.interval import Interval
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


async def test_resolve_routes_hourly_and_coarser_to_the_cagg(db_pool: None) -> None:
    raw = await sources.resolve("knx", Interval.parse("5 minutes"))
    assert (raw.name, raw.kind, raw.time_column) == ("knx", sources.KIND_HYPERTABLE, "time")

    routed = await sources.resolve("knx", Interval.parse("120 minutes"))
    assert (routed.name, routed.kind) == ("knx_1h", sources.KIND_CONTINUOUS_AGGREGATE)
    assert routed.time_column == "bucket"

    # Naming the aggregate directly is honoured at any width.
    direct = await sources.resolve("knx_1h", Interval.parse("1 minute"))
    assert direct.name == "knx_1h"


async def test_resolve_unknown_table_raises(db_pool: None) -> None:
    with pytest.raises(ValueError, match="unknown_table"):
        await sources.resolve("does_not_exist", Interval.parse("1 hour"))
