"""Tests for the forecast read tools (get_forecast, get_pv_forecast,
get_weather_forecast)."""

from __future__ import annotations

from collections.abc import AsyncIterator

import psycopg
import pytest_asyncio
from psycopg.rows import DictRow, dict_row

from lares_mcp_bridge.config import Settings
from lares_mcp_bridge.tools import forecasts


@pytest_asyncio.fixture
async def seeded_pv_forecast(settings: Settings, db_pool: None) -> AsyncIterator[None]:
    """Seed 6 hourly forecast.solar PV rows — the (pv_production / forecast_solar)
    triple get_pv_forecast pins on, deliberately distinct from the seasonal
    job's (pv_production_avg / mstl) rows."""
    conn = psycopg.Connection[DictRow].connect(
        settings.db_dsn, autocommit=True, row_factory=dict_row
    )
    try:
        conn.execute("TRUNCATE TABLE mcp_forecasts")
        conn.execute(
            """
            INSERT INTO mcp_forecasts (forecast_for, source, metric, model,
                                        forecast_value, forecast_lower, forecast_upper)
            SELECT
                NOW() + ((i + 1) || ' hours')::interval,
                'forecast_solar',
                'pv_production',
                'forecast_solar',
                500.0 + i * 250,
                NULL,
                NULL
            FROM generate_series(0, 5) AS i
            """
        )
    finally:
        conn.close()
    yield
    conn = psycopg.connect(settings.db_dsn, autocommit=True)
    try:
        conn.execute("TRUNCATE TABLE mcp_forecasts")
    finally:
        conn.close()


async def test_get_pv_forecast_returns_seeded_rows(seeded_pv_forecast: None) -> None:
    result = await forecasts.get_pv_forecast(hours=24)
    assert result["metric"] == "pv_production"
    assert result["row_count"] == 6
    assert "note" not in result
    assert all(r["model"] == "forecast_solar" for r in result["rows"])


async def test_get_pv_forecast_respects_horizon(seeded_pv_forecast: None) -> None:
    # Only the first 3 hourly rows fall inside a 3h horizon.
    result = await forecasts.get_pv_forecast(hours=3)
    assert result["row_count"] == 3


async def test_get_pv_forecast_empty_sets_note(settings: Settings, db_pool: None) -> None:
    conn = psycopg.connect(settings.db_dsn, autocommit=True)
    try:
        conn.execute("TRUNCATE TABLE mcp_forecasts")
    finally:
        conn.close()
    result = await forecasts.get_pv_forecast(hours=48)
    assert result["row_count"] == 0
    assert "check forecast-solar CronJob health" in result["note"]


@pytest_asyncio.fixture
async def seeded_weather_forecast(settings: Settings, db_pool: None) -> AsyncIterator[None]:
    """Seed 2 forecast hours x 3 open_meteo metrics (6 rows) to exercise the
    per-hour pivot."""
    conn = psycopg.Connection[DictRow].connect(
        settings.db_dsn, autocommit=True, row_factory=dict_row
    )
    try:
        conn.execute("TRUNCATE TABLE mcp_forecasts")
        conn.execute(
            """
            INSERT INTO mcp_forecasts (forecast_for, source, metric, model,
                                        forecast_value, forecast_lower, forecast_upper)
            SELECT
                NOW() + ((h + 1) || ' hours')::interval,
                'open_meteo',
                m.metric,
                'open_meteo',
                m.value + h,
                NULL,
                NULL
            FROM generate_series(0, 1) AS h
            CROSS JOIN (VALUES ('temperature', 15.0), ('cloud_cover', 40.0),
                               ('wind_speed', 3.0)) AS m(metric, value)
            """
        )
    finally:
        conn.close()
    yield
    conn = psycopg.connect(settings.db_dsn, autocommit=True)
    try:
        conn.execute("TRUNCATE TABLE mcp_forecasts")
    finally:
        conn.close()


async def test_get_weather_forecast_pivots_per_hour(seeded_weather_forecast: None) -> None:
    result = await forecasts.get_weather_forecast(hours=24)
    assert result["model"] == "open_meteo"
    # 2 distinct forecast hours, each a dict carrying all 3 metrics.
    assert result["row_count"] == 2
    assert "note" not in result
    first = result["hours"][0]
    assert {"forecast_for", "temperature", "cloud_cover", "wind_speed"} <= set(first)
    assert first["temperature"] == 15.0


async def test_get_weather_forecast_respects_horizon(seeded_weather_forecast: None) -> None:
    # Only the first forecast hour falls inside a 1h horizon.
    result = await forecasts.get_weather_forecast(hours=1)
    assert result["row_count"] == 1


async def test_get_weather_forecast_empty_sets_note(settings: Settings, db_pool: None) -> None:
    conn = psycopg.connect(settings.db_dsn, autocommit=True)
    try:
        conn.execute("TRUNCATE TABLE mcp_forecasts")
    finally:
        conn.close()
    result = await forecasts.get_weather_forecast(hours=48)
    assert result["row_count"] == 0
    assert "check forecast-weather CronJob health" in result["note"]


# =====================================================================
# get_forecast — the generic read the two above specialise
# =====================================================================


@pytest_asyncio.fixture
async def seeded_seasonal_forecast(settings: Settings, db_pool: None) -> AsyncIterator[None]:
    """Seed 4 hourly rows under the seasonal job's (pv_production_avg / mstl)
    triple, which get_forecast reads by metric."""
    conn = psycopg.connect(settings.db_dsn, autocommit=True)
    try:
        conn.execute("TRUNCATE TABLE mcp_forecasts")
        conn.execute(
            """
            INSERT INTO mcp_forecasts (forecast_for, source, metric, model,
                                        forecast_value, forecast_lower, forecast_upper)
            SELECT
                NOW() + ((i + 1) || ' hours')::interval,
                'solaredge_powerflow_1h',
                'pv_production_avg',
                'mstl',
                4000.0 + i * 100,
                3500.0 + i * 100,
                4500.0 + i * 100
            FROM generate_series(0, 3) AS i
            """
        )
    finally:
        conn.close()
    yield
    conn = psycopg.connect(settings.db_dsn, autocommit=True)
    try:
        conn.execute("TRUNCATE TABLE mcp_forecasts")
    finally:
        conn.close()


async def test_get_forecast_returns_seeded_rows(seeded_seasonal_forecast: None) -> None:
    result = await forecasts.get_forecast(metric="pv_production_avg")
    assert result["metric"] == "pv_production_avg"
    assert result["row_count"] == 4


async def test_get_forecast_unknown_metric_empty(seeded_seasonal_forecast: None) -> None:
    result = await forecasts.get_forecast(metric="does_not_exist")
    assert result["row_count"] == 0
