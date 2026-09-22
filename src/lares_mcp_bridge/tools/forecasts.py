"""Read-only views into the forecast table for Claude.

``get_forecast`` is the generic read; ``get_pv_forecast`` and
``get_weather_forecast`` are thin specialisations that pin the
``metric``/``model`` an LLM would otherwise have to guess and reshape the
output for easy consumption. All data is produced by batch jobs
(forecast-solar, forecast-weather) — these tools never call an external API
themselves, and none of them write.
"""

from __future__ import annotations

from typing import Any

from psycopg import sql

from .. import db

# forecast-solar CronJob writes PV production (watts) under this triple.
PV_METRIC = "pv_production"
PV_MODEL = "forecast_solar"

# forecast-weather CronJob writes one row per (hour, metric) under this model.
WEATHER_MODEL = "open_meteo"
_MAX_HORIZON_HOURS = 24 * 14
# A truncated forecast would pivot a partial last hour, so an overflow refuses.
_HORIZON_HINT = "shorten the horizon"


async def get_forecast(
    *,
    metric: str,
    horizon_hours: int = 24,
    model: str | None = None,
) -> dict[str, Any]:
    """Recent stored forecasts for ``metric`` over the next
    ``horizon_hours`` hours. Read-only — rows are populated by the
    ``forecast-solar`` (PV) and ``score-seasonal`` (statsforecast)
    CronJobs."""
    horizon_hours = max(1, min(horizon_hours, _MAX_HORIZON_HOURS))
    where: list[sql.Composable] = [
        sql.SQL("metric = %s"),
        sql.SQL("forecast_for BETWEEN now() AND now() + make_interval(hours => %s)"),
    ]
    params: list[Any] = [metric, horizon_hours]
    if model:
        where.append(sql.SQL("model = %s"))
        params.append(model)

    stmt = sql.SQL(
        """
        SELECT forecast_for, created_at, source, metric, model,
               forecast_value, forecast_lower, forecast_upper
        FROM mcp_forecasts
        WHERE {where}
        ORDER BY forecast_for, model
        """
    ).format(where=sql.SQL(" AND ").join(where))
    result = await db.read(
        "get_forecast", "mcp_forecasts", stmt, params, overflow="error", hint=_HORIZON_HINT
    )

    return {
        "metric": metric,
        "horizon_hours": horizon_hours,
        "row_count": len(result.rows),
        "rows": result.rows,
    }


async def get_pv_forecast(*, hours: int = 48) -> dict[str, Any]:
    """Hour-by-hour PV production forecast (watts) for the next ``hours``.

    Specialises ``get_forecast`` to the forecast.solar rows so the caller
    need not know the metric/model strings. When the forecast-solar job
    hasn't populated the requested window, ``note`` flags it instead of
    silently returning an empty list.
    """
    result = await get_forecast(metric=PV_METRIC, model=PV_MODEL, horizon_hours=hours)
    if result["row_count"] == 0:
        result["note"] = "PV forecast not available — check forecast-solar CronJob health"
    return result


async def get_weather_forecast(*, hours: int = 48) -> dict[str, Any]:
    """Hour-by-hour weather forecast for the next ``hours``, pivoted so each
    hour is one dict with the available metrics (``temperature``,
    ``cloud_cover``, ``precipitation``, ``solar_radiation``, ``wind_speed``).

    Reads the Open-Meteo (DWD ICON) rows written by the forecast-weather job
    — no live API call. When the job hasn't populated the requested window,
    ``note`` flags it instead of returning a silent empty list.
    """
    hours = max(1, min(hours, _MAX_HORIZON_HOURS))
    stmt = """
        SELECT forecast_for, metric, forecast_value
        FROM mcp_forecasts
        WHERE model = %s
          AND forecast_for BETWEEN now() AND now() + make_interval(hours => %s)
        ORDER BY forecast_for
    """
    result = await db.read(
        "get_weather_forecast",
        "mcp_forecasts",
        stmt,
        (WEATHER_MODEL, hours),
        overflow="error",
        hint=_HORIZON_HINT,
    )

    # Pivot per-metric rows into one dict per hour. Rows arrive ordered by
    # forecast_for, so insertion order keeps the hours chronological.
    buckets: dict[str, dict[str, Any]] = {}
    for row in result.rows:
        bucket = buckets.setdefault(row["forecast_for"], {"forecast_for": row["forecast_for"]})
        bucket[row["metric"]] = row["forecast_value"]
    hourly = list(buckets.values())

    out: dict[str, Any] = {
        "model": WEATHER_MODEL,
        "horizon_hours": hours,
        "row_count": len(hourly),
        "hours": hourly,
    }
    if not hourly:
        out["note"] = "weather forecast not available — check forecast-weather CronJob health"
    return out
