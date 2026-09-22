"""The data sources: which hypertables and continuous aggregates exist, what
they look like, and which of them answers a request at a given bucket width.

``list_data_sources`` and ``get_schema`` are served to the LLM as tools;
``resolve`` is what the other tools call to route a coarse bucket to the
``*_1h`` aggregate instead of the raw hypertable.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cachetools import TTLCache
from psycopg import sql

from .. import db
from ..interval import Interval

KIND_HYPERTABLE = "hypertable"
KIND_CONTINUOUS_AGGREGATE = "continuous_aggregate"

# Every tool call funnels through list_data_sources() for validation and CAGG
# routing, and the underlying catalog walk (2 + 2N queries incl. a MIN/MAX scan
# per source) dwarfs the actual tool query. Hypertables and CAGGs change rarely,
# so a short single-entry TTL cache removes the repeat cost; new tables show up
# after at most one TTL period.
_SOURCES_CACHE_TTL_SECONDS = 60
# Alias pins the key/value types for both mypy and Pyright — the cachetools
# stubs cannot infer them from an otherwise-empty constructor.
_SourcesCache = TTLCache[str, list[dict[str, Any]]]
_sources_cache: _SourcesCache = _SourcesCache(maxsize=1, ttl=_SOURCES_CACHE_TTL_SECONDS)

# Label for reads against timescaledb_information / information_schema.
_METADATA = "_metadata"


def invalidate_cache() -> None:
    """Drop the cached data-source catalog (used by tests for isolation)."""
    _sources_cache.clear()


@dataclass(frozen=True)
class Source:
    """The table a query will actually run against, after CAGG routing."""

    name: str
    schema: str
    kind: str
    time_column: str


_LIST_HYPERTABLES_SQL = """
SELECT
    h.hypertable_schema AS schema,
    h.hypertable_name   AS name,
    obj_description(format('%%I.%%I', h.hypertable_schema, h.hypertable_name)::regclass, 'pg_class')
        AS description
FROM timescaledb_information.hypertables h
ORDER BY h.hypertable_name
"""

_LIST_CAGGS_SQL = """
SELECT
    c.view_schema AS schema,
    c.view_name   AS name,
    obj_description(format('%%I.%%I', c.view_schema, c.view_name)::regclass, 'pg_class')
        AS description
FROM timescaledb_information.continuous_aggregates c
ORDER BY c.view_name
"""

# Hypertables use `time` by convention in this project; CAGGs typically use
# `bucket`. The first ``timestamp with time zone`` column wins otherwise.
_TIME_COLUMN_SQL = """
SELECT column_name
FROM information_schema.columns
WHERE table_schema = %s AND table_name = %s
  AND data_type = 'timestamp with time zone'
ORDER BY
    CASE column_name
        WHEN 'time' THEN 0
        WHEN 'bucket' THEN 1
        ELSE 2
    END,
    ordinal_position
LIMIT 1
"""

_TIME_RANGE_SQL = sql.SQL("SELECT MIN({col}) AS min_ts, MAX({col}) AS max_ts FROM {tbl}")

_COLUMNS_SQL = """
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = %s AND table_name = %s
ORDER BY ordinal_position
"""

_JSONB_KEYS_SQL = sql.SQL(
    """
    SELECT key, COUNT(*)::bigint AS occurrences
    FROM (
        SELECT jsonb_object_keys({col}) AS key
        FROM {tbl}
        ORDER BY {time_col} DESC
        LIMIT %s
    ) sub
    GROUP BY key
    ORDER BY occurrences DESC
    LIMIT %s
    """
)
_JSONB_TOP_KEYS = 30


async def _detect_time_column(schema: str, name: str) -> str | None:
    found = await db.lookup("list_data_sources", _METADATA, _TIME_COLUMN_SQL, (schema, name))
    return str(found[0]["column_name"]) if found else None


async def _time_range(schema: str, name: str, time_col: str) -> dict[str, Any]:
    stmt = _TIME_RANGE_SQL.format(col=sql.Identifier(time_col), tbl=sql.Identifier(schema, name))
    (row,) = await db.lookup("list_data_sources", name, stmt)
    return {"min": row["min_ts"], "max": row["max_ts"]}


async def list_data_sources() -> list[dict[str, Any]]:
    """List all hypertables and continuous aggregates, with their time range.

    Each entry contains: ``schema``, ``name``, ``kind``, ``description``,
    ``time_column``, ``time_range`` (``min``/``max``).
    """
    cached = _sources_cache.get("sources")
    if cached is not None:
        return cached

    hyper = await db.lookup("list_data_sources", _METADATA, _LIST_HYPERTABLES_SQL)
    caggs = await db.lookup("list_data_sources", _METADATA, _LIST_CAGGS_SQL)
    rows = [{**r, "kind": KIND_HYPERTABLE} for r in hyper]
    rows += [{**r, "kind": KIND_CONTINUOUS_AGGREGATE} for r in caggs]

    out: list[dict[str, Any]] = []
    for r in rows:
        time_col = await _detect_time_column(r["schema"], r["name"])
        time_range: dict[str, Any] = {"min": None, "max": None}
        if time_col:
            time_range = await _time_range(r["schema"], r["name"], time_col)
        out.append(
            {
                "name": r["name"],
                "schema": r["schema"],
                "kind": r["kind"],
                "description": r["description"],
                "time_column": time_col,
                "time_range": time_range,
            }
        )
    _sources_cache["sources"] = out
    return out


async def resolve(table: str, bucket: Interval) -> Source:
    """The source a query at ``bucket`` width runs against.

    A request for a hypertable at an hourly or coarser bucket is routed to its
    ``<table>_1h`` continuous aggregate when one exists. Unknown tables and
    sources without a detectable time column are ``ValueError``.
    """
    by_name = {s["name"]: s for s in await list_data_sources()}
    if table not in by_name:
        raise ValueError(f"unknown_table: {table}")

    chosen = by_name[table]
    if (
        chosen["kind"] != KIND_CONTINUOUS_AGGREGATE
        and bucket.at_least_hourly
        and (cagg := by_name.get(f"{table}_1h"))
    ):
        chosen = cagg

    if chosen["time_column"] is None:
        raise ValueError(f"no_time_column_detected: {chosen['name']}")
    return Source(
        name=chosen["name"],
        schema=chosen["schema"],
        kind=chosen["kind"],
        time_column=chosen["time_column"],
    )


async def get_schema(table: str, jsonb_sample_size: int = 1000) -> dict[str, Any]:
    """Describe a table: columns, types, time column, JSONB key sample.

    The ``table`` name is validated against ``list_data_sources()``. Unknown
    tables raise ``ValueError``.
    """
    sources = await list_data_sources()
    match = next((s for s in sources if s["name"] == table), None)
    if match is None:
        raise ValueError(f"unknown_table: {table}")

    schema_name = match["schema"]
    columns = await db.lookup("get_schema", _METADATA, _COLUMNS_SQL, (schema_name, table))

    jsonb_samples: dict[str, list[dict[str, Any]]] = {}
    time_col = match["time_column"]
    if time_col:
        for col in (c["column_name"] for c in columns if c["data_type"] == "jsonb"):
            stmt = _JSONB_KEYS_SQL.format(
                col=sql.Identifier(col),
                tbl=sql.Identifier(schema_name, table),
                time_col=sql.Identifier(time_col),
            )
            sampled = await db.lookup(
                "get_schema", table, stmt, (jsonb_sample_size, _JSONB_TOP_KEYS)
            )
            jsonb_samples[col] = [
                {"key": r["key"], "occurrences": r["occurrences"]} for r in sampled
            ]

    hint = None
    if table == "knx":
        hint = (
            "Prefer querying ga_catalog_view (knx joined with ga_catalog) — it "
            "exposes ga_name, room, function, description per row, so queries can "
            "filter or group by room/function without round-tripping the catalog."
        )

    return {
        "name": table,
        "schema": schema_name,
        "kind": match["kind"],
        "time_column": time_col,
        "columns": [
            {
                "name": c["column_name"],
                "type": c["data_type"],
                "nullable": c["is_nullable"] == "YES",
            }
            for c in columns
        ],
        "jsonb_top_keys": jsonb_samples,
        "hint": hint,
    }
