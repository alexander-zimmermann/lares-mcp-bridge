"""Generic bucketed aggregation over hypertables and continuous aggregates."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, LiteralString

from psycopg import sql

from .. import db
from ..interval import Interval
from . import sources

Aggregation = Literal["avg", "sum", "min", "max", "count"]
_AGG_FUNCS: dict[Aggregation, LiteralString] = {
    "avg": "AVG",
    "sum": "SUM",
    "min": "MIN",
    "max": "MAX",
    "count": "COUNT",
}


def _validate_filters(
    filters: dict[str, Any] | None,
    valid_columns: set[str],
) -> dict[str, Any]:
    if not filters:
        return {}
    bad = [k for k in filters if k not in valid_columns]
    if bad:
        raise ValueError(f"unknown_filter_columns: {bad}")
    return filters


async def query_timeseries(
    table: str,
    columns: list[str],
    from_ts: str | datetime,
    to_ts: str | datetime,
    aggregation: Aggregation = "avg",
    bucket: str = "1 hour",
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Bucketed aggregate query against a hypertable or continuous aggregate.

    - ``aggregation`` is one of ``avg|sum|min|max|count``.
    - When ``bucket`` is one hour or coarser AND a ``<table>_1h`` continuous
      aggregate exists, the query is transparently routed to the CAGG.
    - The result is capped at ``MCP_QUERY_ROW_LIMIT`` rows; over-shoot returns
      an error suggesting a coarser bucket.
    - ``filters`` is a flat ``{column: value}`` dict translated to ``=`` predicates.
    """
    if aggregation not in _AGG_FUNCS:
        raise ValueError(f"invalid_aggregation: {aggregation!r}")
    width = Interval.parse(bucket)

    source = await sources.resolve(table, width)
    schema = await sources.get_schema(source.name)
    valid_cols = {c["name"] for c in schema["columns"]}
    unknown = [c for c in columns if c not in valid_cols]
    if unknown:
        raise ValueError(f"unknown_columns: {unknown}")
    flt = _validate_filters(filters, valid_cols)

    select_parts: list[sql.Composable] = [
        sql.SQL("time_bucket(%s, {col}) AS bucket").format(col=sql.Identifier(source.time_column))
    ]
    for c in columns:
        select_parts.append(
            sql.SQL("{fn}({col}) AS {alias}").format(
                fn=sql.SQL(_AGG_FUNCS[aggregation]),
                col=sql.Identifier(c),
                alias=sql.Identifier(f"{c}_{aggregation}"),
            )
        )

    where_parts: list[sql.Composable] = [
        sql.SQL("{col} BETWEEN %s AND %s").format(col=sql.Identifier(source.time_column))
    ]
    params: list[Any] = [str(width), from_ts, to_ts]
    for k, v in flt.items():
        where_parts.append(sql.SQL("{col} = %s").format(col=sql.Identifier(k)))
        params.append(v)

    stmt = sql.SQL(
        "SELECT {selects} FROM {tbl} WHERE {where} GROUP BY bucket ORDER BY bucket"
    ).format(
        selects=sql.SQL(", ").join(select_parts),
        tbl=sql.Identifier(source.schema, source.name),
        where=sql.SQL(" AND ").join(where_parts),
    )
    result = await db.read(
        "query_timeseries",
        source.name,
        stmt,
        params,
        overflow="error",
        hint="widen the bucket or shorten the time range",
    )

    return {
        "table_requested": table,
        "table_used": source.name,
        "kind_used": source.kind,
        "bucket": str(width),
        "aggregation": aggregation,
        "row_count": len(result.rows),
        "rows": result.rows,
    }
