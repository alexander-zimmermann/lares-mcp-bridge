"""TimescaleDB access: the two pools, and the bounded read every query tool goes through.

Two pools, not one: the read pool carries the SELECT-only role every query
tool uses, the write pool carries a role whose whole privilege is the
verdict table. Keeping them apart is what still makes the server read-only
for everything but that one table — the separation is in the grants, not
just in which function happens to call which pool.

Two reads: ``read()`` is the bounded one every data tool goes through — it
appends the LIMIT, fetches one row past it, and either refuses the result or
truncates and flags it, as the tool decides. ``lookup()`` is for the small
reads that must come back whole (catalog, validation lists, one row by key)
and runs its statement verbatim. Neither hands the tool a connection, a
metric label or the +1 trick. Statements are always placeholder-parsed, so a
literal ``%`` is written ``%%``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, LiteralString

import psycopg
from psycopg import sql
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

from . import metrics as metrics_module
from .config import Settings
from .logging_setup import get_logger

log = get_logger(__name__)

# The pool is generic in the row type; we lock it to DictRow so cursor reads
# return dicts everywhere — both at runtime (via row_factory=dict_row in
# kwargs) and for static analysis (via the type parameter).
_Pool = AsyncConnectionPool[psycopg.AsyncConnection[DictRow]]
_pool: _Pool | None = None
_write_pool: _Pool | None = None
_row_limit: int = 0

# The write pool serves one tool called by one person at a time; two
# connections is already generous.
_WRITE_POOL_MAX = 2

Statement = LiteralString | sql.SQL | sql.Composed

# What happens when a result would exceed its limit: aggregation tools refuse
# (a sum over a partial result is silently wrong), event-log tools keep the
# first ``limit`` rows and say so (newest-first stays meaningful).
Overflow = Literal["error", "truncate"]


@dataclass(frozen=True)
class Result:
    """Rows of one bounded read, datetimes already isoformat, plus the cap that shaped them."""

    rows: list[dict[str, Any]]
    limit: int
    truncated: bool


async def _open_pool(dsn: str, min_size: int, max_size: int) -> _Pool:
    pool: _Pool = AsyncConnectionPool(
        conninfo=dsn,
        min_size=min_size,
        max_size=max_size,
        kwargs={"autocommit": True, "row_factory": dict_row},
        open=False,
    )
    await pool.open(wait=True, timeout=10.0)
    return pool


async def init_pool(settings: Settings) -> _Pool:
    """Open the module-level read pool and take the row cap. Idempotent once open."""
    global _pool, _row_limit
    if _pool is not None:
        return _pool
    _pool = await _open_pool(settings.db_dsn, settings.db_pool_min, settings.db_pool_max)
    _row_limit = settings.query_row_limit
    log.info(
        "db_pool_ready",
        host=settings.db_host,
        database=settings.db_name,
        user=settings.db_username,
        pool_min=settings.db_pool_min,
        pool_max=settings.db_pool_max,
        row_limit=_row_limit,
    )
    return _pool


async def init_write_pool(settings: Settings) -> _Pool | None:
    """Open the write pool when write credentials are configured.

    Without them the server runs read-only and this is a no-op — a missing
    credential must never keep the query tools from serving.
    """
    global _write_pool
    if _write_pool is not None:
        return _write_pool
    if not settings.db_write_enabled:
        log.info("db_write_pool_disabled", reason="no write credentials configured")
        return None
    _write_pool = await _open_pool(settings.db_write_dsn, 0, _WRITE_POOL_MAX)
    log.info(
        "db_write_pool_ready",
        host=settings.db_host,
        database=settings.db_name,
        user=settings.db_write_username,
    )
    return _write_pool


async def close_pool() -> None:
    """Close and drop both module-level pools (no-op when already closed)."""
    global _pool, _write_pool
    if _pool is not None:
        await _pool.close()
        _pool = None
    if _write_pool is not None:
        await _write_pool.close()
        _write_pool = None


def _require_pool() -> _Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialised — call init_pool() first")
    return _pool


def _require_write_pool() -> _Pool:
    if _write_pool is None:
        raise RuntimeError(
            "no write credentials configured — set MCP_DB_WRITE_USERNAME /"
            " MCP_DB_WRITE_PASSWORD (or the *_FILE variants) to record verdicts"
        )
    return _write_pool


async def healthcheck() -> bool:
    """Round-trip ``SELECT 1``; False (never an exception) when the DB is unreachable."""
    try:
        async with _require_pool().connection() as conn:
            await conn.execute("SELECT 1")
        return True
    except Exception as exc:
        log.warning("db_healthcheck_failed", error=str(exc))
        return False


def _serialize(row: dict[str, Any]) -> dict[str, Any]:
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}


async def _execute(
    pool: _Pool, tool: str, table_used: str, stmt: Statement, params: Sequence[Any]
) -> list[dict[str, Any]]:
    m = metrics_module.get()
    m.db_queries.labels(tool=tool, table_used=table_used).inc()
    with m.db_query_duration.labels(tool=tool).time():
        async with pool.connection() as conn:
            rows = await (await conn.execute(stmt, params)).fetchall()
    return [_serialize(r) for r in rows]


async def read(
    tool: str,
    table_used: str,
    stmt: Statement,
    params: Sequence[Any] = (),
    *,
    overflow: Overflow,
    limit: int | None = None,
    hint: str = "",
) -> Result:
    """Run ``stmt`` on the read pool, bounded to ``limit`` rows.

    ``stmt`` is a complete SELECT without a trailing LIMIT; the effective limit
    is ``limit`` capped at the configured row limit, or the row limit itself when
    ``limit`` is None. The caller names what an overflow means: ``"error"``
    raises ``row_limit_exceeded`` with ``hint`` appended, ``"truncate"`` returns
    the first ``limit`` rows with ``truncated`` set. A non-positive ``limit`` is
    ``invalid_limit``.
    """
    pool = _require_pool()
    if limit is not None and limit <= 0:
        raise ValueError(f"invalid_limit: {limit}")
    effective = _row_limit if limit is None else min(limit, _row_limit)

    bounded = sql.SQL("{stmt} LIMIT %s").format(
        stmt=sql.SQL(stmt) if isinstance(stmt, str) else stmt
    )
    rows = await _execute(pool, tool, table_used, bounded, [*params, effective + 1])

    truncated = len(rows) > effective
    if truncated and overflow == "error":
        detail = f"; {hint}" if hint else ""
        raise ValueError(f"row_limit_exceeded: result would exceed {effective} rows{detail}")
    return Result(rows=rows[:effective], limit=effective, truncated=truncated)


async def lookup(
    tool: str, table_used: str, stmt: Statement, params: Sequence[Any] = ()
) -> list[dict[str, Any]]:
    """Run ``stmt`` verbatim on the read pool and return every row, serialized.

    For reads whose completeness is the point — a truncated column list or
    room list would be a wrong answer, not a shorter one. Any bound belongs
    in the statement itself.
    """
    return await _execute(_require_pool(), tool, table_used, stmt, params)


async def write(
    tool: str, table_used: str, stmt: Statement, params: Sequence[Any] = ()
) -> list[dict[str, Any]]:
    """Run ``stmt`` on the write pool and return its ``RETURNING`` rows, serialized."""
    return await _execute(_require_write_pool(), tool, table_used, stmt, params)
