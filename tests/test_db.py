"""The bounded read and the one write in ``db``: the seam every tool crosses."""

from __future__ import annotations

import psycopg
import pytest
from psycopg import sql

from lares_mcp_bridge import db
from lares_mcp_bridge.config import Settings

_KNX_NEWEST_FIRST = "SELECT time, ga, value FROM knx ORDER BY time DESC"


async def test_read_serializes_rows_and_reports_the_cap(db_pool: None, settings: Settings) -> None:
    result = await db.read("t", "knx", _KNX_NEWEST_FIRST, limit=3, overflow="truncate")
    assert len(result.rows) == 3
    assert result.limit == 3
    assert result.truncated is True
    assert isinstance(result.rows[0]["time"], str)  # datetimes arrive isoformat


async def test_read_without_limit_uses_the_configured_cap(
    db_pool: None, settings: Settings
) -> None:
    result = await db.read("t", "knx", _KNX_NEWEST_FIRST, overflow="truncate")
    assert result.limit == settings.query_row_limit
    assert result.truncated is False
    assert len(result.rows) == 201  # the whole seed fits under the default cap


async def test_read_caps_a_caller_limit_at_the_configured_cap(settings: Settings) -> None:
    await db.init_pool(settings.model_copy(update={"query_row_limit": 5}))
    try:
        result = await db.read("t", "knx", _KNX_NEWEST_FIRST, limit=500, overflow="truncate")
        assert result.limit == 5
        assert len(result.rows) == 5
        assert result.truncated is True
    finally:
        await db.close_pool()


async def test_read_errors_on_overflow_with_the_hint(settings: Settings) -> None:
    await db.init_pool(settings.model_copy(update={"query_row_limit": 5}))
    try:
        with pytest.raises(ValueError, match="row_limit_exceeded.*exceed 5 rows; shorten it"):
            await db.read("t", "knx", _KNX_NEWEST_FIRST, overflow="error", hint="shorten it")
    finally:
        await db.close_pool()


async def test_read_accepts_composed_statements(db_pool: None) -> None:
    stmt = sql.SQL("SELECT {col} FROM {tbl} ORDER BY {col} DESC").format(
        col=sql.Identifier("time"), tbl=sql.Identifier("knx")
    )
    result = await db.read("t", "knx", stmt, limit=1, overflow="truncate")
    assert len(result.rows) == 1


async def test_read_binds_params_before_the_limit(db_pool: None) -> None:
    result = await db.read(
        "t",
        "knx",
        "SELECT ga FROM knx WHERE ga = %s ORDER BY time DESC",
        ("1/2/3",),
        limit=2,
        overflow="truncate",
    )
    assert {r["ga"] for r in result.rows} == {"1/2/3"}


@pytest.mark.parametrize("limit", [0, -1])
async def test_read_rejects_a_non_positive_limit(db_pool: None, limit: int) -> None:
    with pytest.raises(ValueError, match="invalid_limit"):
        await db.read("t", "knx", _KNX_NEWEST_FIRST, limit=limit, overflow="truncate")


async def test_read_before_init_is_a_runtime_error() -> None:
    with pytest.raises(RuntimeError, match="init_pool"):
        await db.read("t", "knx", _KNX_NEWEST_FIRST, overflow="error")


async def test_write_returns_the_serialized_returning_rows(
    db_pool: None, settings: Settings
) -> None:
    conn = psycopg.connect(settings.db_dsn, autocommit=True)
    try:
        conn.execute("TRUNCATE TABLE episode_verdicts")
        episode = conn.execute("SELECT id FROM episodes ORDER BY id LIMIT 1").fetchone()
    finally:
        conn.close()
    assert episode is not None
    rows = await db.write(
        "t",
        "episode_verdicts",
        "INSERT INTO episode_verdicts (episode_id, verdict) VALUES (%s, %s)"
        " RETURNING episode_id, verdict, decided_at",
        (episode[0], "real"),
    )
    assert rows[0]["verdict"] == "real"
    assert isinstance(rows[0]["decided_at"], str)


async def test_write_without_credentials_names_the_missing_setting(settings: Settings) -> None:
    read_only = settings.model_copy(update={"db_write_username": "", "db_write_password": ""})
    await db.init_pool(read_only)
    assert await db.init_write_pool(read_only) is None
    try:
        with pytest.raises(RuntimeError, match="MCP_DB_WRITE_USERNAME"):
            await db.write("t", "episode_verdicts", "SELECT 1")
    finally:
        await db.close_pool()


async def test_lookup_runs_the_statement_whole_regardless_of_the_cap(settings: Settings) -> None:
    await db.init_pool(settings.model_copy(update={"query_row_limit": 2}))
    try:
        rows = await db.lookup("t", "ga_catalog", "SELECT ga FROM ga_catalog ORDER BY ga")
        assert len(rows) == 12  # every seeded group address, not the first two
    finally:
        await db.close_pool()


async def test_lookup_parses_placeholders_even_without_params(db_pool: None) -> None:
    # A literal percent sign has to be doubled — the same rule as read() and write().
    rows = await db.lookup("t", "_metadata", "SELECT format('%%s', 1) AS one")
    assert rows == [{"one": "1"}]
