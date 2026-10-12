"""backtest_fault through the MCP layer: the engine's own back-test on the seeded container.

Nothing of the engine is replaced. The candidate goes through the engine's
schema, kind registry, measurement and fold exactly as Propose will send it,
reading the container's `knx_1h` and `ga_catalog` over the bridge's read
role. The scenario is the engine's own silence case: two power channels
reported every hour for a week and then fell quiet six hours before the
aggregate's newest bucket, so each has been silent six times its usual pause.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any

import pytest
from fastmcp import Client
from testcontainers.postgres import PostgresContainer

from lares_mcp_bridge import server

from .support import connect

_QUIET = {
    "1/3/10": "Appliance.GF.Kitchen.Freezer.Power",
    "1/3/20": "Appliance.GF.Kitchen.Fridge.Power",
}

_SILENCE = {
    "name": "candidate_silence",
    "sentence": "Ein Kanal schweigt länger als das Fünffache seiner Sendepause.",
    "unit": "× der üblichen Sendepause",
    "kind": "silence",
    "parameters": {"gap_factor": 5, "gap_quantile": 0.95},
    "scope": {"include": ["Appliance.%.Power"]},
    "target": {"per_main_group": True},
}

_EXTERNAL = {
    "name": "candidate_external",
    "sentence": "Der Systemdruck der Gastherme liegt unter 1,0 bar.",
    "unit": "bar",
    "kind": "external",
    "scope": {"include": "%.Gastherme.System-Druck-Anomalie"},
}

# Measured against the plant's expected yield: the one shape that needs the site file.
_YIELD = {
    "name": "candidate_pv",
    "sentence": "Die Anlage hat an einem Tag weniger erzeugt als die Prognose erwartet.",
    "unit": "× des erlaubten Fehlbetrags",
    "kind": "deviation",
    "expectation": "forecast_solar",
    "parameters": {"min_shortfall_pct": 35, "min_expected_kwh": 3},
    "target": {"ga": "15/4/11"},
}


@pytest.fixture(scope="module")
def quiet_channels(timescaledb_container: PostgresContainer) -> Iterator[datetime]:
    """Seed the two quiet channels and yield the aggregate's newest bucket; removed after."""
    conn = connect(timescaledb_container)
    try:
        row = conn.execute("SELECT max(bucket) FROM knx_1h").fetchone()
        assert row is not None
        frontier: datetime = row[0]
        conn.execute(
            """
            INSERT INTO ga_catalog (ga, name, room, function, dpt)
            VALUES ('1/3/20', 'Appliance.GF.Kitchen.Fridge.Power', 'Kitchen', 'Appliance', '7.012')
            """
        )
        conn.execute(
            """
            INSERT INTO knx (time, ga, knx_main, knx_middle, knx_sub, dpt, value)
            SELECT %(frontier)s - (h || ' hours')::interval, g.ga, 1, 3, g.sub, '7.012', 40
            FROM generate_series(6, 168) AS h,
                 (VALUES ('1/3/10', 10), ('1/3/20', 20)) AS g (ga, sub)
            """,
            {"frontier": frontier},
        )
        conn.execute("CALL refresh_continuous_aggregate('knx_1h', NULL, NULL)")
        yield frontier
    finally:
        conn.execute("DELETE FROM knx WHERE ga = ANY(%s)", (list(_QUIET),))
        conn.execute("DELETE FROM ga_catalog WHERE ga = '1/3/20'")
        conn.execute("CALL refresh_continuous_aggregate('knx_1h', NULL, NULL)")
        conn.close()


async def _backtest(**arguments: Any) -> dict[str, Any]:
    async with Client(server.mcp) as client:
        result = await client.call_tool("backtest_fault", arguments)
    data: dict[str, Any] = result.data
    return data


async def test_a_candidate_returns_the_episodes_it_would_have_produced(
    db_pool: None, quiet_channels: datetime
) -> None:
    frontier = quiet_channels

    result = await _backtest(candidate=_SILENCE, weeks=1)

    assert result["fault"] == "candidate_silence"
    assert result["kind"] == "silence"
    assert datetime.fromisoformat(result["frontier"]) == frontier
    assert datetime.fromisoformat(result["window_start"]) == frontier - timedelta(weeks=1)
    assert result["episode_count"] == 2
    assert result["truncated"] is False
    assert [(e["subject"], e["label"]) for e in result["episodes"]] == list(_QUIET.items())
    for episode in result["episodes"]:
        assert datetime.fromisoformat(episode["started_at"]) == frontier
        assert episode["ended_at"] is None
        # Six hours quiet against an hourly pause: six times it, in the fault's unit.
        assert episode["peak_score"] == 6.0
        assert episode["severity"] == 1


async def test_the_measurement_record_makes_no_episodes_readable(
    db_pool: None, quiet_channels: datetime
) -> None:
    """A scope that matched nothing says so, instead of passing for a quiet house."""
    candidate = {**_SILENCE, "scope": {"include": ["Nothing.%.Matches"]}}

    result = await _backtest(candidate=candidate, weeks=1)

    assert result["episodes"] == []
    assert result["measured"]["channels"] == 0


async def test_the_list_is_cut_to_the_limit_and_says_so(
    db_pool: None, quiet_channels: datetime
) -> None:
    result = await _backtest(candidate=_SILENCE, weeks=1, limit=1)

    assert result["episode_count"] == 2
    assert result["truncated"] is True
    assert [e["subject"] for e in result["episodes"]] == ["1/3/10"]


@pytest.mark.parametrize(
    ("candidate", "weeks", "reason"),
    [
        ({**_SILENCE, "parameters": {"gap_factor": 5}}, 1, "candidate_silence.*gap_quantile"),
        (_SILENCE, 53, "53 weeks reaches past the 52 weeks"),
        (_SILENCE, 0, "at least one week"),
        ({**_SILENCE, "target": {"ga": "0/0/230"}}, 1, "per_main_group"),
        (_EXTERNAL, 1, "Basalte"),
        (_YIELD, 1, "needs the site"),
    ],
    ids=["schema", "past-history", "under-a-week", "target-form", "external", "plant-yield"],
)
async def test_what_cannot_be_measured_is_refused_in_the_engines_words(
    db_pool: None, candidate: dict[str, Any], weeks: int, reason: str
) -> None:
    async with Client(server.mcp) as client:
        result = await client.call_tool("backtest_fault", {"candidate": candidate, "weeks": weeks})
    # A refusal is a result, not an error: the harness must not count it as a strike.
    assert re.search(f"backtest_refused: .*{reason}", result.data["refused"])
