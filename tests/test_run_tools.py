"""Tests for the ledger list: which runs the platform made, and what they cost."""

from __future__ import annotations

import pytest
import structlog
from fastmcp import Client

from lares_mcp_bridge import server
from lares_mcp_bridge.tools import runs, verdicts


@pytest.fixture
def kims_connector(clean_context: None) -> None:
    """What GatewayAuth binds for a call Kim signed in to through the claude.ai connector."""
    structlog.contextvars.bind_contextvars(**{"client_id": "claude-ai", "enduser.id": "kim"})


async def test_list_runs_defaults_to_the_last_week(clean_verdicts: None) -> None:
    """The failed scheduled run is ten days old, so the default window leaves
    it out and a wider one brings it back."""
    recent = await runs.list_runs(enduser_id=None)
    assert {r["use_case"] for r in recent["runs"]} == {"explain-episode", "messenger"}

    wide = await runs.list_runs(enduser_id=None, days=30)
    assert {r["use_case"] for r in wide["runs"]} == {
        "explain-episode",
        "messenger",
        "propose-faults",
    }
    assert wide["days"] == 30


async def test_list_runs_is_newest_first_with_the_numbers_of_a_run(clean_verdicts: None) -> None:
    listed = await runs.list_runs(enduser_id=None)
    created = [r["created_at"] for r in listed["runs"]]
    assert created == sorted(created, reverse=True)

    newest = listed["runs"][0]
    assert newest["use_case"] == "messenger"
    assert newest["tldr"] == "Die Wallbox lädt mit 6,1 kW."
    assert newest["model"] == "gpt-5.5"
    assert newest["tokens_in"] == 2100
    assert newest["cost"] == pytest.approx(0.010)
    assert newest["duration_seconds"] == pytest.approx(9.0)
    # The full text is a single-run read, never a column of the list.
    assert "text" not in newest


async def test_list_runs_bounds_by_use_case_subject_and_status(clean_verdicts: None) -> None:
    by_use_case = await runs.list_runs(enduser_id=None, use_case="explain-episode", days=30)
    assert by_use_case["row_count"] == 2
    assert all(r["subject_kind"] == "episode" for r in by_use_case["runs"])

    by_kind = await runs.list_runs(enduser_id=None, subject_kind="chat", days=30)
    assert {r["use_case"] for r in by_kind["runs"]} == {"messenger"}

    by_key = await runs.list_runs(
        enduser_id=None, subject_kind="chat", subject_key="session-a:1", days=30
    )
    assert by_key["row_count"] == 1
    assert by_key["runs"][0]["tldr"] == "Heute Nacht war nichts los."

    failed = await runs.list_runs(enduser_id=None, status="failed", days=30)
    assert failed["row_count"] == 1
    assert failed["runs"][0]["error"] == "model source unreachable"


async def test_one_run_reads_back_whole_outside_the_window(clean_verdicts: None) -> None:
    """A named run is answered whatever its age, and it is the only read that
    carries the output text — that is what makes judging it possible."""
    failed = (await runs.list_runs(enduser_id=None, status="failed", days=30))["runs"][0]

    named = await runs.list_runs(enduser_id=None, run_id=failed["run_id"])
    assert named["row_count"] == 1
    assert named["days"] is None
    assert named["runs"][0]["run_id"] == failed["run_id"]
    assert named["runs"][0]["text"] is None

    explained = (await runs.list_runs(enduser_id=None, use_case="explain-episode"))["runs"][0]
    whole = await runs.list_runs(enduser_id=None, run_id=explained["run_id"])
    assert whole["runs"][0]["text"].startswith("Seit der Eskalation")


async def test_list_runs_can_narrow_to_the_unjudged_ones(clean_verdicts: None) -> None:
    judged = (await runs.list_runs(enduser_id=None))["runs"][0]["run_id"]
    await verdicts.set_verdict(enduser_id=None, target="run", verdict="helpful", run_id=judged)

    unjudged = await runs.list_runs(enduser_id=None, days=30, only_unjudged=True)
    assert judged not in [r["run_id"] for r in unjudged["runs"]]
    assert all(r["verdict"] is None for r in unjudged["runs"])


async def test_a_person_reads_their_own_runs_and_the_machines(clean_verdicts: None) -> None:
    listed = await runs.list_runs(enduser_id="kim")

    assert {r["enduser_id"] for r in listed["runs"]} == {"kim", None}
    assert {r["subject_key"] for r in listed["runs"] if r["enduser_id"] == "kim"} == {
        "session-k:1",
        "session-k:2",
    }
    # The explanations are the machines' and belong to everybody.
    assert "explain-episode" in {r["use_case"] for r in listed["runs"]}


async def test_a_caller_without_a_person_reads_the_machines_runs_only(
    clean_verdicts: None,
) -> None:
    listed = await runs.list_runs(enduser_id=None, days=30)

    assert listed["row_count"] > 0
    assert {r["enduser_id"] for r in listed["runs"]} == {None}


async def test_another_persons_run_is_not_found_even_by_its_id(clean_verdicts: None) -> None:
    """Named outside the caller's rows, a run answers as if it did not exist."""
    lou_run = (await runs.list_runs(enduser_id="lou", subject_kind="chat"))["runs"][0]
    assert lou_run["enduser_id"] == "lou"

    named = await runs.list_runs(enduser_id="kim", run_id=lou_run["run_id"])
    assert named["row_count"] == 0
    assert (await runs.list_runs(enduser_id=None, run_id=lou_run["run_id"]))["row_count"] == 0


async def test_a_run_names_its_person_surface_and_background_review(
    clean_verdicts: None,
) -> None:
    newest = (await runs.list_runs(enduser_id="kim", subject_key="session-k:2"))["runs"][0]

    assert newest["enduser_id"] == "kim"
    assert newest["surface"] == "discord"
    assert newest["review_tokens_in"] == 30000
    assert newest["review_tokens_out"] == 400
    assert newest["review_cost"] == pytest.approx(0.003)
    assert newest["review_text"] == "Nothing to save."

    machine = (await runs.list_runs(enduser_id="kim", use_case="explain-episode"))["runs"][0]
    assert machine["enduser_id"] is None
    assert machine["surface"] is None
    assert machine["review_cost"] is None


async def test_invalid_window_kind_and_status_are_refused(clean_verdicts: None) -> None:
    with pytest.raises(ValueError, match="invalid_days"):
        await runs.list_runs(enduser_id=None, days=0)
    with pytest.raises(ValueError, match="alert_group"):
        await runs.list_runs(enduser_id=None, subject_kind="raum")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="capped"):
        await runs.list_runs(enduser_id=None, status="kaputt")  # type: ignore[arg-type]


async def test_the_caller_the_request_names_is_the_one_whose_runs_come_back(
    clean_verdicts: None, kims_connector: None
) -> None:
    """Through the MCP layer: the handle bound for the request decides the rows,
    for the list and for a verdict given without an address."""
    async with Client(server.mcp) as client:
        listed = await client.call_tool("list_runs", {})
        judged = await client.call_tool("set_verdict", {"target": "run", "verdict": "helpful"})

    assert {r["enduser_id"] for r in listed.data["runs"]} == {"kim", None}
    assert judged.data["subject_key"] == "session-k:2"
