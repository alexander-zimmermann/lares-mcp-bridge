"""Tests for the verdict loop: listing the episodes the engine holds and
attaching a binary verdict to one of them."""

from __future__ import annotations

from collections.abc import AsyncIterator

import psycopg
import pytest
import pytest_asyncio

from lares_mcp_bridge import db
from lares_mcp_bridge.config import Settings
from lares_mcp_bridge.tools import episodes


@pytest_asyncio.fixture
async def clean_verdicts(settings: Settings, db_pool: None) -> AsyncIterator[None]:
    """Every test starts with no verdicts on the seeded episodes."""

    def truncate() -> None:
        conn = psycopg.connect(settings.db_dsn, autocommit=True)
        try:
            conn.execute("TRUNCATE TABLE episode_verdicts")
        finally:
            conn.close()

    truncate()
    yield
    truncate()


async def _episode_id(fault: str, *, open_only: bool = False) -> int:
    result = await episodes.list_episodes(fault=fault, days=365)
    rows = [r for r in result["episodes"] if not open_only or r["ended_at"] is None]
    return int(rows[0]["episode_id"])


async def test_list_episodes_resolves_subject_against_the_catalog(clean_verdicts: None) -> None:
    result = await episodes.list_episodes(days=365)
    by_subject = {r["subject"]: r for r in result["episodes"]}
    assert by_subject["knx [1/2/2]"]["affected"] == "Lighting.1F.Bedroom.Ceiling"
    assert by_subject["knx [1/2/2]"]["room"] == "Bedroom"
    # No group address in the subject — the subject itself is the best label.
    assert by_subject["ems boiler"]["affected"] == "ems boiler"


async def test_list_episodes_filters_by_state_and_window(clean_verdicts: None) -> None:
    open_only = await episodes.list_episodes(state="open", days=365)
    assert [r["fault"] for r in open_only["episodes"]] == ["silence"]
    assert all(r["ended_at"] is None for r in open_only["episodes"])

    ended_only = await episodes.list_episodes(state="ended", days=365)
    assert all(r["ended_at"] is not None for r in ended_only["episodes"])
    assert ended_only["row_count"] == 3

    # The window is an overlap, so the episode that started 5 hours ago and
    # the one that ran three days ago both fall inside a week.
    recent = await episodes.list_episodes(days=7)
    assert {r["fault"] for r in recent["episodes"]} == {"silence"}
    assert recent["row_count"] == 2


async def test_set_verdict_is_read_back_on_the_episode(clean_verdicts: None) -> None:
    episode_id = await _episode_id("silence", open_only=True)

    written = await episodes.set_episode_verdict(episode_id=episode_id, verdict="nonsense")
    assert written["verdict"] == "nonsense"
    assert written["episode_id"] == episode_id
    assert written["fault"] == "silence"

    listed = await episodes.list_episodes(days=365)
    on_episode = next(r for r in listed["episodes"] if r["episode_id"] == episode_id)
    assert on_episode["verdict"] == "nonsense"
    assert on_episode["decided_at"] is not None


async def test_second_verdict_overwrites_and_never_duplicates(
    settings: Settings, clean_verdicts: None
) -> None:
    episode_id = await _episode_id("silence", open_only=True)

    await episodes.set_episode_verdict(episode_id=episode_id, verdict="nonsense")
    second = await episodes.set_episode_verdict(episode_id=episode_id, verdict="real")
    assert second["verdict"] == "real"

    conn = psycopg.connect(settings.db_dsn, autocommit=True)
    try:
        row = conn.execute(
            "SELECT count(*) FROM episode_verdicts WHERE episode_id = %s", (episode_id,)
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] == 1


async def test_list_episodes_can_narrow_to_the_unjudged_ones(clean_verdicts: None) -> None:
    episode_id = await _episode_id("silence", open_only=True)
    await episodes.set_episode_verdict(episode_id=episode_id, verdict="real")

    unjudged = await episodes.list_episodes(days=365, only_unjudged=True)
    assert episode_id not in [r["episode_id"] for r in unjudged["episodes"]]
    assert all(r["verdict"] is None for r in unjudged["episodes"])


async def test_one_episode_reads_back_outside_the_default_window(clean_verdicts: None) -> None:
    """A verdict must be readable without guessing how wide the window has to
    be — the oldest seeded episode is 40 days back, far outside the default."""
    oldest = await _episode_id("constancy")
    await episodes.set_episode_verdict(episode_id=oldest, verdict="real")

    named = await episodes.list_episodes(episode_id=oldest)
    assert named["row_count"] == 1
    assert named["episodes"][0]["verdict"] == "real"
    assert named["days"] is None


async def test_invalid_window_and_state_are_refused(clean_verdicts: None) -> None:
    with pytest.raises(ValueError, match="invalid_days"):
        await episodes.list_episodes(days=0)
    with pytest.raises(ValueError, match="open, ended"):
        await episodes.list_episodes(state="offen")  # type: ignore[arg-type]


async def test_unknown_verdict_names_the_valid_ones(clean_verdicts: None) -> None:
    episode_id = await _episode_id("silence", open_only=True)
    with pytest.raises(ValueError, match="nonsense"):
        await episodes.set_episode_verdict(episode_id=episode_id, verdict="maybe")


async def test_verdict_on_an_unknown_episode_is_a_precise_error(clean_verdicts: None) -> None:
    with pytest.raises(ValueError, match="999999"):
        await episodes.set_episode_verdict(episode_id=999999, verdict="real")


async def test_read_only_server_serves_reads_and_refuses_verdicts(settings: Settings) -> None:
    """Without write credentials the server keeps querying and says plainly
    why it cannot record a verdict — a missing secret must not take the read
    tools down with it."""
    read_only = settings.model_copy(update={"db_write_username": "", "db_write_password": ""})
    await db.init_pool(read_only)
    assert await db.init_write_pool(read_only) is None
    try:
        listed = await episodes.list_episodes(days=365)
        assert listed["row_count"] > 0
        with pytest.raises(RuntimeError, match="MCP_DB_WRITE_USERNAME"):
            await episodes.set_episode_verdict(
                episode_id=listed["episodes"][0]["episode_id"], verdict="real"
            )
    finally:
        await db.close_pool()
