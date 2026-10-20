"""Tests for the one write the server has: a person's verdict on an episode
or on a run, through one tool with two targets."""

from __future__ import annotations

import psycopg
import pytest

from lares_mcp_bridge import db
from lares_mcp_bridge.config import Settings
from lares_mcp_bridge.tools import episodes, runs, verdicts


async def _open_episode_id() -> int:
    listed = await episodes.list_episodes(fault="silence", state="open", days=365)
    return int(listed["episodes"][0]["episode_id"])


async def test_verdict_on_an_episode_is_read_back_on_it(clean_verdicts: None) -> None:
    episode_id = await _open_episode_id()

    written = await verdicts.set_verdict(
        enduser_id=None, target="episode", verdict="nonsense", episode_id=episode_id
    )
    assert written["target"] == "episode"
    assert written["verdict"] == "nonsense"
    assert written["episode_id"] == episode_id
    assert written["fault"] == "silence"

    listed = await episodes.list_episodes(days=365)
    on_episode = next(r for r in listed["episodes"] if r["episode_id"] == episode_id)
    assert on_episode["verdict"] == "nonsense"
    assert on_episode["decided_at"] is not None


async def test_second_verdict_on_an_episode_overwrites(
    settings: Settings, clean_verdicts: None
) -> None:
    episode_id = await _open_episode_id()

    await verdicts.set_verdict(
        enduser_id=None, target="episode", verdict="nonsense", episode_id=episode_id
    )
    second = await verdicts.set_verdict(
        enduser_id=None, target="episode", verdict="real", episode_id=episode_id
    )
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


async def test_verdict_on_a_run_named_by_its_id(clean_verdicts: None) -> None:
    explained = await runs.list_runs(enduser_id=None, use_case="explain-episode")
    run_id = explained["runs"][0]["run_id"]

    written = await verdicts.set_verdict(
        enduser_id=None, target="run", verdict="helpful", run_id=run_id
    )
    assert written["target"] == "run"
    assert written["run_id"] == run_id
    assert written["use_case"] == "explain-episode"
    assert written["verdict"] == "helpful"
    assert written["verdict_at"] is not None

    read_back = await runs.list_runs(enduser_id=None, run_id=run_id)
    assert read_back["runs"][0]["verdict"] == "helpful"


async def test_verdict_on_the_newest_run_of_a_subject(clean_verdicts: None) -> None:
    """ "Die Erklärung dazu war Unsinn" names the episode, not the run: the
    newest run on that subject is the one meant."""
    episode_id = await _open_episode_id()

    written = await verdicts.set_verdict(
        enduser_id=None,
        target="run",
        verdict="useless",
        subject_kind="episode",
        subject_key=str(episode_id),
    )
    assert written["subject_key"].startswith(f"{episode_id}:")

    newest = (await runs.list_runs(enduser_id=None, use_case="explain-episode"))["runs"][0]
    assert written["run_id"] == newest["run_id"]


async def test_verdict_without_an_address_lands_on_the_callers_newest_chat_run(
    clean_verdicts: None,
) -> None:
    """ "Das war hilfreich" said in the chat means the answer just given to the
    one who said it — not Lou's newer turn, not the newer one without a person."""
    written = await verdicts.set_verdict(enduser_id="kim", target="run", verdict="helpful")

    assert written["use_case"] == "messenger"
    assert written["subject_key"] == "session-k:2"


async def test_verdict_without_an_address_and_without_a_person(clean_verdicts: None) -> None:
    """Without a handle the caller's chat runs are the ones without a person."""
    written = await verdicts.set_verdict(enduser_id=None, target="run", verdict="helpful")

    assert written["subject_key"] == "session-a:2"


async def test_another_persons_run_is_never_confirmed(clean_verdicts: None) -> None:
    """Named by id or by subject, a run outside the caller's rows answers as
    one that does not exist, and stays unjudged."""
    lou_run = (await runs.list_runs(enduser_id="lou", subject_kind="chat"))["runs"][0]

    with pytest.raises(ValueError, match=f"unknown_run: {lou_run['run_id']}"):
        await verdicts.set_verdict(
            enduser_id="kim", target="run", verdict="useless", run_id=lou_run["run_id"]
        )
    with pytest.raises(ValueError, match="no_run_on_subject: chat/session-l:1"):
        await verdicts.set_verdict(
            enduser_id="kim",
            target="run",
            verdict="useless",
            subject_kind="chat",
            subject_key="session-l:1",
        )
    with pytest.raises(ValueError, match="unknown_run"):
        await verdicts.set_verdict(
            enduser_id=None, target="run", verdict="useless", run_id=lou_run["run_id"]
        )

    after = await runs.list_runs(enduser_id="lou", run_id=lou_run["run_id"])
    assert after["runs"][0]["verdict"] is None


async def test_a_person_judges_a_machine_run(clean_verdicts: None) -> None:
    explained = (await runs.list_runs(enduser_id="kim", use_case="explain-episode"))["runs"][0]

    written = await verdicts.set_verdict(
        enduser_id="kim", target="run", verdict="helpful", run_id=explained["run_id"]
    )
    assert written["run_id"] == explained["run_id"]


async def test_second_verdict_on_a_run_overwrites(clean_verdicts: None) -> None:
    run_id = (await runs.list_runs(enduser_id=None, use_case="messenger"))["runs"][0]["run_id"]

    first = await verdicts.set_verdict(
        enduser_id=None, target="run", verdict="useless", run_id=run_id
    )
    second = await verdicts.set_verdict(
        enduser_id=None, target="run", verdict="helpful", run_id=run_id
    )
    assert second["verdict"] == "helpful"
    assert second["verdict_at"] >= first["verdict_at"]

    judged = [
        r
        for r in (await runs.list_runs(enduser_id=None, days=30))["runs"]
        if r["verdict"] is not None
    ]
    assert [r["run_id"] for r in judged] == [run_id]


async def test_a_verdict_never_touches_the_output_it_judges(clean_verdicts: None) -> None:
    """The column grant is the guard in the cluster; here the assertion is
    that the tool writes nothing else on the row."""
    explained = await runs.list_runs(enduser_id=None, use_case="explain-episode")
    run_id = explained["runs"][0]["run_id"]
    before = (await runs.list_runs(enduser_id=None, run_id=run_id))["runs"][0]

    await verdicts.set_verdict(enduser_id=None, target="run", verdict="helpful", run_id=run_id)

    after = (await runs.list_runs(enduser_id=None, run_id=run_id))["runs"][0]
    assert {k: v for k, v in after.items() if k not in ("verdict", "verdict_at")} == {
        k: v for k, v in before.items() if k not in ("verdict", "verdict_at")
    }


async def test_the_word_pair_belongs_to_the_target(clean_verdicts: None) -> None:
    episode_id = await _open_episode_id()
    with pytest.raises(ValueError, match="real, nonsense"):
        await verdicts.set_verdict(
            enduser_id=None, target="episode", verdict="helpful", episode_id=episode_id
        )
    with pytest.raises(ValueError, match="helpful, useless"):
        await verdicts.set_verdict(enduser_id=None, target="run", verdict="real")
    with pytest.raises(ValueError, match="episode, run"):
        await verdicts.set_verdict(enduser_id=None, target="erklärung", verdict="real")  # type: ignore[arg-type]


async def test_an_unknown_subject_is_a_precise_error(clean_verdicts: None) -> None:
    with pytest.raises(ValueError, match="unknown_episode: 999999"):
        await verdicts.set_verdict(
            enduser_id=None, target="episode", verdict="real", episode_id=999999
        )
    with pytest.raises(ValueError, match="unknown_run: 999999"):
        await verdicts.set_verdict(enduser_id=None, target="run", verdict="helpful", run_id=999999)
    with pytest.raises(ValueError, match="no_run_on_subject"):
        await verdicts.set_verdict(
            enduser_id=None,
            target="run",
            verdict="helpful",
            subject_kind="alert_group",
            subject_key="nope",
        )


async def test_an_ambiguous_or_missing_address_is_refused(clean_verdicts: None) -> None:
    episode_id = await _open_episode_id()
    with pytest.raises(ValueError, match="missing_episode_id"):
        await verdicts.set_verdict(enduser_id=None, target="episode", verdict="real")
    with pytest.raises(ValueError, match="ambiguous_run_address"):
        await verdicts.set_verdict(
            enduser_id=None, target="run", verdict="helpful", run_id=1, subject_kind="chat"
        )
    with pytest.raises(ValueError, match="episode_id"):
        await verdicts.set_verdict(
            enduser_id=None, target="run", verdict="helpful", episode_id=episode_id
        )
    # A subject is a kind and a key together; half of one names nothing.
    with pytest.raises(ValueError, match="incomplete_subject_address"):
        await verdicts.set_verdict(
            enduser_id=None, target="run", verdict="helpful", subject_kind="chat"
        )
    with pytest.raises(ValueError, match="incomplete_subject_address"):
        await verdicts.set_verdict(
            enduser_id=None, target="run", verdict="helpful", subject_key="session-a:2"
        )


async def test_read_only_server_serves_reads_and_refuses_both_verdicts(
    settings: Settings,
) -> None:
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
            await verdicts.set_verdict(
                enduser_id=None,
                target="episode",
                verdict="real",
                episode_id=listed["episodes"][0]["episode_id"],
            )
        with pytest.raises(RuntimeError, match="MCP_DB_WRITE_USERNAME"):
            await verdicts.set_verdict(enduser_id=None, target="run", verdict="helpful")
    finally:
        await db.close_pool()
