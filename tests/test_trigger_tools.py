"""start_run tests against a respx-mocked lares-agent-trigger.

The tool carries the request to the trigger's `POST /api/runs` with the
shared key and hands back what the trigger answered; the trigger alone
decides whether a run starts. Nothing here needs the database.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx

from lares_mcp_bridge.config import Settings
from lares_mcp_bridge.tools import trigger

from .support import GATEWAY_KEY, TRIGGER_KEY, TRIGGER_URL


def _settings(trigger_url: str | None = None, trigger_key_file: str | None = None) -> Settings:
    return Settings(
        db_host="localhost",
        db_name="x",
        db_username="x",
        db_password="x",
        gateway_key=GATEWAY_KEY,
        nats_enabled=False,
        trigger_url=trigger_url,
        trigger_key_file=trigger_key_file,
    )


# ---------------------------------------------------------------- settings


def test_trigger_settings_must_be_set_together(trigger_key_file: Path) -> None:
    with pytest.raises(ValueError, match="MCP_TRIGGER_URL and MCP_TRIGGER_KEY_FILE"):
        _settings(trigger_url=TRIGGER_URL)
    with pytest.raises(ValueError, match="MCP_TRIGGER_URL and MCP_TRIGGER_KEY_FILE"):
        _settings(trigger_key_file=str(trigger_key_file))


def test_trigger_enabled_only_when_both_set(trigger_key_file: Path) -> None:
    assert _settings().trigger_enabled is False
    enabled = _settings(trigger_url=TRIGGER_URL, trigger_key_file=str(trigger_key_file))
    assert enabled.trigger_enabled


# ---------------------------------------------------------------- lifecycle


async def test_start_run_refuses_when_not_initialised() -> None:
    with pytest.raises(RuntimeError, match="MCP_TRIGGER_URL"):
        await trigger.start_run("explain-episode", "15510")


async def test_init_rejects_empty_key_file(tmp_path: Path) -> None:
    empty = tmp_path / "trigger-api-key"
    empty.write_text("\n", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        await trigger.init(_settings(trigger_url=TRIGGER_URL, trigger_key_file=str(empty)))


# ---------------------------------------------------------------- start_run


async def test_an_episode_run_is_forwarded_with_the_key(
    trigger_client: None, trigger_router: respx.MockRouter
) -> None:
    answer = {
        "use_case": "explain-episode",
        "run_id": 17,
        "subject_key": "15510:message:20261002T142005Z",
        "status": "queued",
        "output": ["stored", "discord", "mail"],
    }
    trigger_router.post("/api/runs").mock(return_value=httpx.Response(202, json=answer))

    out = await trigger.start_run("explain-episode", "15510")

    assert out == answer
    request = trigger_router.calls.last.request
    assert request.headers["authorization"] == f"Bearer {TRIGGER_KEY}"
    assert json.loads(request.content) == {"use_case": "explain-episode", "subject": "15510"}


async def test_a_schedule_run_is_sent_without_a_subject(
    trigger_client: None, trigger_router: respx.MockRouter
) -> None:
    trigger_router.post("/api/runs").mock(
        return_value=httpx.Response(
            202,
            json={"use_case": "propose-faults", "job_id": "b0b000000001", "status": "requested"},
        )
    )

    out = await trigger.start_run("propose-faults")

    assert out["status"] == "requested"
    assert json.loads(trigger_router.calls.last.request.content) == {"use_case": "propose-faults"}


async def test_a_schedule_run_carries_what_the_owner_asked_for(
    trigger_client: None, trigger_router: respx.MockRouter
) -> None:
    trigger_router.post("/api/runs").mock(
        return_value=httpx.Response(
            202,
            json={"use_case": "propose-faults", "job_id": "b0b000000001", "status": "requested"},
        )
    )

    await trigger.start_run("propose-faults", focus="ein Fault für den Trockner")

    assert json.loads(trigger_router.calls.last.request.content) == {
        "use_case": "propose-faults",
        "focus": "ein Fault für den Trockner",
    }


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (404, "no episode 99999"),
        (409, "summarise-week is dormant: Waits for its skill."),
        (429, "explain-episode has spent its 10 runs today"),
    ],
)
async def test_a_refusal_carries_the_triggers_reason(
    trigger_client: None, trigger_router: respx.MockRouter, status: int, reason: str
) -> None:
    """The chat says why nothing started, in the trigger's words."""
    trigger_router.post("/api/runs").mock(
        return_value=httpx.Response(status, json={"error": reason})
    )

    with pytest.raises(ValueError, match=f"run_not_started: {reason}"):
        await trigger.start_run("explain-episode", "99999")


@pytest.mark.parametrize(
    "failure",
    [
        httpx.Response(503, json={"error": "the ledger or the harness did not answer"}),
        httpx.Response(502, text="Bad Gateway"),
        httpx.ConnectError("connection refused"),
    ],
    ids=["503", "no-json", "unreachable"],
)
async def test_a_trigger_that_does_not_answer_is_named(
    trigger_client: None, trigger_router: respx.MockRouter, failure: httpx.Response | Exception
) -> None:
    route = trigger_router.post("/api/runs")
    if isinstance(failure, Exception):
        route.mock(side_effect=failure)
    else:
        route.mock(return_value=failure)

    with pytest.raises(RuntimeError, match="trigger_unavailable"):
        await trigger.start_run("explain-episode", "15510")
