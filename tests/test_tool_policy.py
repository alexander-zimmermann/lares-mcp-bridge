"""ClientToolPolicy: what each client sees and may call, keyed on the bound identity."""

from __future__ import annotations

import pytest
import structlog
from fastmcp import Client
from fastmcp.exceptions import ToolError

from lares_mcp_bridge import metrics as metrics_module
from lares_mcp_bridge import server
from lares_mcp_bridge.config import Settings

ALLOWLIST = {
    "lares-agent": ["list_*", "get_episode", "query_*", "backtest_fault"],
    "lares-runs": ["start_run"],
    "lares-memory": ["get_memory", "append_memory"],
    "lares-wiki": ["update_wiki_page"],
}


@pytest.fixture
def policy_settings(monkeypatch: pytest.MonkeyPatch) -> Settings:
    settings = Settings(
        db_host="localhost",
        db_name="x",
        db_username="x",
        db_password="x",
        nats_enabled=False,
        auth_client_tools=ALLOWLIST,
    )
    monkeypatch.setattr(server, "_settings", settings)
    return settings


async def _visible_tools(**identity: str) -> set[str]:
    # Bound before the client opens: the in-memory server task inherits the
    # context at spawn, exactly as a streamable-HTTP session does.
    structlog.contextvars.bind_contextvars(**identity)
    async with Client(server.mcp) as client:
        return {tool.name for tool in await client.list_tools()}


async def test_machine_client_sees_only_its_allowlist(
    policy_settings: Settings, clean_context: None
) -> None:
    everything = await _visible_tools()
    structlog.contextvars.clear_contextvars()
    visible = await _visible_tools(client_id="lares-agent", client_kind="machine")
    expected = {name for name in everything if name.startswith(("list_", "query_"))}
    assert visible == expected | {"get_episode", "backtest_fault"}
    # The verdict is the owner's act and matches no pattern an agent holds.
    assert "set_verdict" not in visible


async def test_machine_client_without_entry_sees_nothing(
    policy_settings: Settings, clean_context: None
) -> None:
    assert await _visible_tools(client_id="stranger", client_kind="machine") == set()


async def test_user_client_without_entry_keeps_every_tool(
    policy_settings: Settings, clean_context: None
) -> None:
    visible = await _visible_tools(client_id="lares-mcp-bridge", client_kind="user")
    assert "set_verdict" in visible
    assert "list_episodes" in visible


async def test_anonymous_keeps_every_tool(policy_settings: Settings, clean_context: None) -> None:
    visible = await _visible_tools()
    assert "set_verdict" in visible


async def test_denied_call_is_refused_and_counted(
    policy_settings: Settings, clean_context: None
) -> None:
    metrics_module.reset()
    structlog.contextvars.bind_contextvars(
        sub="lares-agent", client_id="lares-agent", client_kind="machine"
    )
    async with Client(server.mcp) as client:
        with pytest.raises(ToolError, match="tool_not_allowed: set_verdict"):
            await client.call_tool(
                "set_verdict", {"target": "episode", "episode_id": 1, "verdict": "real"}
            )
    denied = metrics_module.get().registry.get_sample_value(
        "lares_mcp_bridge_tool_calls_total",
        {"tool": "set_verdict", "sub": "lares-agent", "outcome": "denied"},
    )
    assert denied == 1


async def test_starting_a_run_is_a_client_of_its_own(
    policy_settings: Settings, clean_context: None
) -> None:
    """The chat holds start_run; the read client every event and cron run uses does not."""
    assert await _visible_tools(client_id="lares-runs", client_kind="machine") == {"start_run"}
    structlog.contextvars.clear_contextvars()
    assert "start_run" not in await _visible_tools(client_id="lares-agent", client_kind="machine")


async def test_memory_is_a_client_of_its_own(
    policy_settings: Settings, clean_context: None
) -> None:
    """Only the runs that keep a memory hold it; the read client every run uses does not."""
    assert await _visible_tools(client_id="lares-memory", client_kind="machine") == {
        "get_memory",
        "append_memory",
    }
    structlog.contextvars.clear_contextvars()
    read = await _visible_tools(client_id="lares-agent", client_kind="machine")
    assert {"get_memory", "append_memory"}.isdisjoint(read)


async def test_writing_the_wiki_is_a_client_of_its_own(
    policy_settings: Settings, clean_context: None
) -> None:
    """Only the runs that write the wiki hold the write; the read client every run uses does not."""
    assert await _visible_tools(client_id="lares-wiki", client_kind="machine") == {
        "update_wiki_page"
    }
    structlog.contextvars.clear_contextvars()
    read = await _visible_tools(client_id="lares-agent", client_kind="machine")
    assert "update_wiki_page" not in read
    # The backup and storage reads are reads: the read client holds them through `list_*`.
    assert {
        "list_pbs_datastores",
        "list_pbs_snapshots",
        "list_pbs_tasks",
        "list_s3_buckets",
        "list_s3_objects",
    } <= read
