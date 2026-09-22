"""ClientToolPolicy: what each client sees and may call, keyed on the bound identity."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import structlog
from fastmcp import Client
from fastmcp.exceptions import ToolError

from lares_mcp_bridge import metrics as metrics_module
from lares_mcp_bridge import server
from lares_mcp_bridge.config import Settings

ALLOWLIST = {"lares-agent": ["list_episodes", "query_*"]}


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


@pytest.fixture
async def clean_context() -> AsyncIterator[None]:
    structlog.contextvars.clear_contextvars()
    try:
        yield
    finally:
        structlog.contextvars.clear_contextvars()


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
    expected = {"list_episodes"} | {name for name in everything if name.startswith("query_")}
    assert visible == expected
    assert "set_episode_verdict" not in visible


async def test_machine_client_without_entry_sees_nothing(
    policy_settings: Settings, clean_context: None
) -> None:
    assert await _visible_tools(client_id="stranger", client_kind="machine") == set()


async def test_user_client_without_entry_keeps_every_tool(
    policy_settings: Settings, clean_context: None
) -> None:
    visible = await _visible_tools(client_id="lares-mcp-bridge", client_kind="user")
    assert "set_episode_verdict" in visible
    assert "list_episodes" in visible


async def test_anonymous_keeps_every_tool(policy_settings: Settings, clean_context: None) -> None:
    visible = await _visible_tools()
    assert "set_episode_verdict" in visible


async def test_denied_call_is_refused_and_counted(
    policy_settings: Settings, clean_context: None
) -> None:
    metrics_module.reset()
    structlog.contextvars.bind_contextvars(
        sub="lares-agent", client_id="lares-agent", client_kind="machine"
    )
    async with Client(server.mcp) as client:
        with pytest.raises(ToolError, match="tool_not_allowed: set_episode_verdict"):
            await client.call_tool("set_episode_verdict", {"episode_id": 1, "verdict": "real"})
    denied = metrics_module.get().registry.get_sample_value(
        "lares_mcp_bridge_tool_calls_total",
        {"tool": "set_episode_verdict", "sub": "lares-agent", "outcome": "denied"},
    )
    assert denied == 1
