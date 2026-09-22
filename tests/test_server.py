"""App-factory tests: probe endpoints, OAuth discovery, MCP tool registration.

``build_app()`` is exercised with the real seeded TimescaleDB container behind
it (NATS disabled, auth disabled); the MCP layer is driven in-process via the
fastmcp ``Client`` so the ``@mcp.tool`` wrappers and their metrics
instrumentation run for real.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError
from starlette.testclient import TestClient
from testcontainers.postgres import PostgresContainer

from lares_mcp_bridge import metrics as metrics_module
from lares_mcp_bridge import server

EXPECTED_TOOLS = {
    "list_data_sources",
    "get_schema",
    "query_timeseries",
    "query_energy_flow",
    "query_heating_cycles",
    "query_room_climate",
    "query_knx_events",
    "query_unifi_events",
    "correlate_events",
    "get_forecast",
    "get_pv_forecast",
    "get_weather_forecast",
    "get_current_state",
    "subscribe_nats",
    "get_current_knx",
    "list_episodes",
    "set_episode_verdict",
    "search_wiki",
    "get_wiki_page",
    "list_wiki_pages",
}


@pytest.fixture
def app_env(timescaledb_container: PostgresContainer, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MCP_DB_HOST", timescaledb_container.get_container_host_ip())
    monkeypatch.setenv("MCP_DB_PORT", str(timescaledb_container.get_exposed_port(5432)))
    monkeypatch.setenv("MCP_DB_NAME", "homelab")
    monkeypatch.setenv("MCP_DB_USERNAME", "test")
    monkeypatch.setenv("MCP_DB_PASSWORD", "test")
    monkeypatch.setenv("MCP_AUTH_ENABLED", "false")
    monkeypatch.setenv("MCP_NATS_ENABLED", "false")
    monkeypatch.setenv("MCP_METRICS_PORT", "0")  # ephemeral port, avoids clashes


def test_health_and_discovery_endpoints(app_env: None) -> None:
    app = server.build_app()
    with TestClient(app) as client:
        livez = client.get("/livez")
        assert livez.status_code == 200
        assert livez.json() == {"status": "alive"}

        healthz = client.get("/healthz")
        assert healthz.status_code == 200
        body = healthz.json()
        assert body["status"] == "ok"
        assert body["db"] is True
        assert "nats" not in body  # disabled → not reported

        meta = client.get("/.well-known/oauth-protected-resource")
        assert meta.status_code == 200
        assert "authorization_servers" in meta.json()


def test_healthz_has_no_wiki_dependency(
    app_env: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Wiki configured: readiness stays DB-gated and never reports or contacts the wiki."""
    token_file = tmp_path / "wikijs-token"
    token_file.write_text("not-a-real-key\n", encoding="utf-8")
    monkeypatch.setenv("MCP_WIKIJS_URL", "http://127.0.0.1:9")  # never contacted
    monkeypatch.setenv("MCP_WIKIJS_TOKEN_FILE", str(token_file))

    app = server.build_app()
    with TestClient(app) as client:
        healthz = client.get("/healthz")
        assert healthz.status_code == 200
        assert healthz.json()["status"] == "ok"
        assert "wiki" not in healthz.json()


async def test_all_tools_registered() -> None:
    async with Client(server.mcp) as client:
        tools = await client.list_tools()
    assert {t.name for t in tools} == EXPECTED_TOOLS


async def test_tool_call_roundtrip(db_pool: None) -> None:
    """list_data_sources through the MCP layer returns the seeded hypertables."""
    async with Client(server.mcp) as client:
        result = await client.call_tool("list_data_sources", {})
    names = {entry["name"] for entry in result.data}
    assert {"knx", "ems_esp"} <= names


async def test_literal_parameters_reach_the_client_as_enums() -> None:
    """The LLM sees the allowed values instead of guessing at a free string."""
    async with Client(server.mcp) as client:
        by_name = {t.name: t for t in await client.list_tools()}
    aggregation = by_name["query_timeseries"].input_schema["properties"]["aggregation"]
    assert aggregation["enum"] == ["avg", "sum", "min", "max", "count"]
    state = by_name["list_episodes"].input_schema["properties"]["state"]
    assert state["enum"] == ["all", "open", "ended"]


async def test_middleware_counts_every_tool_outcome(db_pool: None) -> None:
    """One middleware, not one wrapper per tool, records ok and error per tool and subject."""
    metrics_module.reset()
    async with Client(server.mcp) as client:
        await client.call_tool("list_data_sources", {})
        with pytest.raises(ToolError, match="unknown_table"):
            await client.call_tool("get_schema", {"table": "does_not_exist"})

    def count(tool: str, outcome: str) -> float | None:
        return metrics_module.get().registry.get_sample_value(
            "lares_mcp_bridge_tool_calls_total",
            {"tool": tool, "sub": "anonymous", "outcome": outcome},
        )

    assert count("list_data_sources", "ok") == 1
    assert count("list_data_sources", "error") is None
    assert count("get_schema", "error") == 1
    assert count("get_schema", "ok") is None
