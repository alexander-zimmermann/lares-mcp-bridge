"""App-factory tests: probe endpoints, the gateway's sign-in, MCP tool registration.

``build_app()`` is exercised with the real seeded TimescaleDB container behind
it (NATS disabled); the MCP layer is driven in-process via the fastmcp
``Client`` so the ``@mcp.tool`` wrappers and their metrics instrumentation run
for real.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import structlog
from fastmcp import Client
from fastmcp.exceptions import ToolError
from starlette.testclient import TestClient
from testcontainers.postgres import PostgresContainer

from lares_mcp_bridge import metrics as metrics_module
from lares_mcp_bridge import server
from lares_mcp_bridge.tools import sources as sources_tools

from .support import GATEWAY_KEY

EXPECTED_TOOLS = {
    "list_data_sources",
    "get_schema",
    "query_timeseries",
    "query_energy_flow",
    "query_heating_cycles",
    "query_room_climate",
    "query_knx_events",
    "query_unifi_events",
    "query_presence",
    "correlate_events",
    "get_forecast",
    "get_pv_forecast",
    "get_weather_forecast",
    "get_current_state",
    "subscribe_nats",
    "get_current_knx",
    "list_episodes",
    "get_episode",
    "list_runs",
    "set_verdict",
    "start_run",
    "backtest_fault",
    "get_memory",
    "append_memory",
    "search_wiki",
    "get_wiki_page",
    "list_wiki_pages",
    "update_wiki_page",
    "list_pbs_datastores",
    "list_pbs_snapshots",
    "list_pbs_tasks",
    "list_s3_buckets",
    "list_s3_objects",
}


@pytest.fixture
def app_env(timescaledb_container: PostgresContainer, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MCP_DB_HOST", timescaledb_container.get_container_host_ip())
    monkeypatch.setenv("MCP_DB_PORT", str(timescaledb_container.get_exposed_port(5432)))
    monkeypatch.setenv("MCP_DB_NAME", "homelab")
    monkeypatch.setenv("MCP_DB_USERNAME", "test")
    monkeypatch.setenv("MCP_DB_PASSWORD", "test")
    monkeypatch.setenv("MCP_GATEWAY_KEY", GATEWAY_KEY)
    monkeypatch.setenv("MCP_NATS_ENABLED", "false")
    monkeypatch.setenv("MCP_METRICS_PORT", "0")  # ephemeral port, avoids clashes


def test_the_probes_answer_and_everything_else_needs_the_gateway(app_env: None) -> None:
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

        # No discovery: the gateway runs the sign-in, the bridge only checks its key.
        assert client.get("/.well-known/oauth-protected-resource").status_code == 401
        assert client.post("/mcp", json={}).status_code == 401


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
    names = {entry["name"] for entry in result.data["sources"]}
    assert {"knx", "ems_esp"} <= names


async def test_episode_bundle_and_ledger_survive_the_mcp_layer(clean_verdicts: None) -> None:
    """Through the real client, not the function: the bundle and the ledger
    carry a NUMERIC cost, an INTERVAL duration and a text array, and every one
    of them has to reach the caller as a plain JSON value."""
    async with Client(server.mcp) as client:
        listed = await client.call_tool("list_episodes", {"days": 365})
        episode_id = listed.data["episodes"][0]["episode_id"]

        bundle = await client.call_tool("get_episode", {"episode_id": episode_id})
        assert bundle.data["episode"]["episode_id"] == episode_id
        assert isinstance(bundle.data["observations"], list)

        runs = await client.call_tool("list_runs", {"days": 30})
        newest = runs.data["runs"][0]
        assert isinstance(newest["cost"], float)
        assert isinstance(newest["duration_seconds"], float)
        assert isinstance(newest["output_ref"], list)

        judged = await client.call_tool(
            "set_verdict", {"target": "run", "verdict": "helpful", "run_id": newest["run_id"]}
        )
        assert judged.data["verdict"] == "helpful"

        refused = await client.call_tool("get_episode", {"episode_id": 999999})
        assert json.loads(refused.content[0].text)["refused"].startswith("unknown_episode")


async def test_literal_parameters_reach_the_client_as_enums() -> None:
    """The LLM sees the allowed values instead of guessing at a free string."""
    async with Client(server.mcp) as client:
        by_name = {t.name: t for t in await client.list_tools()}
    aggregation = by_name["query_timeseries"].input_schema["properties"]["aggregation"]
    assert aggregation["enum"] == ["avg", "sum", "min", "max", "count"]
    state = by_name["list_episodes"].input_schema["properties"]["state"]
    assert state["enum"] == ["all", "open", "ended"]
    target = by_name["set_verdict"].input_schema["properties"]["target"]
    assert target["enum"] == ["episode", "run"]
    verdict = by_name["set_verdict"].input_schema["properties"]["verdict"]
    assert verdict["enum"] == ["real", "nonsense", "helpful", "useless"]


async def test_middleware_counts_every_tool_outcome(
    db_pool: None, monkeypatch: pytest.MonkeyPatch, clean_context: None
) -> None:
    """One middleware, not one wrapper per tool, counts each outcome per client and tool."""
    metrics_module.reset()
    structlog.contextvars.bind_contextvars(client_id="lares")

    async def database_gone() -> list[dict[str, Any]]:
        raise RuntimeError("database gone")

    async with Client(server.mcp) as client:
        await client.call_tool("list_data_sources", {})
        refused = await client.call_tool("get_schema", {"table": "does_not_exist"})
        assert json.loads(refused.content[0].text) == {"refused": "unknown_table: does_not_exist"}
        monkeypatch.setattr(sources_tools, "list_data_sources", database_gone)
        with pytest.raises(ToolError):
            await client.call_tool("list_data_sources", {})

    def count(tool: str, outcome: str) -> float | None:
        return metrics_module.get().registry.get_sample_value(
            "lares_mcp_bridge_tool_calls_total",
            {"tool": tool, "client_id": "lares", "outcome": outcome},
        )

    assert count("list_data_sources", "ok") == 1
    assert count("list_data_sources", "error") == 1
    assert count("get_schema", "refused") == 1
    assert count("get_schema", "error") is None
    assert count("get_schema", "ok") is None


async def test_every_tool_answers_with_an_object() -> None:
    """A refusal is an object; every tool's output schema has to admit it.

    A tool returning a list gets a wrapped schema the client validates, and
    a refusal could not pass it.
    """
    async with Client(server.mcp) as client:
        tools = await client.list_tools()
    assert tools
    for tool in tools:
        schema = tool.output_schema or {}
        assert schema.get("type") == "object", tool.name
        assert "x-fastmcp-wrap-result" not in schema, tool.name


async def test_refusal_travels_as_a_result(monkeypatch: pytest.MonkeyPatch) -> None:
    """A ValueError is the tool working as designed: the client gets ``refused``, no MCP error.

    An error result is a breaker strike for the harness; three guessed column
    names in one batch would pause the whole server for a minute.
    """
    metrics_module.reset()

    async def refuse(table: str) -> dict[str, Any]:
        raise ValueError("unknown_columns: ['x']; valid: ['time', 'value']")

    monkeypatch.setattr(sources_tools, "get_schema", refuse)
    async with Client(server.mcp) as client:
        result = await client.call_tool("get_schema", {"table": "knx"})
    assert result.is_error is False
    refusal = {"refused": "unknown_columns: ['x']; valid: ['time', 'value']"}
    assert json.loads(result.content[0].text) == refusal
    assert result.data == refusal
