"""Use-case memory through the MCP layer: read from the table, appended through the trigger.

The read runs against the seeded container. The append is carried to the
trigger's `POST /api/memory`, here a respx mock: the trigger alone writes the
memory and keeps it to its bound.
"""

from __future__ import annotations

import json

import httpx
import pytest
import respx
from fastmcp import Client
from fastmcp.exceptions import ToolError

from lares_mcp_bridge import server

from .support import TRIGGER_KEY


async def test_get_memory_reads_the_use_cases_notes(db_pool: None) -> None:
    async with Client(server.mcp) as client:
        result = await client.call_tool("get_memory", {"use_case": "messenger"})

    assert result.data["use_case"] == "messenger"
    assert result.data["text"] == "Der Besitzer fragt meist nach der Wallbox."
    assert result.data["bytes"] == 42
    assert result.data["updated_at"] is not None


async def test_a_use_case_that_wrote_nothing_reads_as_empty(db_pool: None) -> None:
    async with Client(server.mcp) as client:
        result = await client.call_tool("get_memory", {"use_case": "propose-faults"})

    assert result.data == {
        "use_case": "propose-faults",
        "text": "",
        "bytes": 0,
        "updated_at": None,
    }


async def test_append_memory_is_carried_to_the_trigger_with_the_key(
    trigger_client: None, trigger_router: respx.MockRouter
) -> None:
    trigger_router.post("/api/memory").mock(
        return_value=httpx.Response(200, json={"use_case": "propose-faults", "bytes": 61})
    )
    line = "2026-10-04 rejected: silence gap_factor 4 on Stromwert (#2240)"

    async with Client(server.mcp) as client:
        result = await client.call_tool(
            "append_memory", {"use_case": "propose-faults", "line": line}
        )

    assert result.data == {"use_case": "propose-faults", "bytes": 61}
    request = trigger_router.calls.last.request
    assert request.headers["authorization"] == f"Bearer {TRIGGER_KEY}"
    assert json.loads(request.content) == {"use_case": "propose-faults", "text": line}


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (404, "no use case named propose-fault"),
        (409, "explain-episode keeps no memory"),
        (400, "text: 9000 bytes, more than the 8192 a memory holds"),
    ],
)
async def test_a_refused_line_carries_the_triggers_reason(
    trigger_client: None, trigger_router: respx.MockRouter, status: int, reason: str
) -> None:
    trigger_router.post("/api/memory").mock(
        return_value=httpx.Response(status, json={"error": reason})
    )

    async with Client(server.mcp) as client:
        result = await client.call_tool("append_memory", {"use_case": "x", "line": "a line"})
    assert result.data == {"refused": f"memory_not_appended: {reason}"}


async def test_a_trigger_that_does_not_answer_is_named(
    trigger_client: None, trigger_router: respx.MockRouter
) -> None:
    trigger_router.post("/api/memory").mock(side_effect=httpx.ConnectError("connection refused"))

    async with Client(server.mcp) as client:
        with pytest.raises(ToolError, match="trigger_unavailable"):
            await client.call_tool("append_memory", {"use_case": "x", "line": "a line"})


async def test_a_line_break_is_refused_before_the_trigger_is_asked(
    trigger_client: None, trigger_router: respx.MockRouter
) -> None:
    """The trigger cuts the oldest lines whole; two lines sent as one could lose half of it."""
    route = trigger_router.post("/api/memory")

    async with Client(server.mcp) as client:
        result = await client.call_tool(
            "append_memory", {"use_case": "propose-faults", "line": "rejected\nbecause"}
        )
    assert result.data["refused"].startswith("invalid_line")
    assert not route.called
