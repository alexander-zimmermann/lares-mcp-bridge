"""One trace per bridge call, as Tempo would receive it: who called, and what the call did."""

from __future__ import annotations

import json

import httpx
import pytest
import respx
import structlog
from fastmcp import Client
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from lares_mcp_bridge import db, server, tracing
from lares_mcp_bridge.logging_setup import configure_logging

_TRACER = trace.get_tracer(__name__)


def _server_span(spans: InMemorySpanExporter, tool: str) -> ReadableSpan:
    """The bridge's span of the call; the test's own client opens a CLIENT span of that name."""
    (span,) = [
        s
        for s in spans.get_finished_spans()
        if s.name == f"tools/call {tool}" and s.kind is SpanKind.SERVER
    ]
    return span


def _under(spans: InMemorySpanExporter, parent: ReadableSpan) -> list[ReadableSpan]:
    assert parent.context is not None
    return [
        s
        for s in spans.get_finished_spans()
        if s.parent is not None and s.parent.span_id == parent.context.span_id
    ]


async def test_a_tool_call_names_the_person_and_the_client(
    clean_verdicts: None, clean_context: None, spans: InMemorySpanExporter
) -> None:
    """The link on the runs table finds a person's calls by `enduser.id`."""
    structlog.contextvars.bind_contextvars(**{"client_id": "claude-ai", "enduser.id": "kim"})
    async with Client(server.mcp) as client:
        await client.call_tool("list_runs", {"days": 3})

    span = _server_span(spans, "list_runs")
    assert span.attributes is not None
    assert span.attributes["enduser.id"] == "kim"
    assert span.attributes["lares.client_id"] == "claude-ai"
    assert span.attributes["gen_ai.tool.name"] == "list_runs"
    # The query runs under the call, with its placeholders and never their values.
    (query,) = [s for s in _under(spans, span) if s.kind is SpanKind.CLIENT]
    assert query.attributes is not None
    assert query.attributes["db.system"] == "postgresql"
    statement = str(query.attributes["db.statement"])
    assert "r.enduser_id = %s" in statement
    assert "kim" not in statement


async def test_a_caller_without_a_person_is_named_by_the_client_alone(
    clean_verdicts: None, clean_context: None, spans: InMemorySpanExporter
) -> None:
    structlog.contextvars.bind_contextvars(client_id="lares")
    async with Client(server.mcp) as client:
        await client.call_tool("list_runs", {})

    span = _server_span(spans, "list_runs")
    assert span.attributes is not None
    assert "enduser.id" not in span.attributes
    assert span.attributes["lares.client_id"] == "lares"


async def test_a_started_run_continues_the_call_that_asked_for_it(
    trigger_client: None,
    trigger_router: respx.MockRouter,
    clean_context: None,
    spans: InMemorySpanExporter,
) -> None:
    """`POST /api/runs` carries `traceparent`, so the run hangs under the bridge call."""
    trigger_router.post("/api/runs").mock(
        return_value=httpx.Response(
            202,
            json={"use_case": "propose-faults", "job_id": "b0b000000001", "status": "requested"},
        )
    )
    async with Client(server.mcp) as client:
        await client.call_tool("start_run", {"use_case": "propose-faults"})

    span = _server_span(spans, "start_run")
    assert span.context is not None
    traceparent = trigger_router.calls.last.request.headers["traceparent"]
    _, trace_id, parent_id, _ = traceparent.split("-")
    assert trace_id == f"{span.context.trace_id:032x}"
    # The request's own span sits under the call and is the run's parent.
    (request,) = [s for s in _under(spans, span) if s.kind is SpanKind.CLIENT]
    assert request.context is not None
    assert parent_id == f"{request.context.span_id:016x}"
    # It names the host, never the path: a wiki page or a backup id is a tool's argument.
    assert request.attributes is not None
    assert (
        request.attributes["http.url"] == "http://lares-agent-trigger.agents.svc.cluster.local:8080"
    )
    assert not any("/api/runs" in str(value) for value in request.attributes.values())


async def test_a_blocking_read_runs_inside_the_calls_trace(
    db_pool: None, spans: InMemorySpanExporter
) -> None:
    """The back-test reads on a worker thread; its queries still hang under the call."""
    with _TRACER.start_as_current_span("tools/call backtest_fault") as call:
        rows = await db.blocking_read(
            "backtest_fault", "backtest", lambda conn: conn.execute("SELECT 1 AS one").fetchall()
        )
    assert rows == [{"one": 1}]

    finished = [s for s in spans.get_finished_spans() if s.name == "tools/call backtest_fault"]
    (query,) = _under(spans, finished[0])
    assert query.kind is SpanKind.CLIENT
    assert query.context is not None
    assert query.context.trace_id == call.get_span_context().trace_id


async def test_the_readiness_probe_leaves_no_trace(
    db_pool: None, spans: InMemorySpanExporter
) -> None:
    """The probe runs every 10 s and is no bridge call."""
    assert await db.healthcheck()
    assert spans.get_finished_spans() == ()


def test_a_log_line_names_the_person_and_its_trace(
    clean_context: None, capsys: pytest.CaptureFixture[str]
) -> None:
    """Loki reads `enduser.id` as `enduser_id`, and Grafana jumps from a trace to its lines."""
    configure_logging("INFO", "json")
    log = structlog.get_logger("test")
    structlog.contextvars.bind_contextvars(**{"enduser.id": "kim"})
    with _TRACER.start_as_current_span("tools/call list_runs") as span:
        log.info("tool_invoked", tool="list_runs")
    log.info("outside")

    inside, outside = (json.loads(line) for line in capsys.readouterr().out.splitlines()[-2:])
    context = span.get_span_context()
    assert inside["enduser.id"] == "kim"
    assert inside["trace_id"] == f"{context.trace_id:032x}"
    assert inside["span_id"] == f"{context.span_id:016x}"
    assert "trace_id" not in outside


def test_the_provider_samples_new_traces_at_the_rendered_ratio() -> None:
    provider = tracing.build_provider("http://alloy-receiver:4318", 0.25)
    try:
        assert "TraceIdRatioBased{0.25}" in provider.sampler.get_description()
        assert provider.sampler.get_description().startswith("ParentBased")
        assert provider.resource.attributes["service.name"] == "lares-mcp-bridge"
    finally:
        provider.shutdown()


def test_the_exporter_posts_to_the_collectors_traces_path() -> None:
    assert tracing.traces_url("http://alloy-receiver:4318/") == (
        "http://alloy-receiver:4318/v1/traces"
    )
