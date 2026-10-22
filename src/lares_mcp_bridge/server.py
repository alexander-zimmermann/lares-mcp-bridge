"""MCP server wiring: tool registration, HTTP app factory, and lifespan.

Docstring convention: the ``@mcp.tool()`` docstrings in this module ARE the
tool descriptions served to the LLM over MCP — they are the API contract and
must stay accurate and self-contained. The docstrings on the implementations
in ``tools/*`` document internals for developers and must not be relied on
by clients.

Each tool here is its LLM-facing docstring plus the call into ``tools/*``;
logging and the per-tool outcome metric are one middleware, not one wrapper
per tool. Which tools a caller sees is the gateway's business: each of its
routes serves its own list. A tool refuses bad arguments with
``ValueError``; the outcome middleware hands that back as a result,
``{"refused": "<reason>"}``, never as an MCP error — see
:class:`ToolTelemetry` for why.

The app is exposed via :func:`build_app` (uvicorn factory) so importing this
module has no side effects — settings are only loaded when the app is built.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import structlog
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp.tools.base import ToolResult
from mcp.types import CallToolRequestParams
from opentelemetry import trace
from starlette.applications import Starlette
from starlette.middleware import Middleware as StarletteMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from . import __version__, db, tracing
from . import metrics as metrics_module
from . import nats as nats_module
from .auth import GatewayAuth
from .config import Settings, load_settings
from .logging_setup import configure_logging, get_logger
from .tools import backtest as backtest_tools
from .tools import domain as domain_tools
from .tools import episodes as episode_tools
from .tools import forecasts as forecasts_tools
from .tools import live as live_tools
from .tools import memory as memory_tools
from .tools import pbs as pbs_tools
from .tools import presence as presence_tools
from .tools import runs as run_tools
from .tools import s3 as s3_tools
from .tools import sources as sources_tools
from .tools import timeseries as timeseries_tools
from .tools import trigger as trigger_tools
from .tools import verdicts as verdict_tools
from .tools import wiki as wiki_tools
from .tools.episodes import EpisodeState
from .tools.runs import RunStatus, SubjectKind
from .tools.timeseries import Aggregation
from .tools.verdicts import Verdict, VerdictTarget

log = get_logger(__name__)


def _caller_client() -> str | None:
    """The route the gateway named for the call, as GatewayAuth bound it."""
    client_id = structlog.contextvars.get_contextvars().get("client_id")
    return str(client_id) if client_id else None


def _record_tool_call(tool: str, outcome: str) -> None:
    client_id = _caller_client() or "anonymous"
    metrics_module.get().tool_calls.labels(tool=tool, client_id=client_id, outcome=outcome).inc()


def _caller_enduser_id() -> str | None:
    """The person the gateway named for the call, or None for a machine."""
    enduser_id = structlog.contextvars.get_contextvars().get("enduser.id")
    return str(enduser_id) if enduser_id else None


class CallerOnSpan(Middleware):
    """Name the caller on FastMCP's server span: ``enduser.id`` and ``lares.client_id``.

    FastMCP fills ``enduser.id`` only from its own auth, which the bridge does
    not use, so the person the gateway named goes on here; the span sits
    under the gateway's span of the call.
    """

    async def on_request(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        span = trace.get_current_span()
        enduser_id = _caller_enduser_id()
        client_id = _caller_client()
        if enduser_id is not None:
            span.set_attribute("enduser.id", enduser_id)
        if client_id is not None:
            span.set_attribute("lares.client_id", client_id)
        return await call_next(context)


class ToolTelemetry(Middleware):
    """Log every tool call, count its outcome, and hand a refusal back as a result.

    Three outcomes: ``ok``, ``refused`` and ``error``. A refusal is the tool
    rejecting its arguments with ``ValueError`` — an unknown table, column or
    room, an invalid bucket, a result over the row cap — and naming what
    would have been valid. It is the tool working as designed, so it travels
    as an ordinary result, ``{"refused": "<reason>"}``, not as an MCP error:
    a harness counts error results as strikes against the server, and three
    guessed column names in one batch paused the whole server for a minute
    (Hermes, threshold 3, cooldown 60 s, 2026-10-10). The bridge's own
    failures — the database, NATS, a timeout — stay errors, which is what a
    breaker is for.
    """

    async def on_call_tool(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        tool = context.message.name
        log.info("tool_invoked", tool=tool, arguments=context.message.arguments)
        try:
            result = await call_next(context)
        except ToolError as exc:
            # FastMCP wraps whatever a tool raised: the refusal is the cause.
            cause = exc.__cause__
            if not isinstance(cause, ValueError):
                _record_tool_call(tool, "error")
                raise
            _record_tool_call(tool, "refused")
            log.info("tool_refused", tool=tool, reason=str(cause))
            refusal = {"refused": str(cause)}
            # Every tool answers with an object (a test holds the line), so
            # the refusal satisfies every tool's output schema as well.
            return ToolResult(content=json.dumps(refusal), structured_content=refusal)
        except Exception:
            _record_tool_call(tool, "error")
            raise
        _record_tool_call(tool, "ok")
        return result


mcp: FastMCP = FastMCP(
    "lares-mcp-bridge",
    version=__version__,
    middleware=[CallerOnSpan(), ToolTelemetry()],
)


@mcp.tool()
async def list_data_sources() -> dict[str, Any]:
    """List hypertables and continuous aggregates with their time range.

    Use this first to discover what data is available before calling
    ``get_schema`` or ``query_timeseries``. ``sources`` holds one entry per
    table; ``time_range.min`` is where the stored data starts, to the day for
    KNX; ``time_range.max`` is the newest value.
    """
    # An object like every other tool, so a refusal fits every tool's schema.
    return {"sources": await sources_tools.list_data_sources()}


@mcp.tool()
async def get_schema(table: str) -> dict[str, Any]:
    """Describe a data source: columns, types, and JSONB key sample.

    For tables with a ``raw JSONB`` payload, returns the most common JSON keys
    observed in the latest 1000 rows so the LLM can construct
    ``raw->>'<key>'`` expressions.
    """
    return await sources_tools.get_schema(table)


@mcp.tool()
async def query_timeseries(
    table: str,
    columns: list[str],
    from_ts: str,
    to_ts: str,
    aggregation: Aggregation = "avg",
    bucket: str = "1 hour",
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Aggregated time-series query.

    - ``aggregation``: ``avg`` | ``sum`` | ``min`` | ``max`` | ``count``.
    - ``bucket``: Postgres interval literal, e.g. ``'15 minutes'``, ``'1 hour'``,
      ``'1 day'``.
    - When the bucket is at least one hour and a ``<table>_1h`` continuous
      aggregate exists, the query is routed to the aggregate automatically.
    - Result row count is capped (default 5000); exceed → error suggesting a
      coarser bucket.
    """
    return await timeseries_tools.query_timeseries(
        table=table,
        columns=columns,
        from_ts=from_ts,
        to_ts=to_ts,
        aggregation=aggregation,
        bucket=bucket,
        filters=filters,
    )


@mcp.tool()
async def query_energy_flow(
    from_ts: str,
    to_ts: str,
    bucket: str = "1 hour",
) -> dict[str, Any]:
    """Joined PV / grid / consumer / battery / wallbox flow per bucket.

    Pulls from the hourly continuous aggregates ``solaredge_powerflow_1h`` and
    ``warp_meter_1h``. Bucket must be ``1 hour`` or coarser.
    """
    return await domain_tools.query_energy_flow(from_ts=from_ts, to_ts=to_ts, bucket=bucket)


@mcp.tool()
async def query_heating_cycles(
    from_ts: str,
    to_ts: str,
    min_duration_seconds: int = 60,
) -> dict[str, Any]:
    """Detect ON/OFF burner cycles from boiler telemetry.

    A cycle is a contiguous run with ``curburnpow > 0`` over ``ems_esp``
    rows (``topic = 'boiler_data'``). Returns one row per cycle with start,
    end, duration, peak and average burner power.
    """
    return await domain_tools.query_heating_cycles(
        from_ts=from_ts,
        to_ts=to_ts,
        min_duration_seconds=min_duration_seconds,
    )


@mcp.tool()
async def query_room_climate(
    room: str,
    from_ts: str,
    to_ts: str,
    bucket: str = "1 hour",
    functions: list[str] | None = None,
) -> dict[str, Any]:
    """Bucketed average reading per GA name within a room.

    ``room`` is validated against the distinct rooms in ``ga_catalog`` so
    unknown rooms produce a precise error the LLM can self-correct against.
    Optional ``functions`` narrows the result to GAs whose ETS function name
    is in the given list.
    """
    return await domain_tools.query_room_climate(
        room=room,
        from_ts=from_ts,
        to_ts=to_ts,
        bucket=bucket,
        functions=functions,
    )


@mcp.tool()
async def query_knx_events(
    from_ts: str,
    to_ts: str,
    room: str | None = None,
    ga: str | None = None,
    name: str | None = None,
    functions: list[str] | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    """Raw KNX event log against ``ga_catalog_view``.

    All predicates AND-combined; pass none for an unfiltered window.

    * ``room``      — exact match (e.g. ``"Wohnzimmer"``)
    * ``ga``        — exact GA match (``"4/2/161"``)
    * ``name``      — substring match against ``ga_name`` (``ILIKE``);
                      pass partial words like ``"Beleuchtung"``
    * ``functions`` — list of ETS function names, validated against the
                      catalog (unknown names error with the valid list);
                      combine with ``room`` to narrow to e.g. all lighting
                      events in a room

    Default ``limit`` 200; effective cap ``min(limit, query_row_limit)``.
    When more rows match, the newest ``limit`` rows are returned with
    ``truncated: true`` — narrow the filters or shorten the window for
    the rest.
    """
    return await domain_tools.query_knx_events(
        from_ts=from_ts,
        to_ts=to_ts,
        room=room,
        ga=ga,
        name=name,
        functions=functions,
        limit=limit,
    )


@mcp.tool()
async def query_unifi_events(
    from_ts: str,
    to_ts: str,
    camera: str | None = None,
    detection_type: str | None = None,
    event_type: str | None = None,
    min_score: int | None = None,
    event_id: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Recent UniFi Protect Alarm Manager events for security review.

    All predicates AND-combined; pass none for an unfiltered window.

    * ``camera``         — exact match (``"fassade"``, ``"eingang"``,
                           ``"terrasse_wohnzimmer"``, ``"terrasse_esszimmer"``)
    * ``detection_type`` — trigger ``key`` (e.g. ``"person"``, ``"motion"``,
                           ``"line_crossed"``, ``"face_known"``, ``"vehicle"``,
                           ``"license_plate_known"``, ``"audio_alarm_siren"``)
    * ``event_type``     — UniFi source type (``"smartDetectZone"`` /
                           ``"smartDetectLine"`` / ``"motion"``)
    * ``min_score``      — confidence floor 0..100
    * ``event_id``       — exact UUID; use to fetch details for one alarm

    Default ``limit`` 20; effective cap ``min(limit, query_row_limit)``.
    When more rows match, the newest ``limit`` rows are returned with
    ``truncated: true`` — narrow the filters or shorten the window for
    the rest.
    """
    return await domain_tools.query_unifi_events(
        from_ts=from_ts,
        to_ts=to_ts,
        camera=camera,
        detection_type=detection_type,
        event_type=event_type,
        min_score=min_score,
        event_id=event_id,
        limit=limit,
    )


@mcp.tool()
async def query_presence(from_ts: str, to_ts: str) -> dict[str, Any]:
    """Who was at home, and when the house was empty — a few lines, not raw events.

    Use this whenever an answer turns on whether somebody was in the house:
    an appliance drawing power, a door or window opened, a light left on.
    It reads each person's presence (``Person.<name>.Präsenz``), the state it
    had when the window opened included.

    Returns ``persons`` — per name, spans ``{"from", "to", "state"}`` covering
    the whole window, ``state`` one of ``"home"``, ``"away"`` or
    ``"unknown"`` (no state known yet) — ``house_empty``: the spans in which
    every listed person was known to be away, and ``silent``: the people whose
    presence reported nothing for a month, about whom nothing is known — an
    empty house holds only for the listed ones. The window may span at most
    31 days.
    """
    return await presence_tools.query_presence(from_ts=from_ts, to_ts=to_ts)


@mcp.tool()
async def correlate_events(
    source_a: dict[str, Any],
    source_b: dict[str, Any],
    from_ts: str,
    to_ts: str,
    window: str = "15 minutes",
    bucket: str = "1 minute",
) -> dict[str, Any]:
    """Lagged Pearson correlation between two time-series streams.

    Each ``source`` is ``{"table": str, "column": str}``. Returns ``best`` and
    ``top`` (up to 10) lags by ``|corr|``.
    """
    return await domain_tools.correlate_events(
        source_a=source_a,
        source_b=source_b,
        from_ts=from_ts,
        to_ts=to_ts,
        window=window,
        bucket=bucket,
    )


@mcp.tool()
async def get_forecast(
    metric: str,
    horizon_hours: int = 24,
    model: str | None = None,
) -> dict[str, Any]:
    """Stored model forecasts for ``metric`` looking ``horizon_hours`` ahead.

    Rows are produced by the ``forecast-solar`` (PV) and ``score-seasonal``
    (statsforecast) batch jobs. An empty list means no stored forecast
    covers the requested metric/window. Result row count is capped; exceed →
    error suggesting a shorter horizon.
    """
    return await forecasts_tools.get_forecast(
        metric=metric,
        horizon_hours=horizon_hours,
        model=model,
    )


@mcp.tool()
async def get_pv_forecast(hours: int = 48) -> dict[str, Any]:
    """Hour-by-hour PV production forecast (watts) for the next ``hours``.

    Sourced from forecast.solar (already weather-adjusted) via the
    ``forecast-solar`` batch job — no live API call. A ``note`` field means
    the job hasn't populated the requested window yet. Capped like
    ``get_forecast``.
    """
    return await forecasts_tools.get_pv_forecast(hours=hours)


@mcp.tool()
async def get_weather_forecast(hours: int = 48) -> dict[str, Any]:
    """Hour-by-hour weather forecast for the next ``hours``, each hour a dict
    of metrics (temperature °C, cloud_cover %, precipitation mm,
    solar_radiation W/m², wind_speed km/h).

    Sourced from Open-Meteo (DWD ICON) via the ``forecast-weather`` batch job
    — no live API call. A ``note`` field means the job hasn't populated the
    requested window yet. Capped like ``get_forecast``.
    """
    return await forecasts_tools.get_weather_forecast(hours=hours)


@mcp.tool()
async def list_episodes(
    state: EpisodeState = "all",
    episode_id: int | None = None,
    fault: str | None = None,
    days: int = 7,
    only_unjudged: bool = False,
    only_judged: bool = False,
    limit: int = 100,
) -> dict[str, Any]:
    """Situations the detection chain recorded, newest first — the review list.

    Repeated observations of one fault fold into one episode, and this is the
    list the Basalte mails are the reminder to go through. Each row carries
    the verdict it already has, so ``only_unjudged=True`` is "what still needs
    judging" and ``only_judged=True`` is what a verdict count reads — the two
    exclude each other.

    * ``episode_id`` — read one episode back by id, whatever its age
    * ``state`` — ``"all"`` | ``"open"`` (still running) | ``"ended"``
    * ``fault`` — exact fault name (e.g. ``"silence"``, ``"constancy"``)
    * ``days``  — window in days, by overlap: an episode that started weeks
                  ago and is still open is included in a short window
    * ``severity`` in the result is the delivery contract 1 (info) / 2
      (warning) / 3 (critical); ``affected`` and ``room`` resolve the group
      address in the subject against the KNX catalog.
    * ``explanation_tldr`` and ``explanation_run_id`` are the newest thing the
      agent platform said about that episode; the run id is what a verdict on
      the explanation is given on.

    For the whole evidence of one episode, call ``get_episode`` instead.
    """
    return await episode_tools.list_episodes(
        state=state,
        episode_id=episode_id,
        fault=fault,
        days=days,
        only_unjudged=only_unjudged,
        only_judged=only_judged,
        limit=limit,
    )


@mcp.tool()
async def get_episode(episode_id: int) -> dict[str, Any]:
    """Everything one episode is, in one call — start here when explaining one.

    Returns ``episode`` (the row as ``list_episodes`` gives it, plus
    ``channel_ga``), ``events`` (appeared / escalated / ended with their time
    and severity), ``observations`` (the score curve, oldest first, the most
    recent 48), ``channel`` (the KNX catalog entry of the group address in
    the subject, or ``null``), ``siblings`` (the other catalog channels of the
    same room that report a state or a reading — commands left out — the
    channel's own device first, at most 50) and ``explanation`` (the newest
    explanation of this episode: ``run_id``, ``tldr``, ``text``,
    ``created_at``, or ``null``).

    An unknown id errors with the id in the message. ``observations_truncated``
    and ``siblings_truncated`` say when a list was cut to its cap.
    """
    return await episode_tools.get_episode(episode_id=episode_id)


@mcp.tool()
async def list_runs(
    run_id: int | None = None,
    use_case: str | None = None,
    subject_kind: SubjectKind | None = None,
    subject_key: str | None = None,
    status: RunStatus | None = None,
    only_unjudged: bool = False,
    days: int = 7,
    limit: int = 100,
) -> dict[str, Any]:
    """Runs of the agent platform, newest first — the ledger.

    One row per run of every use case, whatever started it: an episode event,
    a schedule, a chat message or a hand. Each row carries what it produced
    (``tldr``, ``output_ref``), what it cost (``tokens_in``, ``tokens_out``,
    ``cost``, ``duration_seconds``) and the verdict it already has.

    You see your own chat runs and the machines' runs, never another
    person's. ``enduser_id`` names the person a chat run answered and
    ``surface`` where it came in (``discord`` or ``web``); both are null on a
    machine's run.

    * ``run_id`` — read one run back by id, whatever its age; that read alone
                   carries the full output ``text``
    * ``use_case`` — exact name (e.g. ``"messenger"``, ``"explain-episode"``)
    * ``subject_kind`` / ``subject_key`` — what the run was about; for an
      explanation the kind is ``"episode"`` and the key names the episode
    * ``status`` — ``queued`` | ``running`` | ``completed`` | ``failed`` | ``capped``
    * ``only_unjudged=True`` — what still needs judging
    * ``days`` — window in days on the run's creation
    """
    return await run_tools.list_runs(
        enduser_id=_caller_enduser_id(),
        run_id=run_id,
        use_case=use_case,
        subject_kind=subject_kind,
        subject_key=subject_key,
        status=status,
        only_unjudged=only_unjudged,
        days=days,
        limit=limit,
    )


@mcp.tool()
async def set_verdict(
    target: VerdictTarget,
    verdict: Verdict,
    episode_id: int | None = None,
    run_id: int | None = None,
    subject_kind: SubjectKind | None = None,
    subject_key: str | None = None,
) -> dict[str, Any]:
    """Record the owner's judgement on one episode or one run. Setting it
    again overwrites the earlier one.

    * ``target="episode"`` — was the detected situation ``"real"`` or
      ``"nonsense"``? Name it with ``episode_id`` from ``list_episodes``. The
      verdict belongs to that one situation, never to the fault as a whole —
      a verdict on the fault would be a mute switch wearing a different hat.
    * ``target="run"`` — was the platform's output ``"helpful"`` or
      ``"useless"``? Name the run with ``run_id`` from ``list_runs``, or with
      ``subject_kind`` and ``subject_key`` together for the newest run on that
      subject (``"episode"`` and the episode id for an explanation), or with
      neither for your newest chat run — the answer just given in this chat.
      Only a run ``list_runs`` shows you can be judged.

    Nothing acts on this automatically: verdicts are counted per fault and per
    use case on the "Vorfälle" dashboard, and thresholds stay a human decision
    informed by those counts. The returned row names what was judged, so the
    verdict can be confirmed against the thing it was meant for.

    This is the owner's act. An agent never calls it.
    """
    return await verdict_tools.set_verdict(
        enduser_id=_caller_enduser_id(),
        target=target,
        verdict=verdict,
        episode_id=episode_id,
        run_id=run_id,
        subject_kind=subject_kind,
        subject_key=subject_key,
    )


@mcp.tool()
async def backtest_fault(
    candidate: dict[str, Any], weeks: int = 8, limit: int = 100
) -> dict[str, Any]:
    """What one candidate fault would have found in the last ``weeks`` — the
    number a proposal to the fault list is reviewed by.

    * ``candidate`` — one entry of the fault list, in its schema: a new fault,
      or a copy of a declared one with a parameter moved (``name``,
      ``sentence``, ``unit``, ``kind``, ``parameters``, ``scope``, ``target``)
    * ``weeks`` — how far back to measure, in whole weeks off the newest
      hourly bucket; at most what the fault's measurement can read (a year
      for most kinds)
    * ``limit`` — at most this many episodes in the list, positive, capped
      like every row limit

    Nothing is written: the engine runs the candidate through the same
    schema, measurement and episode fold the detection job uses, read-only.
    Returns ``fault``, ``kind`` and ``weeks`` as measured, ``episodes``
    (oldest first: ``subject`` and ``label`` — the channel, device or room it
    accuses — ``started_at``, ``ended_at``, ``severity``, ``peak_score`` in
    the fault's own unit, ``observations``), ``episode_count`` (all of them,
    ``truncated`` when the list was cut), ``window_start`` and ``frontier``
    (the window measured) and ``measured`` (the measurement's own record:
    how many channels the scope resolved to, how many were candidates — zero
    episodes from a scope that matched nothing is not a quiet house).

    The severities are those of a rule without history: a candidate has no
    stored distribution to be rare against, so judge it by its peak score.
    A candidate the engine cannot measure is refused with the reason, worded
    the way a real edit of the fault list would fail: a schema error, an
    ``external`` fault, a target form its kind does not deliver on, a window
    past what it can read, a fault measured against the plant's expected
    yield.
    """
    return await backtest_tools.backtest_fault(candidate, weeks, limit)


@mcp.tool()
async def get_current_state(
    domain: str,
    identifier: str | None = None,
) -> dict[str, Any]:
    """Current state of a homelab domain, read live from NATS JetStream.

    ``domain`` is one of ``knx`` | ``heating`` | ``dhw`` | ``solar`` |
    ``wallbox``. ``knx`` needs an ``identifier`` group address (``"1/2/3"``);
    ``solar`` accepts an optional inverter id (``"1"`` | ``"2"``).

    Answers "what is X doing right now?" sub-second. Returns ``status: "ok"``
    with the live ``state``, an ``as_of`` timestamp and a ``freshness_seconds``
    age. Cyclic domains (heating/dhw/solar) add ``stale: true`` once the sensor
    goes quiet; event-driven domains (knx/wallbox) report the age only — an old
    value is still current. If the subject was never seen → ``status: "unknown"``
    with ``last_known_in_tsdb`` from TimescaleDB.
    """
    return await live_tools.get_current_state(
        domain,
        identifier,
        stale_after_seconds=_require_settings().live_stale_seconds,
    )


@mcp.tool()
async def subscribe_nats(
    subject: str,
    duration_seconds: int = 10,
) -> dict[str, Any]:
    """Tail a NATS subject for a short window and return the collected messages.

    Token-expensive — use only for "watch this for a moment" requests.
    ``subject`` must start with a known stream prefix (``knx.`` | ``ems-esp.`` |
    ``solaredge-1.`` | ``solaredge-2.`` | ``warp.``) and carry at least two
    concrete tokens before any wildcard, so ``knx.>`` is rejected but
    ``knx.1.2.3`` / ``knx.1.>`` are accepted. ``duration_seconds`` is capped at 30.
    """
    return await live_tools.subscribe_nats(
        subject,
        duration_seconds,
        max_duration_seconds=_require_settings().subscribe_max_seconds,
    )


@mcp.tool()
async def get_current_knx(
    room: str | None = None,
    function: str | None = None,
    name: str | None = None,
    only_active: bool = False,
    limit: int = 200,
) -> dict[str, Any]:
    """Current value of KNX group addresses matching a filter, read live from NATS.

    Use this — not ``query_knx_events`` — for "is the living-room light on right
    now?" / "which lights are on?". It reads the last retained value per group
    address from JetStream, so it also covers GAs that publish only on change
    (lights, switches) and would be missing from a recent event-log window.

    Filters (optional, AND-combined) resolve GAs via the catalog: ``room``
    (exact), ``function`` (exact, e.g. ``"Beleuchtung"``), ``name`` (substring).

    Each state carries a ``role``: ``status`` (a ``-Status`` datapoint, the
    device now), ``command`` (the datapoint it reports on, e.g. ``Ein/Aus`` or
    ``Dimmen-Absolut``; its value is the last order sent, however old, not the
    device now) or ``reading`` (sensors, meters, diagnostics). Answer "is it
    on?" from ``status`` rows. ``only_active=True`` returns only GAs whose
    current value is "on" and that are not commands.
    """
    return await live_tools.get_current_knx(
        room=room,
        function=function,
        name=name,
        only_active=only_active,
        limit=limit,
    )


@mcp.tool()
async def start_run(
    use_case: str, subject: str | None = None, focus: str | None = None
) -> dict[str, Any]:
    """Start a declared use case now, as a run of its own — only when the
    owner asks for one in so many words: "Starte die Erklärung zu Episode
    15510", "lass Propose jetzt laufen", "schlag einen Fault für den Trockner
    vor". A question — why something happened, what an episode means — is
    answered in this conversation instead, never by starting a run.

    * ``use_case`` — the declared name, e.g. ``"explain-episode"``
    * ``subject`` — what the run is about: the episode id for a use case that
      runs on an episode (``"15510"``); left out for one that runs on a schedule
    * ``focus`` — for a use case that runs on a schedule, what the owner asked
      it to look at, in the owner's words (``"ein Fault für den Trockner"``);
      left out when the owner named nothing

    The run does not answer here. Its output goes where the use case delivers
    — ``output`` in the reply names the targets, e.g. Discord and mail — and
    the run is in the ledger under ``run_id`` (``list_runs``). The trigger
    refuses a dormant use case, the chat itself, an episode that does not
    exist, a use case that has spent its runs for the day, a second request
    while the first run on that episode is still going (it names that run),
    and a schedule's job a person paused; the error says which, so tell the
    owner.
    """
    return await trigger_tools.start_run(use_case, subject, focus)


@mcp.tool()
async def get_memory(use_case: str) -> dict[str, Any]:
    """The working notes a use case keeps between its runs — read them at the
    start of a run, before deciding anything they could change.

    * ``use_case`` — the declared name, e.g. ``"propose-faults"``

    Returns ``text`` (one entry per line, oldest first), ``bytes`` (what it
    takes of the bound, about 8 KB) and ``updated_at``. A use case that has
    written nothing yet reads as an empty ``text`` with ``updated_at`` null.
    The memory is never a second truth for what the tables hold: an episode,
    a run or a verdict is read where it is.
    """
    return await memory_tools.get_memory(use_case)


@mcp.tool()
async def append_memory(use_case: str, line: str) -> dict[str, Any]:
    """Add one line to the end of a use case's memory, for its later runs to read.

    * ``use_case`` — the declared name, e.g. ``"propose-faults"``
    * ``line`` — one line that stands on its own, without line breaks: what
      was decided, about what, and when (``"2026-10-04 rejected: silence
      gap_factor 4 on Stromwert"``)

    The memory is bounded to about 8 KB. When a line takes it past that, the
    oldest lines fall away until it fits, so the newest line always stays. A
    use case that keeps no memory, an empty line, a line break and a line
    larger than the bound alone are refused with the reason. Returns the
    ``bytes`` the memory now holds.
    """
    return await memory_tools.append_memory(use_case, line)


@mcp.tool()
async def search_wiki(query: str) -> dict[str, Any]:
    """Search the house wiki — the human-written references: devices, vendor
    docs digests, how-the-house-works notes.

    Matches ``query`` against page titles, descriptions and paths. Returns
    ``results`` (``id``, ``path``, ``locale``, ``title``, ``description``) and
    ``total_hits``; read a hit in full with ``get_wiki_page``. When a search
    comes up empty, ``list_wiki_pages`` is the table of contents.
    """
    return await wiki_tools.search_wiki(query)


@mcp.tool()
async def get_wiki_page(path: str | None = None, page_id: int | None = None) -> dict[str, Any]:
    """One wiki page in full: its raw ``content`` (Markdown unless
    ``content_type`` says otherwise) plus title, description, tags and dates.

    Address it by ``path`` as returned by ``search_wiki`` / ``list_wiki_pages``
    (e.g. ``"basalte/logic-blocks"``) or by numeric ``page_id`` — exactly one
    of the two. A path that exists in several locales errors with the candidate
    ids; call again with ``page_id``.
    """
    return await wiki_tools.get_wiki_page(path=path, page_id=page_id)


@mcp.tool()
async def list_wiki_pages() -> dict[str, Any]:
    """Every wiki page the connector may read, ordered by path — ``id``,
    ``path``, ``locale``, ``title``, ``description``, ``updated_at``.

    The table of contents: use it to see where a topic lives, then read the
    page with ``get_wiki_page``.
    """
    return await wiki_tools.list_wiki_pages()


@mcp.tool()
async def update_wiki_page(path: str, content: str, title: str) -> dict[str, Any]:
    """Write one page of the house wiki: create it at ``path`` or replace the
    content and title of the page already there.

    * ``path`` — where the page lives, as ``list_wiki_pages`` shows paths
      (e.g. ``"haus/wartungsplan"``); no dots or spaces
    * ``content`` — the whole page in Markdown, not a diff: what is not in it
      is gone from the page
    * ``title`` — the page title

    A page that exists keeps its description, tags and whether it is
    published (a publish window set in the editor is cleared); a new one is
    published, without tags. Every update leaves the previous revision in the
    page's history, where a person can restore it. Read the page with
    ``get_wiki_page`` first when changing one. Returns ``action``
    (``created`` / ``updated``), ``id``, ``path``, ``locale``, ``title`` and
    ``updated_at``; a path the key may not write is refused with the wiki's
    reason.
    """
    return await wiki_tools.update_wiki_page(path=path, content=content, title=title)


@mcp.tool()
async def list_pbs_datastores() -> dict[str, Any]:
    """The backup server's datastores, each with whether its backups were
    checked — start here for "do the backups work?".

    Per datastore: ``name`` (what ``list_pbs_snapshots`` takes),
    ``total_bytes`` / ``used_bytes`` / ``avail_bytes``, ``estimated_full_at``
    (a linear guess over the last month; a date in the past means usage is
    flat or falling), ``error`` when the server cannot open it, ``gc`` (the
    garbage collection: ``schedule``, ``last_run_state``,
    ``last_run_ended_at``, ``next_run_at``, ``removed_bytes``,
    ``pending_bytes``, ``still_bad_chunks``; ``null`` when the datastore has
    no garbage-collection job) and ``verify_jobs`` (``id``, ``schedule``,
    ``last_run_state``, ``last_run_ended_at``, ``next_run_at``,
    ``last_run_upid``; empty when none is set up).

    A ``last_run_state`` is ``"OK"``, ``"WARNINGS: <n>"`` or the error
    itself; ``null`` means the job never finished a run — not a success.
    """
    return await pbs_tools.list_pbs_datastores()


@mcp.tool()
async def list_pbs_snapshots(
    datastore: str,
    backup_type: str | None = None,
    backup_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """The snapshots of one backup datastore, newest first, each with its
    last verification.

    * ``datastore`` — a ``name`` from ``list_pbs_datastores``
    * ``backup_type`` — ``"vm"``, ``"ct"`` or ``"host"``
    * ``backup_id`` — the guest's id (``"105"``); with ``backup_type`` the
      snapshots of one guest
    * ``limit`` — at most this many snapshots in the list, positive, capped
      like every row limit

    Each snapshot: ``backup_type``, ``backup_id``, ``backup_time``,
    ``size_bytes``, ``verification`` (``"ok"``, ``"failed"`` or ``null`` for
    never verified) with ``verify_upid`` (its task in ``list_pbs_tasks``),
    ``protected``, ``owner``, ``comment``. ``counts`` (``ok``, ``failed``,
    ``unverified``) and ``snapshot_count`` cover every matching snapshot, the
    cut ones too; ``truncated`` says the list was cut.
    """
    return await pbs_tools.list_pbs_snapshots(
        datastore=datastore, backup_type=backup_type, backup_id=backup_id, limit=limit
    )


@mcp.tool()
async def list_pbs_tasks(
    task_type: str | None = None,
    datastore: str | None = None,
    errors_only: bool = False,
    days: int = 7,
    limit: int = 50,
) -> dict[str, Any]:
    """The backup server's task log, newest first — what ran, and how it ended.

    * ``task_type`` — part of the task type: ``"backup"``, ``"verif"``
      (``verify`` and ``verificationjob``), ``"garbage_collection"``,
      ``"prune"``, ``"syncjob"``
    * ``datastore`` — only tasks on that datastore
    * ``errors_only=True`` — only tasks that ended in an error or warnings
    * ``days`` — window in days, by start, positive
    * ``limit`` — at most this many tasks, positive, capped like every row
      limit; ``truncated`` says more matched

    Each task: ``upid``, ``type``, ``target`` (e.g. the datastore and job, or
    the guest a backup was of), ``user``, ``started_at``, ``ended_at``,
    ``running`` and ``status`` (``"OK"``, ``"WARNINGS: <n>"`` or the error;
    ``null`` while running).
    """
    return await pbs_tools.list_pbs_tasks(
        task_type=task_type,
        datastore=datastore,
        errors_only=errors_only,
        days=days,
        limit=limit,
    )


@mcp.tool()
async def list_s3_buckets() -> dict[str, Any]:
    """The buckets of the house's S3 object store — the database and volume
    backups and the cold archive — each with how full and how fresh it is.

    Per bucket, in name order: ``name``, ``created_at``, ``object_count``,
    ``size_bytes``, ``newest_key`` and ``newest_at`` (the object written last
    — today's date there means today's backup arrived), ``oldest_at``. An
    empty bucket has ``object_count`` 0 and no times.

    ``nats-archive`` and ``influxdb-archive`` are the long-term history and
    exist nowhere else; the ``*-backups`` buckets are copies of a living
    source. Every object of every bucket is listed to find the newest, so a
    call takes a few seconds. Only listings are read, never an object.
    """
    return await s3_tools.list_s3_buckets()


@mcp.tool()
async def list_s3_objects(bucket: str, prefix: str = "", limit: int = 100) -> dict[str, Any]:
    """The objects of one bucket under a key prefix, newest first, with the
    next level of the key tree summed up.

    * ``bucket`` — a ``name`` from ``list_s3_buckets``
    * ``prefix`` — keys starting with it, e.g. ``"knx/"`` in
      ``nats-archive`` or ``"timescaledb-db/base/"`` in
      ``timescaledb-backups``; empty for the whole bucket
    * ``limit`` — at most this many objects in the list, positive, capped
      like every row limit

    Returns ``object_count``, ``size_bytes``, ``newest_key``, ``newest_at``
    and ``oldest_at`` over everything under the prefix; ``children``, one
    row per next ``/``-level below the prefix (a stream of the archive, a
    backup of a database) with its own count, size and newest object — a
    child whose ``newest_at`` lags behind its siblings stopped writing; and
    ``objects`` (``key``, ``size_bytes``, ``last_modified``), newest first,
    ``truncated`` when cut. A prefix nothing matches is an empty listing, an
    unknown bucket an error naming ``list_s3_buckets``.
    """
    return await s3_tools.list_s3_objects(bucket=bucket, prefix=prefix, limit=limit)


_settings: Settings | None = None


def _require_settings() -> Settings:
    """Return the module-level settings, raising if the app hasn't been built yet."""
    if _settings is None:
        raise RuntimeError("settings not initialised — build_app() must run first")
    return _settings


async def _livez(_request: Request) -> JSONResponse:
    # Liveness: the process is up and serving — deliberately independent of
    # DB/NATS health so an upstream outage never restarts the pod.
    return JSONResponse({"status": "alive"})


async def _healthz(_request: Request) -> JSONResponse:
    # Readiness/deep health: DB is the critical dependency and gates the
    # status code; NATS is reported for visibility but a transient blip must
    # not flap the pod (live tools only). Use /livez for liveness probes.
    db_ok = await db.healthcheck()
    body: dict[str, Any] = {"status": "ok" if db_ok else "degraded", "db": db_ok}
    if _settings is not None and _settings.nats_enabled:
        body["nats"] = await nats_module.healthcheck()
    return JSONResponse(body, status_code=200 if db_ok else 503)


def build_app() -> Starlette:
    """Load settings and assemble the Starlette app (uvicorn factory target)."""
    global _settings
    _settings = load_settings()

    # FastMCP's StreamableHTTPSessionManager runs as part of mcp_app.lifespan;
    # the parent Starlette app must include it or tool calls fail with
    # "Task group is not initialized".
    mcp_app = mcp.http_app()

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        assert _settings is not None
        configure_logging(_settings.log_level, _settings.log_format)
        # Before the pools and clients open, so their queries and calls are traced.
        tracing.configure(_settings)
        metrics_module.init()
        await db.init_pool(_settings)
        await db.init_write_pool(_settings)
        if _settings.nats_enabled:
            await nats_module.init(_settings)
        if _settings.wikijs_enabled:
            await wiki_tools.init(_settings)
        if _settings.wikijs_write_enabled:
            await wiki_tools.init_writer(_settings)
        if _settings.pbs_enabled:
            await pbs_tools.init(_settings)
        if _settings.s3_enabled:
            await s3_tools.init(_settings)
        if _settings.trigger_enabled:
            await trigger_tools.init(_settings)
        metrics_server, metrics_thread = metrics_module.serve(
            metrics_module.get(), _settings.metrics_port
        )
        log.info(
            "lares_mcp_bridge_ready",
            host=_settings.host,
            port=_settings.port,
            metrics_port=_settings.metrics_port,
            nats_enabled=_settings.nats_enabled,
            wikijs_enabled=_settings.wikijs_enabled,
            wikijs_write_enabled=_settings.wikijs_write_enabled,
            pbs_enabled=_settings.pbs_enabled,
            s3_enabled=_settings.s3_enabled,
            trigger_enabled=_settings.trigger_enabled,
            tracing_enabled=_settings.tracing_endpoint is not None,
        )
        try:
            async with mcp_app.lifespan(app):
                yield
        finally:
            metrics_server.shutdown()
            metrics_thread.join()
            await wiki_tools.close()
            await pbs_tools.close()
            await s3_tools.close()
            await trigger_tools.close()
            await nats_module.close()
            await db.close_pool()
            tracing.shutdown()

    routes = [
        Route("/livez", _livez, methods=["GET"]),
        Route("/healthz", _healthz, methods=["GET"]),
        Mount("/", app=mcp_app),
    ]
    middleware = [StarletteMiddleware(GatewayAuth, key=_settings.gateway_key)]
    return Starlette(routes=routes, middleware=middleware, lifespan=lifespan)
