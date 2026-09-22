"""MCP server wiring: tool registration, HTTP app factory, and lifespan.

Docstring convention: the ``@mcp.tool()`` docstrings in this module ARE the
tool descriptions served to the LLM over MCP — they are the API contract and
must stay accurate and self-contained. The docstrings on the implementations
in ``tools/*`` document internals for developers and must not be relied on
by clients.

Each tool here is its LLM-facing docstring plus the call into ``tools/*``;
logging and the per-tool outcome metric are one middleware, not one wrapper
per tool, and the per-client tool allowlist is another one in front of it.

The app is exposed via :func:`build_app` (uvicorn factory) so importing this
module has no side effects — settings are only loaded when the app is built.
"""

from __future__ import annotations

import fnmatch
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

import structlog
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp.tools.base import Tool, ToolResult
from mcp.types import CallToolRequestParams, ListToolsRequest
from starlette.applications import Starlette
from starlette.middleware import Middleware as StarletteMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from . import __version__, db
from . import auth as auth_module
from . import metrics as metrics_module
from . import nats as nats_module
from .config import Settings, load_settings
from .logging_setup import configure_logging, get_logger
from .tools import domain as domain_tools
from .tools import episodes as episode_tools
from .tools import forecasts as forecasts_tools
from .tools import live as live_tools
from .tools import sources as sources_tools
from .tools import timeseries as timeseries_tools
from .tools import wiki as wiki_tools
from .tools.episodes import EpisodeState
from .tools.timeseries import Aggregation

log = get_logger(__name__)


def _principal_sub() -> str:
    """Return the OIDC ``sub`` bound by AuthMiddleware, or ``anonymous``."""
    sub = structlog.contextvars.get_contextvars().get("sub")
    return str(sub) if sub else "anonymous"


def _record_tool_call(tool: str, outcome: str) -> None:
    metrics_module.get().tool_calls.labels(tool=tool, sub=_principal_sub(), outcome=outcome).inc()


def _principal_client() -> tuple[str | None, bool]:
    """The ``client_id`` AuthMiddleware bound and whether it is a machine client."""
    ctx = structlog.contextvars.get_contextvars()
    client_id = ctx.get("client_id")
    return (str(client_id) if client_id else None), ctx.get("client_kind") == "machine"


class ClientToolPolicy(Middleware):
    """Expose to each client only the tools its allowlist names.

    Keyed on the ``client_id`` AuthMiddleware bound: a machine client without
    an entry gets nothing, a user client without one keeps everything. The
    allowlist is the platform's ceiling for an agent; whatever the agent's
    own harness restricts on top is its business. A denied call is counted
    like any other outcome, so a misconfigured agent shows up in metrics
    instead of failing quietly.
    """

    @staticmethod
    def _allowed(tool: str) -> bool:
        client_id, machine = _principal_client()
        policy = _settings.auth_client_tools if _settings is not None else {}
        patterns = policy.get(client_id) if client_id is not None else None
        if patterns is None:
            return not machine
        return any(fnmatch.fnmatchcase(tool, pattern) for pattern in patterns)

    async def on_list_tools(
        self,
        context: MiddlewareContext[ListToolsRequest],
        call_next: CallNext[ListToolsRequest, Sequence[Tool]],
    ) -> Sequence[Tool]:
        tools = await call_next(context)
        return [tool for tool in tools if self._allowed(tool.name)]

    async def on_call_tool(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        tool = context.message.name
        if not self._allowed(tool):
            _record_tool_call(tool, "denied")
            log.info("tool_denied", tool=tool)
            raise ToolError(f"tool_not_allowed: {tool}")
        return await call_next(context)


class ToolTelemetry(Middleware):
    """Log every tool call and count its outcome per tool and OIDC subject."""

    async def on_call_tool(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        tool = context.message.name
        log.info("tool_invoked", tool=tool, arguments=context.message.arguments)
        try:
            result = await call_next(context)
        except Exception:
            _record_tool_call(tool, "error")
            raise
        _record_tool_call(tool, "ok")
        return result


mcp: FastMCP = FastMCP(
    "lares-mcp-bridge",
    version=__version__,
    middleware=[ClientToolPolicy(), ToolTelemetry()],
)


@mcp.tool()
async def list_data_sources() -> list[dict[str, Any]]:
    """List hypertables and continuous aggregates with their time range.

    Use this first to discover what data is available before calling
    ``get_schema`` or ``query_timeseries``.
    """
    return await sources_tools.list_data_sources()


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
    limit: int = 200,
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

    Default ``limit`` 200; effective cap ``min(limit, query_row_limit)``.
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
    limit: int = 100,
) -> dict[str, Any]:
    """Situations the detection chain recorded, newest first — the review list.

    Repeated observations of one fault fold into one episode, and this is the
    list the Basalte mails are the reminder to go through. Each row carries
    the verdict it already has, so ``only_unjudged=True`` is "what still needs
    judging".

    * ``episode_id`` — read one episode back by id, whatever its age
    * ``state`` — ``"all"`` | ``"open"`` (still running) | ``"ended"``
    * ``fault`` — exact fault name (e.g. ``"silence"``, ``"constancy"``)
    * ``days``  — window in days, by overlap: an episode that started weeks
                  ago and is still open is included in a short window
    * ``severity`` in the result is the delivery contract 1 (info) / 2
      (warning) / 3 (critical); ``affected`` and ``room`` resolve the group
      address in the subject against the KNX catalog.
    """
    return await episode_tools.list_episodes(
        state=state,
        episode_id=episode_id,
        fault=fault,
        days=days,
        only_unjudged=only_unjudged,
        limit=limit,
    )


@mcp.tool()
async def set_episode_verdict(episode_id: int, verdict: str) -> dict[str, Any]:
    """Record whether one episode was ``"real"`` or ``"nonsense"``.

    ``episode_id`` comes from ``list_episodes``. The verdict belongs to that
    one situation, never to the fault as a whole — a verdict on the fault
    would be a mute switch wearing a different hat. Setting it again
    overwrites the earlier one.

    Nothing acts on this automatically: verdicts are counted per fault on the
    "Vorfälle" dashboard, and thresholds stay a human decision informed by
    those counts. The returned row names the fault and subject, so the
    verdict can be confirmed against the episode it was meant for.
    """
    return await episode_tools.set_episode_verdict(episode_id=episode_id, verdict=verdict)


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


async def _oauth_protected_resource(_request: Request) -> JSONResponse:
    if _settings is None:
        return JSONResponse({"error": "not_ready"}, status_code=503)
    return JSONResponse(auth_module.oauth_protected_resource_metadata(_settings))


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
        metrics_module.init()
        auth_module.configure(_settings)
        await db.init_pool(_settings)
        await db.init_write_pool(_settings)
        if _settings.nats_enabled:
            await nats_module.init(_settings)
        if _settings.wikijs_enabled:
            await wiki_tools.init(_settings)
        metrics_server, metrics_thread = metrics_module.serve(
            metrics_module.get(), _settings.metrics_port
        )
        log.info(
            "lares_mcp_bridge_ready",
            host=_settings.host,
            port=_settings.port,
            metrics_port=_settings.metrics_port,
            auth_enabled=_settings.auth_enabled,
            nats_enabled=_settings.nats_enabled,
            wikijs_enabled=_settings.wikijs_enabled,
        )
        try:
            async with mcp_app.lifespan(app):
                yield
        finally:
            metrics_server.shutdown()
            metrics_thread.join()
            await wiki_tools.close()
            await nats_module.close()
            await db.close_pool()

    routes = [
        Route("/livez", _livez, methods=["GET"]),
        Route("/healthz", _healthz, methods=["GET"]),
        Route(
            "/.well-known/oauth-protected-resource",
            _oauth_protected_resource,
            methods=["GET"],
        ),
        Mount("/", app=mcp_app),
    ]
    middleware = [StarletteMiddleware(auth_module.AuthMiddleware, settings=_settings)]
    return Starlette(routes=routes, middleware=middleware, lifespan=lifespan)
