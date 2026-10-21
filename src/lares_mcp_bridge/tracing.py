"""OpenTelemetry: one trace per bridge call, exported to the collector over OTLP/HTTP.

FastMCP opens a SERVER span per request on its own (``tools/call <tool>``)
and continues a ``traceparent`` the caller put in the request's ``_meta``; it
only needs a provider to export through. The server names the caller on that
span. Under it the database queries and the outgoing HTTP calls (the trigger,
the wiki, the backup server) become children through psycopg's and httpx's
instrumentation, and httpx sends ``traceparent`` on, so a run ``start_run``
asks for hangs under the call that asked. A span carries names and numbers,
never a query's parameters or a tool's arguments: an outgoing call names its
host, not its path or query, where a wiki page or a backup id would sit.

New traces are kept at the platform's sampling ratio, decided by trace id;
a trace that arrives sampled stays sampled. Without ``MCP_TRACING_ENDPOINT``
nothing is installed and every span is a no-op.
"""

from __future__ import annotations

import httpx
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor, RequestInfo
from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_VERSION, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from opentelemetry.trace import Span

from . import __version__
from .config import Settings

# The provider configure() installed, so shutdown() flushes only its own.
_provider: TracerProvider | None = None


def traces_url(endpoint: str) -> str:
    """The OTLP/HTTP traces path of a collector base URL; the exporter wants the full URL."""
    return endpoint.rstrip("/") + "/v1/traces"


def build_provider(endpoint: str, sampling_ratio: float) -> TracerProvider:
    """A provider that samples new traces at ``sampling_ratio`` and exports to ``endpoint``."""
    provider = TracerProvider(
        resource=Resource.create({SERVICE_NAME: "lares-mcp-bridge", SERVICE_VERSION: __version__}),
        sampler=ParentBased(TraceIdRatioBased(sampling_ratio)),
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=traces_url(endpoint))))
    return provider


async def _host_only(span: Span, request: RequestInfo) -> None:
    """Replace the recorded URL of an outgoing call by its scheme and host."""
    url = request.url
    # The tuple form is what transports before httpx 0.20 handed over.
    assert isinstance(url, httpx.URL)
    span.set_attribute("http.url", f"{url.scheme}://{url.netloc.decode('ascii')}")


def instrument() -> None:
    """Trace every database query and outgoing HTTP call, and carry the context on.

    Must run before the pools and clients open.
    """
    PsycopgInstrumentor().instrument()
    HTTPXClientInstrumentor().instrument(async_request_hook=_host_only)


def configure(settings: Settings) -> None:
    """Install the provider and the instrumentation; without an endpoint, nothing."""
    global _provider
    if settings.tracing_endpoint is None:
        return
    _provider = build_provider(settings.tracing_endpoint, settings.tracing_sampling_ratio)
    trace.set_tracer_provider(_provider)
    instrument()


def shutdown() -> None:
    """Flush the spans still queued; a no-op when tracing is off."""
    global _provider
    if _provider is not None:
        _provider.shutdown()
        _provider = None
