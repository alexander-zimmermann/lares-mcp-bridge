"""Prometheus metrics registry and the /metrics exposition server."""

from __future__ import annotations

import threading
from wsgiref.simple_server import WSGIServer

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Histogram,
    start_http_server,
)

from .logging_setup import get_logger

log = get_logger(__name__)


class Metrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()

        self.tool_calls = Counter(
            "lares_mcp_bridge_tool_calls_total",
            "MCP tool invocations partitioned by tool, OIDC subject, and outcome.",
            ["tool", "sub", "outcome"],
            registry=self.registry,
        )
        self.db_queries = Counter(
            "lares_mcp_bridge_db_queries_total",
            "Database queries issued by the MCP tools.",
            ["tool", "table_used"],
            registry=self.registry,
        )
        self.db_query_duration = Histogram(
            "lares_mcp_bridge_db_query_duration_seconds",
            "Wall-clock duration of database queries.",
            ["tool"],
            registry=self.registry,
        )
        self.jwks_refresh = Counter(
            "lares_mcp_bridge_jwks_refresh_total",
            "JWKS cache refreshes partitioned by result (ok|error).",
            ["result"],
            registry=self.registry,
        )
        self.nats_fetches = Counter(
            "lares_mcp_bridge_nats_fetches_total",
            "Live-state JetStream reads partitioned by domain and result (ok|miss|error).",
            ["domain", "result"],
            registry=self.registry,
        )
        self.nats_state_age = Histogram(
            "lares_mcp_bridge_nats_state_age_seconds",
            "Freshness of the live state returned to the caller, by domain.",
            ["domain"],
            registry=self.registry,
        )
        self.nats_subscribe_messages = Counter(
            "lares_mcp_bridge_nats_subscribe_messages_total",
            "Messages collected by subscribe_nats, partitioned by subject prefix.",
            ["prefix"],
            registry=self.registry,
        )


_metrics: Metrics | None = None


def init() -> Metrics:
    """Create the module-level metrics singleton (idempotent)."""
    global _metrics
    if _metrics is None:
        _metrics = Metrics()
    return _metrics


def get() -> Metrics:
    """Return the module-level metrics singleton; lazily creates one if absent."""
    if _metrics is None:
        return init()
    return _metrics


def reset() -> None:
    """Drop the singleton — for tests that need a fresh registry."""
    global _metrics
    _metrics = None


def serve(metrics: Metrics, port: int) -> tuple[WSGIServer, threading.Thread]:
    """Expose ``metrics.registry`` on ``/metrics`` via prometheus_client's threaded server.

    Returns the server and its daemon thread so the caller can ``shutdown()``
    and ``join()`` them on app teardown.
    """
    server, thread = start_http_server(port, registry=metrics.registry)
    log.info("metrics_server_listening", port=server.server_port)
    return server, thread
