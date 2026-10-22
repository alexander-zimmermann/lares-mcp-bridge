"""The gateway's sign-in: one key opens the bridge, two headers name the caller.

The bridge answers the gateway only. The gateway signs every caller in itself,
serves each the tools of its route, and calls the bridge with one static key,
``MCP_GATEWAY_KEY_FILE``, as a bearer token. A request without that key gets
401.

The gateway names the caller in two headers it sets itself and overwrites
whatever a caller sent: ``X-Client-Id``, the route the call came in on, and
``X-Enduser-Id``, the person's handle, absent for a machine. ``GatewayAuth``
(pure ASGI) binds them to structlog's contextvars as ``client_id`` and
``enduser.id``, so every log line of the request names the caller, and the
run tools, the span and the outcome metric read it there.
"""

from __future__ import annotations

import hmac
from typing import TYPE_CHECKING

import structlog

from .logging_setup import get_logger

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Receive, Scope, Send

log = get_logger(__name__)

# Probe endpoints, which the kubelet calls without the key.
_PUBLIC_PATHS: frozenset[str] = frozenset({"/healthz", "/livez"})

_UNAUTHORIZED = b'{"error":"unauthorized"}'


def _header(headers: list[tuple[bytes, bytes]], wanted: bytes) -> str | None:
    for name, value in headers:
        if name.lower() == wanted:
            return value.decode("latin-1").strip() or None
    return None


def _bearer(headers: list[tuple[bytes, bytes]]) -> str | None:
    value = _header(headers, b"authorization")
    if value is None or value[:7].lower() != "bearer ":
        return None
    return value[7:].strip() or None


async def _refuse(send: Send) -> None:
    headers = [
        (b"content-type", b"application/json"),
        (b"www-authenticate", b'Bearer realm="lares-mcp-bridge"'),
        (b"content-length", str(len(_UNAUTHORIZED)).encode("ascii")),
    ]
    await send({"type": "http.response.start", "status": 401, "headers": headers})
    await send({"type": "http.response.body", "body": _UNAUTHORIZED})


class GatewayAuth:
    """Admit a request carrying the gateway's key, and bind the caller it names."""

    def __init__(self, app: ASGIApp, key: str) -> None:
        self.app = app
        self._key = key.encode("utf-8")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("path") in _PUBLIC_PATHS:
            await self.app(scope, receive, send)
            return

        headers = scope.get("headers", [])
        token = _bearer(headers)
        if token is None or not hmac.compare_digest(token.encode("utf-8"), self._key):
            log.info("auth_rejected", reason="gateway_key", path=scope.get("path"))
            await _refuse(send)
            return
        client_id = _header(headers, b"x-client-id")
        if client_id is None:
            log.info("auth_rejected", reason="no_client", path=scope.get("path"))
            await _refuse(send)
            return

        enduser_id = _header(headers, b"x-enduser-id")
        enduser = {} if enduser_id is None else {"enduser.id": enduser_id}
        bound = structlog.contextvars.bind_contextvars(client_id=client_id, **enduser)
        try:
            await self.app(scope, receive, send)
        finally:
            structlog.contextvars.unbind_contextvars(*bound.keys())
