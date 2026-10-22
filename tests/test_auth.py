"""The gateway's sign-in: its key opens the bridge, its two headers name the caller."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import structlog
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route

from lares_mcp_bridge.auth import GatewayAuth
from lares_mcp_bridge.config import Settings

from .support import GATEWAY_KEY


async def _identity(_request: Request) -> JSONResponse:
    return JSONResponse(structlog.contextvars.get_contextvars())


async def _ok(_request: Request) -> PlainTextResponse:
    return PlainTextResponse("ok")


def _client() -> httpx.AsyncClient:
    inner = Starlette(routes=[Route("/mcp", _identity), Route("/healthz", _ok)])
    transport = httpx.ASGITransport(app=GatewayAuth(inner, GATEWAY_KEY))
    return httpx.AsyncClient(transport=transport, base_url="http://bridge")


def _gateway(**headers: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {GATEWAY_KEY}", **headers}


async def test_a_request_without_the_gateways_key_is_refused(clean_context: None) -> None:
    async with _client() as client:
        response = await client.get("/mcp", headers={"X-Client-Id": "lares"})
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == 'Bearer realm="lares-mcp-bridge"'


async def test_a_wrong_key_is_refused(clean_context: None) -> None:
    async with _client() as client:
        response = await client.get(
            "/mcp", headers={"Authorization": f"Bearer {'h' * 40}", "X-Client-Id": "lares"}
        )
    assert response.status_code == 401


async def test_the_headers_name_the_client_and_the_person(clean_context: None) -> None:
    """Every log line of the request carries the caller, and the tools read it there."""
    async with _client() as client:
        person = await client.get(
            "/mcp", headers=_gateway(**{"X-Client-Id": "lares", "X-Enduser-Id": "kim"})
        )
        machine = await client.get("/mcp", headers=_gateway(**{"X-Client-Id": "lares-memory"}))

    assert person.json() == {"client_id": "lares", "enduser.id": "kim"}
    assert machine.json() == {"client_id": "lares-memory"}
    assert structlog.contextvars.get_contextvars() == {}


async def test_a_call_the_gateway_names_no_client_for_is_refused(clean_context: None) -> None:
    async with _client() as client:
        response = await client.get("/mcp", headers=_gateway(**{"X-Enduser-Id": "kim"}))
    assert response.status_code == 401


async def test_the_probes_need_no_key(clean_context: None) -> None:
    async with _client() as client:
        response = await client.get("/healthz")
    assert response.status_code == 200


def _settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "db_host": "localhost",
        "db_name": "x",
        "db_username": "x",
        "db_password": "x",
    }
    return Settings(**{**base, **overrides})  # type: ignore[arg-type]


def test_the_gateways_key_is_read_from_its_file(tmp_path: Path) -> None:
    key_file = tmp_path / "gateway-key"
    key_file.write_text(f"{GATEWAY_KEY}\n", encoding="utf-8")
    assert _settings(gateway_key_file=str(key_file)).gateway_key == GATEWAY_KEY


def test_the_bridge_does_not_start_without_the_gateways_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The settings fixture leaves the key in the environment for the session.
    monkeypatch.delenv("MCP_GATEWAY_KEY", raising=False)
    with pytest.raises(ValueError, match="MCP_GATEWAY_KEY or MCP_GATEWAY_KEY_FILE is required"):
        _settings()


def test_a_short_gateway_key_refuses_the_start() -> None:
    with pytest.raises(ValueError, match="at least 32 characters"):
        _settings(gateway_key="short")
