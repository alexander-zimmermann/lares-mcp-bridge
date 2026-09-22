"""Auth module tests: JWKS roundtrip, claim validation, ASGI middleware."""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import jwt
import pytest
import respx
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from jwt.algorithms import RSAAlgorithm
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from lares_mcp_bridge import auth as auth_module
from lares_mcp_bridge.auth import AuthError, AuthMiddleware, Principal, verify_token
from lares_mcp_bridge.config import Settings

JWKS_URL = "https://issuer.test/jwks"
ISSUER = "https://issuer.test/"
AUDIENCE = "lares-mcp-bridge"
RESOURCE_URL = "https://mcp.test/mcp"


def _settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "db_host": "localhost",
        "db_name": "homelab",
        "db_username": "x",
        "db_password": "x",
        "auth_enabled": True,
        "auth_jwks_url": JWKS_URL,
        "auth_issuer": ISSUER,
        "auth_audience": AUDIENCE,
        "auth_resource_url": RESOURCE_URL,
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def _make_keypair() -> RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _public_jwk(private_key: RSAPrivateKey, kid: str) -> dict[str, Any]:
    jwk: dict[str, Any] = json.loads(RSAAlgorithm.to_jwk(private_key.public_key()))
    jwk.update({"kid": kid, "alg": "RS256", "use": "sig"})
    return jwk


def _sign(private_key: RSAPrivateKey, kid: str, claims: dict[str, Any]) -> str:
    return jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": kid})


def _claims(**overrides: Any) -> dict[str, Any]:
    now = int(time.time())
    base: dict[str, Any] = {
        "sub": "user-123",
        "iss": ISSUER,
        "aud": AUDIENCE,
        "iat": now - 5,
        "exp": now + 60,
        "azp": AUDIENCE,
    }
    base.update(overrides)
    return base


@pytest.fixture
def keypair() -> RSAPrivateKey:
    return _make_keypair()


@pytest.fixture
def jwks_mock(keypair: RSAPrivateKey) -> Iterator[respx.MockRouter]:
    jwk = _public_jwk(keypair, "key1")
    with respx.mock(assert_all_called=False) as router:
        router.get(JWKS_URL).mock(return_value=httpx.Response(200, json={"keys": [jwk]}))
        yield router


@pytest.fixture
def configured(jwks_mock: respx.MockRouter) -> Iterator[Settings]:
    settings = _settings()
    auth_module.configure(settings)
    try:
        yield settings
    finally:
        auth_module.configure(
            Settings(
                db_host="localhost",
                db_name="x",
                db_username="x",
                db_password="x",
                auth_enabled=False,
            )
        )


# ------------- Settings validation (sync) -------------


def test_disabled_auth_skips_validation() -> None:
    s = Settings(
        db_host="localhost",
        db_name="x",
        db_username="x",
        db_password="x",
        auth_enabled=False,
    )
    assert s.auth_enabled is False


def test_enabled_auth_requires_jwks_config() -> None:
    with pytest.raises(ValueError, match="MCP_AUTH_JWKS_URL"):
        Settings(
            db_host="localhost",
            db_name="x",
            db_username="x",
            db_password="x",
            auth_enabled=True,
            auth_issuer="x",
            auth_audience="x",
        )


def test_auth_clients_file_is_loaded(tmp_path: Path) -> None:
    clients = tmp_path / "clients.json"
    clients.write_text(json.dumps({"lares-agent": "k" * 40}), encoding="utf-8")
    settings = _settings(auth_clients_file=str(clients))
    assert settings.auth_clients == {"lares-agent": "k" * 40}
    assert "k" * 40 not in repr(settings)


def test_short_machine_client_key_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least 32 characters: lares-agent"):
        _settings(auth_clients={"lares-agent": "short"})


def test_malformed_clients_file_is_rejected(tmp_path: Path) -> None:
    clients = tmp_path / "clients.json"
    clients.write_text(json.dumps(["not", "an", "object"]), encoding="utf-8")
    with pytest.raises(ValueError, match="JSON object of name to key"):
        _settings(auth_clients_file=str(clients))


# ------------- verify_token -------------


@pytest.mark.asyncio
async def test_disabled_auth_passes_through() -> None:
    s = Settings(
        db_host="localhost",
        db_name="x",
        db_username="x",
        db_password="x",
        auth_enabled=False,
    )
    auth_module.configure(s)
    assert await verify_token(None, s) is None
    assert await verify_token("anything", s) is None


@pytest.mark.asyncio
async def test_missing_token_raises(configured: Settings) -> None:
    with pytest.raises(AuthError, match="missing_bearer_token"):
        await verify_token(None, configured)


@pytest.mark.asyncio
async def test_valid_token_returns_principal(configured: Settings, keypair: RSAPrivateKey) -> None:
    token = _sign(keypair, "key1", _claims(sub="alex", azp="lares-mcp-bridge"))
    principal = await verify_token(token, configured)
    assert isinstance(principal, Principal)
    assert principal.sub == "alex"
    assert principal.client_id == "lares-mcp-bridge"
    assert principal.claims["iss"] == ISSUER


@pytest.mark.asyncio
async def test_expired_token_raises(configured: Settings, keypair: RSAPrivateKey) -> None:
    now = int(time.time())
    token = _sign(keypair, "key1", _claims(iat=now - 3600, exp=now - 60))
    with pytest.raises(AuthError, match="token_expired"):
        await verify_token(token, configured)


@pytest.mark.asyncio
async def test_wrong_audience_raises(configured: Settings, keypair: RSAPrivateKey) -> None:
    token = _sign(keypair, "key1", _claims(aud="wrong-audience"))
    with pytest.raises(AuthError, match="invalid_audience"):
        await verify_token(token, configured)


@pytest.mark.asyncio
async def test_wrong_issuer_raises(configured: Settings, keypair: RSAPrivateKey) -> None:
    token = _sign(keypair, "key1", _claims(iss="https://attacker.test/"))
    with pytest.raises(AuthError, match="invalid_issuer"):
        await verify_token(token, configured)


async def test_machine_key_yields_machine_principal(jwks_mock: respx.MockRouter) -> None:
    settings = _settings(auth_clients={"lares-agent": "k" * 40, "other": "o" * 40})
    auth_module.configure(settings)
    principal = await verify_token("k" * 40, settings)
    assert principal == Principal(
        sub="lares-agent", client_id="lares-agent", claims={}, machine=True
    )
    assert principal.kind == "machine"


async def test_unknown_key_falls_through_to_jwt(jwks_mock: respx.MockRouter) -> None:
    """A key that matches no client is handled as a token, and fails as one."""
    settings = _settings(auth_clients={"lares-agent": "k" * 40})
    auth_module.configure(settings)
    with pytest.raises(AuthError) as exc_info:
        await verify_token("x" * 40, settings)
    assert exc_info.value.reason == "invalid_token"


def test_match_machine_client_compares_every_key() -> None:
    clients = {"a": "a" * 40, "b": "b" * 40}
    assert auth_module.match_machine_client("b" * 40, clients) == "b"
    assert auth_module.match_machine_client("c" * 40, clients) is None
    assert auth_module.match_machine_client("", clients) is None


@pytest.mark.asyncio
async def test_signature_from_unknown_key_raises(
    configured: Settings,
) -> None:
    # Token kid is not in JWKS even after a refresh.
    foreign = _make_keypair()
    token = _sign(foreign, "key-unknown", _claims())
    with pytest.raises(AuthError, match="unknown_signing_key"):
        await verify_token(token, configured)


@pytest.mark.asyncio
async def test_jwks_refreshes_on_unknown_kid(keypair: RSAPrivateKey) -> None:
    """A token signed with kid=key2 forces a JWKS refresh that returns key2."""
    jwk_v1 = _public_jwk(keypair, "key1")
    jwk_v2 = _public_jwk(keypair, "key2")

    # Cooldown 0 — this test exercises the rotation refetch, not the rate limit.
    settings = _settings(auth_jwks_min_refresh_seconds=0)
    with respx.mock(assert_all_called=False) as router:
        route = router.get(JWKS_URL).mock(
            side_effect=[
                httpx.Response(200, json={"keys": [jwk_v1]}),
                httpx.Response(200, json={"keys": [jwk_v1, jwk_v2]}),
            ]
        )
        auth_module.configure(settings)

        # Prime cache with kid=key1 (1 fetch).
        token1 = _sign(keypair, "key1", _claims())
        assert await verify_token(token1, settings) is not None

        # Sign with kid=key2 — cache miss must trigger one more fetch.
        token2 = _sign(keypair, "key2", _claims())
        principal = await verify_token(token2, settings)
        assert principal is not None
        assert route.call_count == 2


@pytest.mark.asyncio
async def test_unknown_kid_within_cooldown_does_not_refetch(keypair: RSAPrivateKey) -> None:
    """Unknown-kid tokens cannot force repeated JWKS fetches (issuer-DoS guard)."""
    jwk_v1 = _public_jwk(keypair, "key1")

    settings = _settings()  # default 30 s cooldown
    with respx.mock(assert_all_called=False) as router:
        route = router.get(JWKS_URL).mock(return_value=httpx.Response(200, json={"keys": [jwk_v1]}))
        auth_module.configure(settings)

        token1 = _sign(keypair, "key1", _claims())
        assert await verify_token(token1, settings) is not None

        foreign = _make_keypair()
        token2 = _sign(foreign, "key-unknown", _claims())
        with pytest.raises(AuthError, match="unknown_signing_key"):
            await verify_token(token2, settings)
        assert route.call_count == 1  # the rotation refetch was rate-limited


@pytest.mark.asyncio
async def test_jwks_outage_serves_stale_keys(keypair: RSAPrivateKey) -> None:
    """A failing JWKS refresh falls back to the previously cached key set."""
    jwk_v1 = _public_jwk(keypair, "key1")

    settings = _settings(auth_jwks_ttl_seconds=0, auth_jwks_min_refresh_seconds=0)
    with respx.mock(assert_all_called=False) as router:
        router.get(JWKS_URL).mock(
            side_effect=[
                httpx.Response(200, json={"keys": [jwk_v1]}),
                httpx.ConnectError("jwks down"),
            ]
        )
        auth_module.configure(settings)

        token = _sign(keypair, "key1", _claims())
        assert await verify_token(token, settings) is not None  # prime (fetch 1)
        # TTL 0 forces a refresh; the fetch fails but the stale set still validates.
        assert await verify_token(token, settings) is not None


@pytest.mark.asyncio
async def test_jwks_outage_with_empty_cache_is_503(keypair: RSAPrivateKey) -> None:
    """No cached keys + unreachable JWKS endpoint → 503-grade AuthError."""
    settings = _settings()
    with respx.mock(assert_all_called=False) as router:
        router.get(JWKS_URL).mock(side_effect=httpx.ConnectError("jwks down"))
        auth_module.configure(settings)

        token = _sign(keypair, "key1", _claims())
        with pytest.raises(AuthError) as exc_info:
            await verify_token(token, settings)
        assert exc_info.value.reason == "jwks_unavailable"
        assert exc_info.value.status == 503


# ------------- AuthMiddleware -------------


async def _ok(request: Request) -> PlainTextResponse:
    return PlainTextResponse(f"hello {request.scope.get('path')}")


def _build_app(settings: Settings) -> AuthMiddleware:
    inner = Starlette(
        routes=[
            Route("/mcp", _ok),
            Route("/healthz", _ok),
            Route("/.well-known/oauth-protected-resource", _ok),
        ]
    )
    return AuthMiddleware(inner, settings)


@pytest.mark.asyncio
async def test_middleware_401_without_token(configured: Settings) -> None:
    app = _build_app(configured)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="https://test") as c:
        r = await c.get("/mcp")
    assert r.status_code == 401
    challenge = r.headers["www-authenticate"]
    assert 'realm="lares-mcp-bridge"' in challenge
    assert 'error="invalid_token"' in challenge
    assert "resource_metadata=" in challenge


@pytest.mark.asyncio
async def test_middleware_401_on_expired_token(
    configured: Settings, keypair: RSAPrivateKey
) -> None:
    now = int(time.time())
    token = _sign(keypair, "key1", _claims(iat=now - 3600, exp=now - 60))
    app = _build_app(configured)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="https://test") as c:
        r = await c.get("/mcp", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 401
    assert "token_expired" in r.headers["www-authenticate"]


@pytest.mark.asyncio
async def test_middleware_200_with_valid_token(
    configured: Settings, keypair: RSAPrivateKey
) -> None:
    token = _sign(keypair, "key1", _claims())
    app = _build_app(configured)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="https://test") as c:
        r = await c.get("/mcp", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
    assert r.text == "hello /mcp"


@pytest.mark.asyncio
async def test_middleware_503_when_jwks_unavailable(keypair: RSAPrivateKey) -> None:
    """JWKS outage with no cached keys surfaces as 503, not 401 or a crash."""
    settings = _settings()
    with respx.mock(assert_all_called=False) as router:
        router.get(JWKS_URL).mock(side_effect=httpx.ConnectError("jwks down"))
        auth_module.configure(settings)

        token = _sign(keypair, "key1", _claims())
        app = _build_app(settings)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="https://test") as c:
            r = await c.get("/mcp", headers={"Authorization": f"Bearer {token}"})
        assert r.status_code == 503
        assert "www-authenticate" not in r.headers


@pytest.mark.asyncio
async def test_middleware_healthz_bypasses_auth(configured: Settings) -> None:
    app = _build_app(configured)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="https://test") as c:
        r = await c.get("/healthz")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_middleware_well_known_bypasses_auth(configured: Settings) -> None:
    app = _build_app(configured)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="https://test") as c:
        r = await c.get("/.well-known/oauth-protected-resource")
    assert r.status_code == 200
