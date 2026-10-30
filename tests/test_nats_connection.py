"""The shared NATS connection: unlimited reconnect, one warning line per failed attempt."""

from __future__ import annotations

from typing import Any

import nats as natslib
import pytest
from structlog.testing import capture_logs

from lares_mcp_bridge import nats as nats_module
from lares_mcp_bridge.config import Settings

from .support import GATEWAY_KEY


class FakeClient:
    """What init() needs of a connected nats.py client."""

    is_connected = True

    def jetstream(self) -> object:
        return object()


async def _connected(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Open the module's connection against a fake nats.py; what it asked of it."""
    asked: dict[str, Any] = {}

    async def fake_connect(**kwargs: Any) -> FakeClient:
        asked.update(kwargs)
        return FakeClient()

    monkeypatch.setattr(natslib, "connect", fake_connect)
    monkeypatch.setattr(nats_module, "_nc", None)
    monkeypatch.setattr(nats_module, "_js", None)
    settings = Settings(
        db_host="unused",
        db_name="unused",
        db_username="unused",
        db_password="unused",
        gateway_key=GATEWAY_KEY,
        nats_servers="nats://unused:4222",
    )
    await nats_module.init(settings)
    return asked


async def test_the_client_reconnects_for_as_long_as_it_lives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asked = await _connected(monkeypatch)

    assert asked["name"] == "lares-mcp-bridge"
    assert asked["max_reconnect_attempts"] == -1
    assert asked["reconnect_time_wait"] == 2


async def test_a_failed_attempt_is_one_warning_line_without_a_traceback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asked = await _connected(monkeypatch)

    with capture_logs() as logs:
        await asked["error_cb"](ConnectionRefusedError(111, "Connect call failed"))
        await asked["disconnected_cb"]()
        await asked["reconnected_cb"]()
        await asked["closed_cb"]()

    assert [(entry["log_level"], entry["event"]) for entry in logs] == [
        ("warning", "nats_error"),
        ("warning", "nats_disconnected"),
        ("info", "nats_reconnected"),
        ("warning", "nats_closed"),
    ]
    assert "Connect call failed" in logs[0]["error"]
    assert "exc_info" not in logs[0]
