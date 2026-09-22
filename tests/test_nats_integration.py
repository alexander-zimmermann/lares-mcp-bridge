"""Integration tests for the NATS JetStream layer against a real nats-server.

Unlike test_live_tools.py (which mocks ``lares_mcp_bridge.nats``), these exercise
the actual nats-py API usage — ``get_last_msg`` parsing and the core-subscribe
tail — against a throwaway ``nats:2.10`` container with JetStream enabled.
Skipped automatically when Docker is unavailable.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Iterator

import nats as natslib
import pytest
import pytest_asyncio
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from lares_mcp_bridge import nats as nats_module
from lares_mcp_bridge.config import Settings


@pytest.fixture(scope="module")
def nats_url() -> Iterator[str]:
    container = DockerContainer("nats:2.10-alpine").with_command("-js").with_exposed_ports(4222)
    container.start()
    try:
        wait_for_logs(container, "Server is ready")
        host = container.get_container_host_ip()
        port = container.get_exposed_port(4222)
        yield f"nats://{host}:{port}"
    finally:
        container.stop()


@pytest_asyncio.fixture
async def seeded_nats(nats_url: str) -> AsyncIterator[str]:
    """Create the KNX stream + seed one message, then open our module connection."""
    seed = await natslib.connect(servers=[nats_url])
    js = seed.jetstream()
    await js.add_stream(name="KNX", subjects=["knx.>"])
    await js.publish("knx.1.2.3", json.dumps({"ga": "1/2/3", "value": 21.5}).encode())
    await seed.flush()
    await seed.close()

    settings = Settings(
        db_host="unused",
        db_name="unused",
        db_username="unused",
        db_password="unused",
        nats_servers=nats_url,
    )
    await nats_module.init(settings)
    try:
        yield nats_url
    finally:
        await nats_module.close()


async def test_last_msg_round_trip(seeded_nats: str) -> None:
    msg = await nats_module.last_msg("KNX", "knx.1.2.3")
    assert msg is not None
    assert json.loads(msg.data)["value"] == 21.5
    assert msg.ts.tzinfo is not None  # tz-aware UTC parsed from the JetStream time


async def test_last_msg_missing_subject_returns_none(seeded_nats: str) -> None:
    assert await nats_module.last_msg("KNX", "knx.9.9.9") is None


async def test_tail_collects_live_messages(seeded_nats: str) -> None:
    async def _publish_after_delay() -> None:
        await asyncio.sleep(0.3)
        pub = await natslib.connect(servers=[seeded_nats])
        await pub.publish("knx.1.2.4", json.dumps({"value": True}).encode())
        await pub.flush()
        await pub.close()

    publisher = asyncio.create_task(_publish_after_delay())
    msgs = await nats_module.tail("knx.1.2.4", duration_s=2.0, max_messages=10)
    await publisher
    assert any(json.loads(m.data).get("value") is True for m in msgs)
