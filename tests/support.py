"""What the fixtures and the tests share: the container connection, the trigger's address
and the gateway's key."""

from __future__ import annotations

import psycopg
from testcontainers.postgres import PostgresContainer

# lares-agent-trigger as the bridge reaches it: the in-cluster URL and the key
# both pods share, answered by a respx mock.
TRIGGER_URL = "http://lares-agent-trigger.agents.svc.cluster.local:8080"
TRIGGER_KEY = "t" * 32

# The key the gateway calls the bridge with.
GATEWAY_KEY = "g" * 40


def connect(container: PostgresContainer) -> psycopg.Connection:
    """Open an autocommit connection to the running testcontainer."""
    return psycopg.connect(
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(5432)),
        user="test",
        password="test",
        dbname="homelab",
        autocommit=True,
    )
