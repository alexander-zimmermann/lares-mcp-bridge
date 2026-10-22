"""Backup-tool tests against a respx-mocked Proxmox Backup Server.

The answers below are the API's own shapes (kebab-case, epoch seconds,
everything under ``data``), cut down to the fields the tools read. Nothing
here needs the database.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio
import respx

from lares_mcp_bridge.config import Settings
from lares_mcp_bridge.tools import pbs

from .support import GATEWAY_KEY

PBS_URL = "https://backup.test:8007"
TOKEN_ID = "lares-mcp-bridge@pbs!lares-mcp-bridge"
SECRET = "0f3c9a1e-5b7d-4c2a-9e8f-1a2b3c4d5e6f"

# 2026-10-03 00:00:00 UTC and friends, as PBS sends them.
MIDNIGHT = 1_790_985_600
UPID_VERIFY = (
    "UPID:backup-01:00001F40:0001A2B3:00000003:68DF1100:verificationjob:"
    "datastore-primary\\x3av\\x2dverify\\x2ddatastore\\x2dprimary:root@pam:"
)
UPID_GC = (
    "UPID:backup-01:00001F41:0001A2B4:00000004:68DF0F00:garbage_collection:"
    "datastore-primary:root@pam:"
)

_USAGE = [
    {
        "store": "datastore-primary",
        "total": 1_000_000_000_000,
        "used": 610_000_000_000,
        "avail": 390_000_000_000,
        "history": [0.6, 0.61],
        "history-start": MIDNIGHT - 86_400,
        "history-delta": 86_400,
        "estimated-full-date": 1_806_000_000,
        "gc-status": {
            "upid": UPID_GC,
            "removed-bytes": 1024,
            "pending-bytes": 2048,
            "still-bad": 0,
        },
    },
    {"store": "datastore-secondary", "error": "datastore is offline (ENOENT)"},
]
_GC_JOBS = [
    {
        "store": "datastore-primary",
        "upid": UPID_GC,
        "removed-bytes": 1024,
        "pending-bytes": 2048,
        "still-bad": 0,
        "schedule": "daily",
        "next-run": MIDNIGHT + 86_400,
        "last-run-endtime": MIDNIGHT + 120,
        "last-run-state": "OK",
        "duration": 120,
    }
]
_VERIFY_JOBS = [
    {
        "id": "verify-datastore-primary",
        "store": "datastore-primary",
        "schedule": "daily",
        "next-run": MIDNIGHT + 86_400,
        "last-run-state": "OK",
        "last-run-upid": UPID_VERIFY,
        "last-run-endtime": MIDNIGHT + 720,
    },
    {
        "id": "verify-datastore-secondary",
        "store": "datastore-secondary",
        "schedule": "daily",
        "next-run": MIDNIGHT + 86_400,
    },
]


def _snapshot(
    backup_id: str, time: int, verification: str | None, backup_type: str = "vm"
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "backup-type": backup_type,
        "backup-id": backup_id,
        "backup-time": time,
        "files": [{"filename": "index.json.blob", "size": 512}],
        "size": 41_000_000_000,
        "owner": "backup@pbs",
        "protected": False,
    }
    if verification is not None:
        entry["verification"] = {"state": verification, "upid": UPID_VERIFY}
    return entry


def _settings(
    pbs_url: str | None = None, pbs_token_id: str | None = None, pbs_token_file: str | None = None
) -> Settings:
    return Settings(
        db_host="localhost",
        db_name="x",
        db_username="x",
        db_password="x",
        gateway_key=GATEWAY_KEY,
        nats_enabled=False,
        pbs_url=pbs_url,
        pbs_token_id=pbs_token_id,
        pbs_token_file=pbs_token_file,
    )


def _data(data: Any) -> httpx.Response:
    return httpx.Response(200, json={"data": data})


@pytest.fixture
def token_file(tmp_path: Path) -> Path:
    path = tmp_path / "pbs-token"
    path.write_text(f"{SECRET}\n", encoding="utf-8")  # trailing newline, like a mounted Secret
    return path


@pytest_asyncio.fixture
async def pbs_client(token_file: Path) -> AsyncIterator[None]:
    await pbs.init(_settings(PBS_URL, TOKEN_ID, str(token_file)))
    try:
        yield
    finally:
        await pbs.close()


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    with respx.mock(base_url=f"{PBS_URL}/api2/json", assert_all_called=False) as mock:
        yield mock


# ---------------------------------------------------------------- settings and lifecycle


def test_pbs_settings_must_be_set_together(token_file: Path) -> None:
    with pytest.raises(ValueError, match="MCP_PBS_URL, MCP_PBS_TOKEN_ID and MCP_PBS_TOKEN_FILE"):
        _settings(pbs_url=PBS_URL)
    with pytest.raises(ValueError, match="MCP_PBS_URL, MCP_PBS_TOKEN_ID and MCP_PBS_TOKEN_FILE"):
        _settings(pbs_url=PBS_URL, pbs_token_file=str(token_file))


def test_pbs_enabled_only_when_all_set(token_file: Path) -> None:
    assert _settings().pbs_enabled is False
    assert _settings(PBS_URL, TOKEN_ID, str(token_file)).pbs_enabled


async def test_tools_refuse_when_not_initialised() -> None:
    with pytest.raises(RuntimeError, match="MCP_PBS_URL"):
        await pbs.list_pbs_datastores()


async def test_init_rejects_empty_token_file(tmp_path: Path) -> None:
    empty = tmp_path / "pbs-token"
    empty.write_text("\n", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        await pbs.init(_settings(PBS_URL, TOKEN_ID, str(empty)))


# ---------------------------------------------------------------- list_pbs_datastores


async def test_datastores_join_usage_gc_and_verification(
    pbs_client: None, router: respx.MockRouter
) -> None:
    usage = router.get("/status/datastore-usage").mock(return_value=_data(_USAGE))
    router.get("/admin/gc").mock(return_value=_data(_GC_JOBS))
    router.get("/admin/verify").mock(return_value=_data(_VERIFY_JOBS))

    out = await pbs.list_pbs_datastores()

    # The audit token, in the header form PBS reads.
    assert usage.calls.last.request.headers["authorization"] == f"PBSAPIToken={TOKEN_ID}:{SECRET}"
    primary, secondary = out["datastores"]
    assert primary == {
        "name": "datastore-primary",
        "total_bytes": 1_000_000_000_000,
        "used_bytes": 610_000_000_000,
        "avail_bytes": 390_000_000_000,
        "estimated_full_at": "2027-03-25T18:40:00+00:00",
        "error": None,
        "gc": {
            "schedule": "daily",
            "last_run_state": "OK",
            "last_run_ended_at": "2026-10-03T00:02:00+00:00",
            "next_run_at": "2026-10-04T00:00:00+00:00",
            "removed_bytes": 1024,
            "pending_bytes": 2048,
            "still_bad_chunks": 0,
        },
        "verify_jobs": [
            {
                "id": "verify-datastore-primary",
                "schedule": "daily",
                "last_run_state": "OK",
                "last_run_ended_at": "2026-10-03T00:12:00+00:00",
                "last_run_upid": UPID_VERIFY,
                "next_run_at": "2026-10-04T00:00:00+00:00",
            }
        ],
    }
    # A datastore PBS cannot open says so, and its verify job that never ran
    # reads as never run rather than as a success.
    assert secondary["name"] == "datastore-secondary"
    assert secondary["error"] == "datastore is offline (ENOENT)"
    assert secondary["used_bytes"] is None
    assert secondary["gc"] is None
    assert secondary["verify_jobs"] == [
        {
            "id": "verify-datastore-secondary",
            "schedule": "daily",
            "last_run_state": None,
            "last_run_ended_at": None,
            "last_run_upid": None,
            "next_run_at": "2026-10-04T00:00:00+00:00",
        }
    ]


async def test_a_token_pbs_refuses_names_the_permission(
    pbs_client: None, router: respx.MockRouter
) -> None:
    router.get("/status/datastore-usage").mock(
        return_value=httpx.Response(401, json={"data": None, "message": "authentication failed"})
    )
    router.get("/admin/gc").mock(return_value=_data([]))
    router.get("/admin/verify").mock(return_value=_data([]))

    with pytest.raises(pbs.PbsError, match="Audit"):
        await pbs.list_pbs_datastores()


async def test_server_errors_propagate(pbs_client: None, router: respx.MockRouter) -> None:
    router.get("/status/datastore-usage").mock(return_value=httpx.Response(500, text="boom"))
    router.get("/admin/gc").mock(return_value=_data([]))
    router.get("/admin/verify").mock(return_value=_data([]))

    with pytest.raises(httpx.HTTPStatusError):
        await pbs.list_pbs_datastores()


# ---------------------------------------------------------------- list_pbs_snapshots


async def test_snapshots_newest_first_with_verification_and_counts(
    pbs_client: None, router: respx.MockRouter
) -> None:
    route = router.get("/admin/datastore/datastore-primary/snapshots").mock(
        return_value=_data(
            [
                _snapshot("105", MIDNIGHT - 7200, "ok"),
                _snapshot("105", MIDNIGHT - 3600, None),
                _snapshot("110", MIDNIGHT - 5400, "failed"),
            ]
        )
    )

    out = await pbs.list_pbs_snapshots("datastore-primary", backup_type="vm", limit=2)

    assert dict(route.calls.last.request.url.params) == {"backup-type": "vm"}
    assert out["datastore"] == "datastore-primary"
    assert out["counts"] == {"ok": 1, "failed": 1, "unverified": 1}
    assert out["snapshot_count"] == 3
    assert out["truncated"] is True
    assert out["snapshots"] == [
        {
            "backup_type": "vm",
            "backup_id": "105",
            "backup_time": "2026-10-02T23:00:00+00:00",
            "size_bytes": 41_000_000_000,
            "verification": None,
            "verify_upid": None,
            "protected": False,
            "owner": "backup@pbs",
            "comment": None,
        },
        {
            "backup_type": "vm",
            "backup_id": "110",
            "backup_time": "2026-10-02T22:30:00+00:00",
            "size_bytes": 41_000_000_000,
            "verification": "failed",
            "verify_upid": UPID_VERIFY,
            "protected": False,
            "owner": "backup@pbs",
            "comment": None,
        },
    ]


async def test_snapshots_of_one_group(pbs_client: None, router: respx.MockRouter) -> None:
    route = router.get("/admin/datastore/datastore-primary/snapshots").mock(return_value=_data([]))

    out = await pbs.list_pbs_snapshots("datastore-primary", backup_type="ct", backup_id="201")

    assert dict(route.calls.last.request.url.params) == {"backup-type": "ct", "backup-id": "201"}
    assert out["snapshots"] == []
    assert out["truncated"] is False


async def test_an_unknown_datastore_says_so(pbs_client: None, router: respx.MockRouter) -> None:
    router.get("/admin/datastore/nope/snapshots").mock(
        return_value=httpx.Response(
            400, json={"data": None, "message": "no such datastore 'nope'\n"}
        )
    )

    with pytest.raises(pbs.PbsError, match="no such datastore 'nope'"):
        await pbs.list_pbs_snapshots("nope")


async def test_snapshot_limit_must_be_positive(pbs_client: None) -> None:
    with pytest.raises(ValueError, match="invalid_limit"):
        await pbs.list_pbs_snapshots("datastore-primary", limit=0)


# ---------------------------------------------------------------- list_pbs_tasks


async def test_tasks_with_their_state(pbs_client: None, router: respx.MockRouter) -> None:
    route = router.get("/nodes/localhost/tasks").mock(
        return_value=_data(
            [
                {
                    "upid": UPID_VERIFY,
                    "node": "backup-01",
                    "pid": 8000,
                    "pstart": 107187,
                    "starttime": MIDNIGHT,
                    "worker_type": "verificationjob",
                    "worker_id": "datastore-primary:v-verify-datastore-primary",
                    "user": "root@pam",
                    "endtime": MIDNIGHT + 720,
                    "status": "ERROR: verification failed - please check the log for details",
                },
                {
                    "upid": UPID_GC,
                    "node": "backup-01",
                    "pid": 8001,
                    "pstart": 107188,
                    "starttime": MIDNIGHT + 900,
                    "worker_type": "garbage_collection",
                    "worker_id": "datastore-primary",
                    "user": "root@pam",
                },
            ]
        )
    )

    out = await pbs.list_pbs_tasks(
        task_type="verif", datastore="datastore-primary", errors_only=True, days=2, limit=10
    )

    params = route.calls.last.request.url.params
    assert params["typefilter"] == "verif"
    assert params["store"] == "datastore-primary"
    assert params["errors"] == "1"
    # One more than asked, so a cut list says so.
    assert params["limit"] == "11"
    assert abs(int(params["since"]) - (time.time() - 2 * 86_400)) < 60
    assert out["truncated"] is False
    failed, running = out["tasks"]
    assert failed == {
        "upid": UPID_VERIFY,
        "type": "verificationjob",
        "target": "datastore-primary:v-verify-datastore-primary",
        "user": "root@pam",
        "started_at": "2026-10-03T00:00:00+00:00",
        "ended_at": "2026-10-03T00:12:00+00:00",
        "running": False,
        "status": "ERROR: verification failed - please check the log for details",
    }
    assert running["running"] is True
    assert running["ended_at"] is None
    assert running["status"] is None


async def test_a_full_task_page_is_truncated(pbs_client: None, router: respx.MockRouter) -> None:
    task = {
        "upid": UPID_GC,
        "node": "backup-01",
        "pid": 1,
        "pstart": 1,
        "starttime": MIDNIGHT,
        "worker_type": "garbage_collection",
        "worker_id": "datastore-primary",
        "user": "root@pam",
        "endtime": MIDNIGHT + 1,
        "status": "OK",
    }
    router.get("/nodes/localhost/tasks").mock(return_value=_data([task, task, task]))

    out = await pbs.list_pbs_tasks(limit=2)

    assert len(out["tasks"]) == 2
    assert out["truncated"] is True


async def test_task_window_must_be_positive(pbs_client: None) -> None:
    with pytest.raises(ValueError, match="invalid_days"):
        await pbs.list_pbs_tasks(days=0)
