"""Object-storage tools against a real rustfs, the S3 service the house runs.

A throwaway ``rustfs/rustfs`` container stands in for the store: listing,
paging and the error codes are the service's own, which a fake would have to
rebuild. Buckets are seeded with the key layouts the house uses — daily
Parquet files per stream in the cold archive, base backups and WAL segments
for a database. Uploads are a few milliseconds apart, so "newest" is the
upload order.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import boto3
import httpx
import pytest
import pytest_asyncio
from botocore.config import Config
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from lares_mcp_bridge.config import Settings
from lares_mcp_bridge.tools import s3

from .support import GATEWAY_KEY

RUSTFS_IMAGE = "rustfs/rustfs:1.0.0"
ACCESS_KEY = "lares-test-access"
SECRET_KEY = "lares-test-secret-key"

# Seeded oldest first; each key's size is its index + 1 KiB.
ARCHIVE = [
    "influxdb/2026/04/17/daily.parquet",
    "knx/2026/10/01/daily.parquet",
    "knx/2026/10/02/daily.parquet",
    "ems_esp/2026/10/02/daily.parquet",
    "knx/2026/10/03/daily.parquet",
]
BACKUPS = [
    "timescaledb-db/base/20261002T000000/data.tar",
    "timescaledb-db/wals/0000000100000001/000000010000000100000001",
    "timescaledb-db/base/20261003T000000/data.tar",
]


@pytest.fixture(scope="module")
def endpoint() -> Iterator[str]:
    container = (
        DockerContainer(RUSTFS_IMAGE)
        .with_env("RUSTFS_ACCESS_KEY", ACCESS_KEY)
        .with_env("RUSTFS_SECRET_KEY", SECRET_KEY)
        .with_exposed_ports(9000)
    )
    container.start()
    try:
        wait_for_logs(container, "Starting")
        url = f"http://{container.get_container_host_ip()}:{container.get_exposed_port(9000)}"
        for _ in range(60):
            try:
                if httpx.get(f"{url}/health", timeout=1).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.5)
        _seed(url)
        yield url
    finally:
        container.stop()


def _seed(url: str) -> None:
    admin = boto3.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )
    for bucket, keys in (("nats-archive", ARCHIVE), ("timescaledb-backups", BACKUPS)):
        admin.create_bucket(Bucket=bucket)
        for index, key in enumerate(keys):
            admin.put_object(Bucket=bucket, Key=key, Body=b"x" * (1024 * (index + 1)))
            time.sleep(0.01)
    admin.create_bucket(Bucket="wiki-js-backups")


def _settings(
    endpoint_url: str | None = None,
    access_key_file: str | None = None,
    secret_key_file: str | None = None,
) -> Settings:
    return Settings(
        db_host="localhost",
        db_name="x",
        db_username="x",
        db_password="x",
        gateway_key=GATEWAY_KEY,
        nats_enabled=False,
        s3_endpoint_url=endpoint_url,
        s3_access_key_file=access_key_file,
        s3_secret_key_file=secret_key_file,
    )


@pytest.fixture
def key_files(tmp_path: Path) -> tuple[str, str]:
    access, secret = tmp_path / "access-key", tmp_path / "secret-key"
    access.write_text(
        f"{ACCESS_KEY}\n", encoding="utf-8"
    )  # trailing newline, like a mounted Secret
    secret.write_text(f"{SECRET_KEY}\n", encoding="utf-8")
    return str(access), str(secret)


@pytest_asyncio.fixture
async def store(endpoint: str, key_files: tuple[str, str]) -> AsyncIterator[None]:
    await s3.init(_settings(endpoint, *key_files))
    try:
        yield
    finally:
        await s3.close()


def _by_name(rows: list[dict[str, Any]], field: str = "name") -> dict[str, dict[str, Any]]:
    return {row[field]: row for row in rows}


# ---------------------------------------------------------------- settings and lifecycle


def test_s3_settings_must_be_set_together(key_files: tuple[str, str]) -> None:
    message = "MCP_S3_ENDPOINT_URL, MCP_S3_ACCESS_KEY_FILE and MCP_S3_SECRET_KEY_FILE"
    with pytest.raises(ValueError, match=message):
        _settings(endpoint_url="http://rustfs.test:9000")
    with pytest.raises(ValueError, match=message):
        _settings("http://rustfs.test:9000", key_files[0])


def test_s3_enabled_only_when_all_set(key_files: tuple[str, str]) -> None:
    assert _settings().s3_enabled is False
    assert _settings("http://rustfs.test:9000", *key_files).s3_enabled


async def test_tools_refuse_when_not_initialised() -> None:
    with pytest.raises(RuntimeError, match="MCP_S3_ENDPOINT_URL"):
        await s3.list_s3_buckets()


async def test_init_rejects_an_empty_key_file(tmp_path: Path, key_files: tuple[str, str]) -> None:
    empty = tmp_path / "empty"
    empty.write_text("\n", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        await s3.init(_settings("http://rustfs.test:9000", key_files[0], str(empty)))


# ---------------------------------------------------------------- list_s3_buckets


async def test_buckets_with_their_size_and_newest_object(store: None) -> None:
    out = await s3.list_s3_buckets()

    buckets = _by_name(out["buckets"])
    assert list(buckets) == ["nats-archive", "timescaledb-backups", "wiki-js-backups"]
    archive = buckets["nats-archive"]
    assert archive["object_count"] == 5
    assert archive["size_bytes"] == 1024 * (1 + 2 + 3 + 4 + 5)
    # Newest by time, not by key: lexicographic order would name `knx/…/03`
    # last too, so the backups prove it below.
    assert archive["newest_key"] == "knx/2026/10/03/daily.parquet"
    assert archive["newest_at"] > archive["oldest_at"]
    assert archive["created_at"]
    # The base backup was the last upload, though `wals/` sorts after `base/`.
    assert buckets["timescaledb-backups"]["newest_key"] == BACKUPS[-1]
    # An empty bucket says so, with no times at all.
    assert buckets["wiki-js-backups"] == {
        "name": "wiki-js-backups",
        "created_at": buckets["wiki-js-backups"]["created_at"],
        "object_count": 0,
        "size_bytes": 0,
        "newest_key": None,
        "newest_at": None,
        "oldest_at": None,
    }


# ---------------------------------------------------------------- list_s3_objects


async def test_objects_newest_first_with_one_level_of_children(store: None) -> None:
    out = await s3.list_s3_objects("nats-archive", limit=2)

    assert out["bucket"] == "nats-archive"
    assert out["prefix"] == ""
    assert out["object_count"] == 5
    assert out["truncated"] is True
    assert [obj["key"] for obj in out["objects"]] == [ARCHIVE[4], ARCHIVE[3]]
    assert out["objects"][0]["size_bytes"] == 5 * 1024
    # One row per stream, each with its own newest day: a stream that stopped
    # shows up as an old newest_at beside the others.
    children = _by_name(out["children"], "prefix")
    assert list(children) == ["ems_esp/", "influxdb/", "knx/"]
    assert children["knx/"]["object_count"] == 3
    assert children["knx/"]["newest_key"] == "knx/2026/10/03/daily.parquet"
    assert children["influxdb/"]["newest_key"] == "influxdb/2026/04/17/daily.parquet"
    assert children["influxdb/"]["newest_at"] < children["knx/"]["newest_at"]


async def test_a_prefix_narrows_the_listing_and_its_children(store: None) -> None:
    out = await s3.list_s3_objects("timescaledb-backups", prefix="timescaledb-db/base/")

    assert out["object_count"] == 2
    assert [child["prefix"] for child in out["children"]] == [
        "timescaledb-db/base/20261002T000000/",
        "timescaledb-db/base/20261003T000000/",
    ]
    assert out["objects"][0]["key"] == BACKUPS[2]
    assert out["truncated"] is False


async def test_an_empty_prefix_listing_is_empty_not_an_error(store: None) -> None:
    out = await s3.list_s3_objects("nats-archive", prefix="warp/")

    assert out["object_count"] == 0
    assert out["newest_at"] is None
    assert out["children"] == []
    assert out["objects"] == []


async def test_an_unknown_bucket_names_where_to_look(store: None) -> None:
    with pytest.raises(s3.S3Error, match="NoSuchBucket.*list_s3_buckets"):
        await s3.list_s3_objects("nope")


async def test_a_key_the_store_refuses_names_the_policy(
    endpoint: str, tmp_path: Path, key_files: tuple[str, str]
) -> None:
    wrong = tmp_path / "wrong-secret"
    wrong.write_text("not-the-secret-key", encoding="utf-8")
    await s3.init(_settings(endpoint, key_files[0], str(wrong)))
    try:
        with pytest.raises(s3.S3Error, match="SignatureDoesNotMatch|AccessDenied"):
            await s3.list_s3_buckets()
    finally:
        await s3.close()


async def test_object_limit_must_be_positive(store: None) -> None:
    with pytest.raises(ValueError, match="invalid_limit"):
        await s3.list_s3_objects("nats-archive", limit=0)
