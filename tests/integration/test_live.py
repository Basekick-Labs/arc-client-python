"""Opt-in integration tests against a live Arc server.

These run only when ``ARC_TEST_URL`` (and usually ``ARC_TEST_TOKEN``) are set;
otherwise every test is skipped. They exercise the new 0.2.0 surface end-to-end.

Run with, e.g.::

    ARC_TEST_URL=http://localhost:8000 ARC_TEST_TOKEN=... uv run pytest tests/integration -v

The msgpack query test additionally requires the server to be built with the
``duckdb_arrow`` build tag; it is skipped (not failed) on a 501.
"""

from __future__ import annotations

import contextlib
import os
import time

import pytest

from arc_client import ArcClient
from arc_client.exceptions import ArcLicenseError, ArcNotSupportedError

pytestmark = pytest.mark.skipif(
    not os.environ.get("ARC_TEST_URL"),
    reason="Set ARC_TEST_URL (and ARC_TEST_TOKEN) to run live integration tests.",
)


def test_health_and_ready(sync_client: ArcClient) -> None:
    health = sync_client.health()
    assert health.status
    assert sync_client.ready() in (True, False)


def test_columnar_write_then_query_msgpack(sync_client: ArcClient) -> None:
    base = int(time.time() * 1_000_000)
    measurement = "sdk_it_cpu"
    sync_client.write.write_columnar(
        measurement,
        {"time": [base, base + 1_000_000], "host": ["a", "b"], "usage": [1.0, 2.0]},
    )
    # Give the server a moment to flush.
    time.sleep(1.0)

    try:
        result = sync_client.query.query_msgpack(f"SELECT * FROM {measurement} ORDER BY time")
    except ArcNotSupportedError:
        pytest.skip("Server built without the duckdb_arrow build tag")

    assert result.success
    assert "usage" in result.columns


def test_databases_lifecycle(sync_client: ArcClient) -> None:
    name = "sdk_it_db"
    # May already exist from a previous run; ignore and continue.
    with contextlib.suppress(Exception):
        sync_client.databases.create(name)

    listing = sync_client.databases.list()
    assert any(db.name == name for db in listing.databases)

    info = sync_client.databases.get(name)
    assert info.name == name


def test_import_csv(sync_client: ArcClient) -> None:
    csv = b"time,value\n1633046400000000,1.5\n1633046401000000,2.5\n"
    result = sync_client.import_.import_csv(
        csv, measurement="sdk_it_import", time_format="epoch_us"
    )
    assert result.rows_imported == 2


def test_enterprise_governance_usage_or_license_error(sync_client: ArcClient) -> None:
    # On a licensed server this returns usage; on OSS it should be a clean
    # license/permission error rather than a crash.
    try:
        sync_client.governance.list_policies()
    except ArcLicenseError:
        pytest.skip("Server has no query_governance license feature (expected on OSS)")
