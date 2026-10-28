"""Opt-in compatibility checks against isolated released and partial-PUT servers."""

from __future__ import annotations

import inspect
import os
import time
import uuid
from contextlib import AsyncExitStack
from urllib.parse import urlparse

import pytest

from arc_client import CLEAR, ArcClient, AsyncArcClient
from arc_client.exceptions import ArcError

SERVERS = [
    ("release", os.environ.get("ARC_COMPAT_RELEASE_URL", "")),
    ("main", os.environ.get("ARC_COMPAT_MAIN_URL", "")),
]


async def result(value):
    return await value if inspect.isawaitable(value) else value


@pytest.fixture(params=SERVERS, ids=["released-full-PUT", "main-partial-PUT"])
def target(request):
    name, url = request.param
    if not url:
        pytest.skip("Set both ARC_COMPAT_RELEASE_URL and ARC_COMPAT_MAIN_URL for Docker checks")
    return name, urlparse(url)


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def async_mode(request):
    return request.param


async def client_for(stack, target, async_mode, token=None):
    _, url = target
    kwargs = {
        "host": url.hostname,
        "port": url.port or 8000,
        "token": token if token is not None else os.environ["ARC_TEST_TOKEN"],
        "max_retries": 0,
    }
    if async_mode:
        return await stack.enter_async_context(AsyncArcClient(**kwargs))
    return stack.enter_context(ArcClient(**kwargs))


async def test_retention_lifecycle(target, async_mode):
    name = "sdkc_" + uuid.uuid4().hex[:10]
    async with AsyncExitStack() as stack:
        c = await client_for(stack, target, async_mode)
        policy = await result(
            c.retention.create(
                name=name,
                database="default",
                retention_days=30,
                measurement="sdkc_cpu",
                buffer_days=0,
                is_active=False,
            )
        )
        try:
            assert policy.measurement == "sdkc_cpu" and not policy.is_active
            assert any(p.id == policy.id for p in await result(c.retention.list()))
            assert await result(c.retention.get_executions(policy.id)) == []
            replaced = await result(
                c.retention.replace(
                    policy.id,
                    name=name,
                    database="default",
                    retention_days=60,
                    measurement="sdkc_cpu",
                    buffer_days=0,
                    is_active=False,
                )
            )
            assert replaced.retention_days == 60 and replaced.buffer_days == 0
            if target[0] == "main":
                patched = await result(
                    c.retention.update(policy.id, measurement=CLEAR, is_active=False)
                )
                assert patched.measurement is None and patched.retention_days == 60
            else:
                with pytest.raises(ArcError):
                    await result(c.retention.update(policy.id, retention_days=90))
                unchanged = await result(c.retention.get(policy.id))
                assert unchanged.retention_days == 60 and unchanged.measurement == "sdkc_cpu"
        finally:
            await result(c.retention.delete(policy.id))


async def test_cq_lifecycle(target, async_mode):
    name = "sdkc_" + uuid.uuid4().hex[:10]
    sql = (
        "SELECT time_bucket(INTERVAL '1 hour', time) AS time, host, avg(value) AS avg_value "
        "FROM default.sdkc_cpu WHERE time >= {start_time} AND time < {end_time} "
        "GROUP BY time, host"
    )
    args = {
        "name": name,
        "database": "default",
        "source_measurement": "sdkc_cpu",
        "destination_measurement": "sdkc_rollup",
        "query": sql,
        "interval": "1h",
        "description": "keep",
        "retention_days": 30,
        "delete_source_after_days": 60,
        "is_active": False,
        "tag_columns": ["host"],
    }
    async with AsyncExitStack() as stack:
        c = await client_for(stack, target, async_mode)
        cq = await result(c.continuous_queries.create(**args))
        try:
            assert cq.query == sql and cq.tag_columns == ["host"]
            assert any(q.id == cq.id for q in await result(c.continuous_queries.list()))
            assert await result(c.continuous_queries.get_executions(cq.id)) == []
            args["interval"] = "30m"
            replaced = await result(c.continuous_queries.replace(cq.id, **args))
            assert replaced.interval == "30m" and replaced.tag_columns == ["host"]
            if target[0] == "main":
                patched = await result(
                    c.continuous_queries.update(
                        cq.id, description=CLEAR, tag_columns=CLEAR, is_active=False
                    )
                )
                assert patched.description is None and patched.tag_columns is None
                assert patched.query == sql and patched.interval == "30m"
            else:
                with pytest.raises(ArcError):
                    await result(c.continuous_queries.update(cq.id, interval="15m"))
                unchanged = await result(c.continuous_queries.get(cq.id))
                assert unchanged.interval == "30m" and unchanged.query == sql
        finally:
            await result(c.continuous_queries.delete(cq.id))


async def test_json_msgpack_integer_null_fidelity(target, async_mode):
    sql = (
        "SELECT 9223372036854775807::BIGINT AS hi, "
        "CAST('-9223372036854775808' AS BIGINT) AS lo, NULL::BIGINT AS empty"
    )
    async with AsyncExitStack() as stack:
        c = await client_for(stack, target, async_mode)
        json_result = await result(c.query.query(sql))
        packed = await result(c.query.query_msgpack(sql))
        assert (
            json_result.data == packed.data == [[9223372036854775807, -9223372036854775808, None]]
        )
        assert not json_result.rows_capped and json_result.row_cap is None
        assert not packed.rows_capped and packed.row_cap is None


async def test_columnar_ingest_query_and_delete_dry_run(target, async_mode):
    measurement = "sdkc_" + uuid.uuid4().hex[:10]
    stamp = int(time.time() * 1_000_000)
    async with AsyncExitStack() as stack:
        c = await client_for(stack, target, async_mode)
        await result(
            c.write.write_columnar(
                measurement, {"time": [stamp, stamp + 1], "value": [1, 2], "host": ["a", "b"]}
            )
        )
        rows = None
        # Arc acknowledges ingestion before its default five-second disk flush.
        # Wait for visibility without weakening the row-integrity assertion.
        for _ in range(120):
            try:
                rows = await result(
                    c.query.query(f"SELECT value FROM {measurement} ORDER BY value")
                )
                if rows.data == [[1], [2]]:
                    break
            except ArcError:
                pass
            await __import__("asyncio").sleep(0.1)
        assert rows is not None and rows.data == [[1], [2]]
        dry = await result(c.delete.delete("default", measurement, "value < 0", dry_run=True))
        assert dry.dry_run and dry.deleted_count == 0
        assert dry.failed_files == []


async def test_auth_failure_retains_body(target, async_mode):
    async with AsyncExitStack() as stack:
        c = await client_for(stack, target, async_mode, token="invalid-local-test-token")
        with pytest.raises(ArcError) as failure:
            await result(c.retention.list())
        assert isinstance(failure.value.response_body, dict)
        assert "error" in failure.value.response_body
