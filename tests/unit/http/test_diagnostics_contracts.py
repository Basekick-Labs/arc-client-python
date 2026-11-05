"""Operational diagnostics wire contracts.

Arc sources: internal/api/server.go (readiness, metrics, logs) and
internal/api/debug.go (memory diagnostics).
"""

from __future__ import annotations

import inspect
from contextlib import AsyncExitStack
from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcClient, AsyncArcClient
from arc_client.exceptions import (
    ArcError,
    ArcPermissionError,
    ArcRateLimitError,
    ArcValidationError,
)
from arc_client.models import ReadyResponse

ROOT = "http://localhost:8000"


async def call(client: Any, method: str, *args: Any, **kwargs: Any) -> Any:
    result = getattr(client.diagnostics, method)(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def async_mode(request: Any) -> bool:
    return bool(request.param)


async def enter_client(stack: AsyncExitStack, async_mode: bool) -> Any:
    if async_mode:
        return await stack.enter_async_context(AsyncArcClient(token="t", max_retries=0))
    return stack.enter_context(ArcClient(token="t", max_retries=0))


# Readiness lives on the client (client.readiness / write_readiness) --------


async def probe(client: Any, write: bool) -> Any:
    result = client.write_readiness() if write else client.readiness()
    return await result if inspect.isawaitable(result) else result


@pytest.mark.parametrize(("write", "path"), [(False, "/ready"), (True, "/ready/write")])
async def test_readiness_ready(
    httpx_mock: HTTPXMock, async_mode: bool, write: bool, path: str
) -> None:
    body = {"status": "ready", "time": "2027-01-05T10:00:00Z", "uptime_sec": 12.5}
    httpx_mock.add_response(method="GET", url=f"{ROOT}{path}", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        result = await probe(client, write)
    assert isinstance(result, ReadyResponse)
    assert result.ready is True
    assert result.uptime_sec == 12.5


@pytest.mark.parametrize(
    ("write", "body"),
    [
        (False, {"status": "not_ready", "reason": "storage credentials expired", "tier": "cold"}),
        (True, {"status": "not_ready", "reason": "replaying write-ahead log"}),
        (True, {"status": "not_write_target", "reason": "route writes to a writer"}),
    ],
)
async def test_readiness_503_is_an_answer(
    httpx_mock: HTTPXMock, async_mode: bool, write: bool, body: dict[str, Any]
) -> None:
    httpx_mock.add_response(status_code=503, json={**body, "time": "2027-01-05T10:00:00Z"})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        result = await probe(client, write)
    assert result.ready is False
    assert result.status == body["status"]
    assert result.reason == body["reason"]
    assert result.tier == body.get("tier")


# Metrics -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("metrics", "/api/v1/metrics"),
        ("memory_metrics", "/api/v1/metrics/memory"),
        ("query_pool_metrics", "/api/v1/metrics/query-pool"),
        ("endpoint_metrics", "/api/v1/metrics/endpoints"),
        ("memstats", "/api/v1/debug/memstats"),
        ("duckdb_memory", "/api/v1/debug/duckdb-memory"),
    ],
)
async def test_raw_dict_reads(
    httpx_mock: HTTPXMock, async_mode: bool, method: str, path: str
) -> None:
    body = {"timestamp": "2027-01-05T10:00:00Z", "value": {"nested": 1}}
    httpx_mock.add_response(method="GET", url=f"{ROOT}{path}", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, method) == body


async def test_raw_dict_rejects_non_object(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(method="GET", url=f"{ROOT}/api/v1/metrics", json=[1, 2])
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcError):
            await call(client, "metrics")


async def test_timeseries(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"type": "api", "duration_minutes": 15, "points_count": 0, "data": []}
    httpx_mock.add_response(
        method="GET",
        url=f"{ROOT}/api/v1/metrics/timeseries/api?duration_minutes=15",
        json=body,
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "timeseries", "api", duration_minutes=15) == body


async def test_timeseries_default_window(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(
        method="GET", url=f"{ROOT}/api/v1/metrics/timeseries/system", json={"data": []}
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        await call(client, "timeseries", "system")


@pytest.mark.parametrize(
    ("args", "kwargs"),
    [
        (("disk",), {}),
        (("api",), {"duration_minutes": 0}),
        (("api",), {"duration_minutes": 1441}),
        (("api",), {"duration_minutes": True}),
    ],
)
async def test_timeseries_validation(
    httpx_mock: HTTPXMock, async_mode: bool, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> None:
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, "timeseries", *args, **kwargs)
    assert httpx_mock.get_requests() == []


async def test_prometheus_text(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    text = "# HELP arc_up 1\narc_up 1\n"
    httpx_mock.add_response(method="GET", url=f"{ROOT}/metrics", text=text)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "prometheus") == text


# Logs ----------------------------------------------------------------------


async def test_logs_params(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"count": 0, "limit": 50, "level_filter": "warn", "since_minutes": 10, "logs": []}
    httpx_mock.add_response(
        method="GET",
        url=f"{ROOT}/api/v1/logs?limit=50&level=warn&since_minutes=10",
        json=body,
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "logs", limit=50, level="warn", since_minutes=10) == body


async def test_logs_defaults_send_no_params(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(method="GET", url=f"{ROOT}/api/v1/logs", json={"logs": []})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        await call(client, "logs")


@pytest.mark.parametrize(
    "kwargs",
    [{"limit": 0}, {"limit": 1001}, {"since_minutes": 0}, {"since_minutes": 1441}],
)
async def test_logs_validation(
    httpx_mock: HTTPXMock, async_mode: bool, kwargs: dict[str, Any]
) -> None:
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, "logs", **kwargs)
    assert httpx_mock.get_requests() == []


@pytest.mark.parametrize("method", ["logs", "memstats", "duckdb_memory"])
async def test_admin_routes_refuse_non_admin(
    httpx_mock: HTTPXMock, async_mode: bool, method: str
) -> None:
    httpx_mock.add_response(status_code=403, json={"error": "Admin permission required"})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcPermissionError):
            await call(client, method)


# free-os-memory ------------------------------------------------------------


async def test_free_os_memory_requires_confirm(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, "free_os_memory")
    assert httpx_mock.get_requests() == []


async def test_free_os_memory(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"duration_ms": 1.5, "heap_released_delta": 4096}
    httpx_mock.add_response(method="POST", url=f"{ROOT}/api/v1/debug/free-os-memory", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "free_os_memory", confirm=True) == body


async def test_free_os_memory_throttled(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"error": "throttled", "retry_after_seconds": 12}
    httpx_mock.add_response(method="POST", status_code=429, json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcRateLimitError) as info:
            await call(client, "free_os_memory", confirm=True)
    assert info.value.response_body == body
    assert len(httpx_mock.get_requests()) == 1
