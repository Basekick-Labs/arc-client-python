"""Auth token-cache wire contracts (Arc internal/api/auth_routes.go)."""

from __future__ import annotations

import inspect
from contextlib import AsyncExitStack
from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcClient, AsyncArcClient
from arc_client.exceptions import ArcError, ArcPermissionError

BASE = "http://localhost:8000/api/v1/auth/cache"
STATS = {
    "cache_size": 3,
    "max_cache_size": 1000,
    "cache_ttl_seconds": 30.0,
    "utilization_percent": 0.3,
    "total_requests": 10,
    "cache_hits": 7,
    "cache_misses": 3,
    "cache_evictions": 0,
    "hit_rate_percent": 70.0,
}


async def call(client: Any, method: str, *args: Any, **kwargs: Any) -> Any:
    result = getattr(client.auth, method)(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def async_mode(request: Any) -> bool:
    return bool(request.param)


async def enter_client(stack: AsyncExitStack, async_mode: bool) -> Any:
    if async_mode:
        return await stack.enter_async_context(AsyncArcClient(token="t", max_retries=0))
    return stack.enter_context(ArcClient(token="t", max_retries=0))


async def test_cache_stats_unwraps_stats(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/stats", json={"success": True, "stats": STATS}
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "cache_stats") == STATS


async def test_cache_stats_malformed(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(method="GET", url=f"{BASE}/stats", json={"success": True})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcError):
            await call(client, "cache_stats")


async def test_invalidate_cache(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"success": True, "message": "Cache invalidated successfully"}
    httpx_mock.add_response(method="POST", url=f"{BASE}/invalidate", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "invalidate_cache") == body
    assert httpx_mock.get_request().content == b""


@pytest.mark.parametrize("method", ["cache_stats", "invalidate_cache"])
async def test_non_admin_refused(httpx_mock: HTTPXMock, async_mode: bool, method: str) -> None:
    httpx_mock.add_response(status_code=403, json={"error": "Admin permission required"})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcPermissionError):
            await call(client, method)
