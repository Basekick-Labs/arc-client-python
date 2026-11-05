"""Server buffer flush wire contract.

Arc source: internal/api/lineprotocol.go (``Flush``, admin-gated).
"""

from __future__ import annotations

import inspect
from contextlib import AsyncExitStack
from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcClient, AsyncArcClient
from arc_client.exceptions import ArcPermissionError, ArcServerError

URL = "http://localhost:8000/api/v1/write/line-protocol/flush"


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def async_mode(request: Any) -> bool:
    return bool(request.param)


async def flush(async_mode: bool, **kwargs: Any) -> Any:
    kwargs.setdefault("max_retries", 0)
    async with AsyncExitStack() as stack:
        if async_mode:
            client: Any = await stack.enter_async_context(AsyncArcClient(token="t", **kwargs))
        else:
            client = stack.enter_context(ArcClient(token="t", **kwargs))
        result = client.write.flush_server_buffers()
        return await result if inspect.isawaitable(result) else result


async def test_flush_posts_and_returns_body(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"status": "success", "message": "All buffers flushed"}
    httpx_mock.add_response(method="POST", url=URL, json=body)

    assert await flush(async_mode) == body

    request = httpx_mock.get_request()
    assert request is not None
    assert request.headers["Authorization"] == "Bearer t"
    assert request.content == b""


async def test_flush_requires_admin(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(
        method="POST", url=URL, status_code=403, json={"error": "Permission denied: admin required"}
    )
    with pytest.raises(ArcPermissionError):
        await flush(async_mode)


async def test_flush_failure_is_not_retried(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(
        method="POST", url=URL, status_code=503, json={"error": "Flush failed: disk full"}
    )
    with pytest.raises(ArcServerError) as exc:
        await flush(async_mode, max_retries=3, retry_backoff=0.0)
    assert exc.value.status_code == 503
    assert "disk full" in str(exc.value)
    assert len(httpx_mock.get_requests()) == 1
