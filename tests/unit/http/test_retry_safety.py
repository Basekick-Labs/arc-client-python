"""Mutation outcomes must not be hidden by automatically replaying writes."""

import inspect
from contextlib import AsyncExitStack

import httpx
import pytest

from arc_client import ArcClient, AsyncArcClient
from arc_client.exceptions import ArcError


@pytest.mark.parametrize("async_mode", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    "error", [httpx.ReadTimeout, httpx.WriteTimeout, httpx.RemoteProtocolError]
)
async def test_uncertain_write_is_sent_once(httpx_mock, async_mode, error):
    calls = []

    def failed_write(request):
        calls.append(request.content)
        raise error("unknown write outcome")

    httpx_mock.add_callback(failed_write, is_reusable=True)
    async with AsyncExitStack() as stack:
        if async_mode:
            client = await stack.enter_async_context(AsyncArcClient(max_retries=3, retry_backoff=0))
        else:
            client = stack.enter_context(ArcClient(max_retries=3, retry_backoff=0))
        with pytest.raises(ArcError):
            result = client.write.write_line_protocol("cpu,host=a value=1i")
            if inspect.isawaitable(result):
                await result
    assert len(calls) == 1


@pytest.mark.parametrize("async_mode", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("status", [429, 503])
async def test_write_http_failure_is_not_replayed(httpx_mock, async_mode, status):
    httpx_mock.add_response(
        status_code=status, json={"error": "write outcome unknown"}, is_reusable=True
    )
    async with AsyncExitStack() as stack:
        if async_mode:
            client = await stack.enter_async_context(AsyncArcClient(max_retries=3, retry_backoff=0))
        else:
            client = stack.enter_context(ArcClient(max_retries=3, retry_backoff=0))
        with pytest.raises(ArcError):
            result = client.write.write_line_protocol("cpu,host=a value=1i")
            if inspect.isawaitable(result):
                await result
    assert len(httpx_mock.get_requests()) == 1


@pytest.mark.parametrize("async_mode", [False, True], ids=["sync", "async"])
async def test_presend_connection_failure_can_retry_write(httpx_mock, async_mode):
    httpx_mock.add_exception(httpx.ConnectError("connection not established"))
    httpx_mock.add_response(json={"success": True})
    async with AsyncExitStack() as stack:
        if async_mode:
            client = await stack.enter_async_context(AsyncArcClient(max_retries=1, retry_backoff=0))
        else:
            client = stack.enter_context(ArcClient(max_retries=1, retry_backoff=0))
        result = client.write.write_line_protocol("cpu,host=a value=1i")
        if inspect.isawaitable(result):
            await result
    assert len(httpx_mock.get_requests()) == 2
