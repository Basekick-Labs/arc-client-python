"""Structured HTTP diagnostics survive existing public exception wrappers."""

import inspect
from contextlib import AsyncExitStack

import pytest

from arc_client import ArcClient, AsyncArcClient
from arc_client.exceptions import ArcError


@pytest.mark.parametrize("async_mode", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("operation", ["query", "write", "cq-execute"])
async def test_wrapper_retains_response_body(httpx_mock, async_mode, operation):
    body = {"error": "Permission denied: read required", "detail": "server diagnostic"}
    httpx_mock.add_response(status_code=403, json=body)
    async with AsyncExitStack() as stack:
        if async_mode:
            client = await stack.enter_async_context(AsyncArcClient(max_retries=0))
        else:
            client = stack.enter_context(ArcClient(max_retries=0))
        with pytest.raises(ArcError) as failure:
            if operation == "query":
                value = client.query.query("SELECT 1")
            elif operation == "write":
                value = client.write.write_line_protocol("cpu value=1i")
            else:
                value = client.continuous_queries.execute(1)
            if inspect.isawaitable(value):
                await value
        assert failure.value.response_body == body


def test_error_body_handles_cause_cycle():
    first, second = ArcError("first"), ArcError("second")
    first.__cause__ = second
    second.__cause__ = first
    assert first.response_body is None
