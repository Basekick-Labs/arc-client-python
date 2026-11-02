"""allow_unreachable on row and database deletes, shared by sync and async clients."""

from __future__ import annotations

import inspect
import json
from contextlib import AsyncExitStack

import pytest

from arc_client import ArcClient, AsyncArcClient

DELETE_ARGS = {"database": "db", "measurement": "cpu", "where": "value < 0"}


async def call(obj, method, *args, **kwargs):
    result = getattr(obj, method)(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def async_mode(request):
    return request.param


async def enter_client(stack, async_mode):
    if async_mode:
        return await stack.enter_async_context(AsyncArcClient(token="t", max_retries=0))
    return stack.enter_context(ArcClient(token="t", max_retries=0))


@pytest.mark.parametrize("allow", [None, False, True])
async def test_row_delete_allow_unreachable(httpx_mock, async_mode, allow):
    httpx_mock.add_response(
        method="POST",
        json={"success": True, "unreachable_files": ["a.parquet"], "unflushed_nodes": ["w2"]},
    )
    kwargs = {} if allow is None else {"allow_unreachable": allow}
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        result = await call(
            client.delete, "delete", **DELETE_ARGS, dry_run=False, confirm=True, **kwargs
        )
    assert result.unreachable_files == ["a.parquet"]
    assert result.unflushed_nodes == ["w2"]
    expected = {**DELETE_ARGS, "dry_run": False, "confirm": True}
    if allow:
        expected["allow_unreachable"] = True
    assert json.loads(httpx_mock.get_request().content) == expected


@pytest.mark.parametrize("allow", [None, False, True])
async def test_database_delete_allow_unreachable(httpx_mock, async_mode, allow):
    httpx_mock.add_response(
        method="DELETE", json={"message": "deleted", "files_deleted": 2, "unflushed_nodes": ["w2"]}
    )
    kwargs = {} if allow is None else {"allow_unreachable": allow}
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        result = await call(client.databases, "delete", "db", confirm=True, **kwargs)
    assert result["unflushed_nodes"] == ["w2"]
    request = httpx_mock.get_request()
    assert request.url.path == "/api/v1/databases/db"
    expected = {"confirm": "true"}
    if allow:
        expected["allow_unreachable"] = "true"
    assert dict(request.url.params) == expected
