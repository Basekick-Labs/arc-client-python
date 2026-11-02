"""Backup wire contracts shared by sync and async public clients."""

from __future__ import annotations

import inspect
import json
from contextlib import AsyncExitStack

import pytest

from arc_client import ArcClient, AsyncArcClient
from arc_client.exceptions import ArcValidationError


async def call(client, method, *args, **kwargs):
    result = getattr(client.backup, method)(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def async_mode(request):
    return request.param


async def enter_client(stack, async_mode):
    if async_mode:
        return await stack.enter_async_context(AsyncArcClient(token="t", max_retries=0))
    return stack.enter_context(ArcClient(token="t", max_retries=0))


def body_of(request):
    return json.loads(request.content) if request.content else None


@pytest.mark.parametrize(
    ("kwargs", "body"),
    [
        ({}, None),
        ({"databases": ["b", "a"]}, {"databases": ["b", "a"]}),
        (
            {"databases": ("a",), "include_config": False},
            {"include_config": False, "databases": ["a"]},
        ),
    ],
)
async def test_create_scope(httpx_mock, async_mode, kwargs, body):
    httpx_mock.add_response(method="POST", status_code=202, json={"status": "running"})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        await call(client, "create", **kwargs)
    request = httpx_mock.get_request()
    assert request.url.path == "/api/v1/backup/"
    assert body_of(request) == body


@pytest.mark.parametrize("bad", [[], "metrics", [""]])
async def test_create_rejects_bad_scope(httpx_mock, async_mode, bad):
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, "create", databases=bad)
    assert httpx_mock.get_requests() == []


@pytest.mark.parametrize(
    ("kwargs", "params"),
    [
        ({}, {}),
        ({"include_foreign": False}, {}),
        ({"include_foreign": True}, {"include_foreign": "true"}),
    ],
)
async def test_list_include_foreign(httpx_mock, async_mode, kwargs, params):
    httpx_mock.add_response(method="GET", json={"backups": [], "count": 0, "filtered_foreign": 2})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        result = await call(client, "list", **kwargs)
    assert result["filtered_foreign"] == 2
    assert dict(httpx_mock.get_request().url.params) == params


@pytest.mark.parametrize(
    ("mode", "sent"), [(None, None), ("merge", "merge"), (" Replace ", "replace")]
)
async def test_restore_mode(httpx_mock, async_mode, mode, sent):
    httpx_mock.add_response(method="POST", status_code=202, json={"mode": sent or "merge"})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        await call(client, "restore", "b1", confirm=True, mode=mode)
    expected = {"backup_id": "b1", "confirm": True}
    if sent:
        expected["mode"] = sent
    assert body_of(httpx_mock.get_request()) == expected


async def test_restore_rejects_unknown_mode_and_missing_confirm(httpx_mock, async_mode):
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, "restore", "b1", confirm=True, mode="overwrite")
        with pytest.raises(ArcValidationError):
            await call(client, "restore", "b1", mode="merge")
    assert httpx_mock.get_requests() == []
