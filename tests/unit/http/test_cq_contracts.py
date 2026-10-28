"""Continuous-query wire contracts shared by sync and async public clients."""

import inspect
import json
from contextlib import AsyncExitStack

import pytest

from arc_client import CLEAR, ArcClient, AsyncArcClient
from arc_client.exceptions import ArcPermissionError, ArcValidationError

DEFINITION = {
    "id": 7,
    "name": "rollup",
    "database": "metrics",
    "source_measurement": "cpu",
    "destination_measurement": "cpu_hour",
    "query": "SELECT avg(value) FROM metrics.cpu",
    "interval": "1h",
    "description": "keep",
    "tag_columns": ["host"],
    "retention_days": 30,
    "delete_source_after_days": 60,
    "is_active": True,
}


async def call(client, method, *args, **kwargs):
    result = getattr(client.continuous_queries, method)(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def async_mode(request):
    return request.param


async def enter_client(stack, async_mode):
    if async_mode:
        return await stack.enter_async_context(AsyncArcClient(token="test", max_retries=0))
    return stack.enter_context(ArcClient(token="test", max_retries=0))


@pytest.mark.parametrize("body", [None, [], {"queries": None}, {"queries": []}])
async def test_empty_list_shapes(httpx_mock, async_mode, body):
    httpx_mock.add_response(
        method="GET",
        content=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "list") == []


async def test_empty_execution_history(httpx_mock, async_mode):
    httpx_mock.add_response(method="GET", json={"executions": None})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "get_executions", 7) == []


async def test_create_flat_definition_and_tags(httpx_mock, async_mode):
    httpx_mock.add_response(method="POST", json=DEFINITION)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        args = {
            k: DEFINITION[k]
            for k in (
                "name",
                "database",
                "source_measurement",
                "destination_measurement",
                "query",
                "interval",
                "tag_columns",
            )
        }
        result = await call(client, "create", **args)
        assert result.query == DEFINITION["query"]
        assert result.tag_columns == ["host"]
    assert json.loads(httpx_mock.get_request().content)["tag_columns"] == ["host"]


async def test_sparse_update_no_get_preserves_false_zero_and_clear(httpx_mock, async_mode):
    httpx_mock.add_response(
        method="PUT", json={**DEFINITION, "is_active": False, "description": None}
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        result = await call(
            client,
            "update",
            7,
            is_active=False,
            description=CLEAR,
            retention_days=0,
            tag_columns=[],
            database=None,
        )
        assert not result.is_active
    requests = httpx_mock.get_requests()
    assert len(requests) == 1
    assert requests[0].method == "PUT"
    assert json.loads(requests[0].content) == {
        "is_active": False,
        "description": None,
        "retention_days": 0,
        "tag_columns": [],
    }


async def test_replace_sends_complete_definition_once(httpx_mock, async_mode):
    httpx_mock.add_response(method="PUT", json=DEFINITION)
    args = {k: v for k, v in DEFINITION.items() if k != "id"}
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        result = await call(client, "replace", 7, **args)
        assert result.tag_columns == ["host"]
    requests = httpx_mock.get_requests()
    assert len(requests) == 1
    assert json.loads(requests[0].content) == args


async def test_none_omits_existing_arguments(httpx_mock, async_mode):
    httpx_mock.add_response(method="PUT", json=DEFINITION)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        await call(client, "update", 7, description=None, retention_days=None)
    assert json.loads(httpx_mock.get_request().content) == {}


async def test_permissions_remain_typed(httpx_mock, async_mode):
    httpx_mock.add_response(status_code=403, json={"error": "Permission denied: read required"})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcPermissionError):
            await call(client, "list")


async def test_clear_nonnullable_rejected_before_request(async_mode):
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, "update", 7, name=CLEAR)
