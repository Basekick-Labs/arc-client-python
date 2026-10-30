"""Retention wire contracts exercised through both public clients."""

import inspect
import json
from contextlib import AsyncExitStack

import pytest

from arc_client import CLEAR, ArcClient, AsyncArcClient
from arc_client.exceptions import ArcError, ArcPermissionError, ArcValidationError

DEFINITION = {
    "id": 7,
    "name": "cleanup",
    "database": "metrics",
    "measurement": "cpu",
    "retention_days": 30,
    "buffer_days": 7,
    "is_active": True,
}
REPLACEMENT = {key: value for key, value in DEFINITION.items() if key != "id"}


async def call(client, method, *args, **kwargs):
    result = getattr(client.retention, method)(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def async_mode(request):
    return request.param


async def enter_client(stack, async_mode):
    if async_mode:
        return await stack.enter_async_context(AsyncArcClient(token="test", max_retries=0))
    return stack.enter_context(ArcClient(token="test", max_retries=0))


@pytest.mark.parametrize("body", [None, [], {"policies": None}, {"policies": []}])
async def test_empty_list_shapes(httpx_mock, async_mode, body):
    httpx_mock.add_response(content=json.dumps(body).encode())
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "list") == []


@pytest.mark.parametrize("body", [[DEFINITION], {"policies": [DEFINITION]}])
async def test_populated_list_shapes(httpx_mock, async_mode, body):
    httpx_mock.add_response(json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert (await call(client, "list"))[0].measurement == "cpu"


@pytest.mark.parametrize("body", [False, 0, "", {}, {"policies": False}, {"policies": {}}])
async def test_malformed_lists_rejected(httpx_mock, async_mode, body):
    httpx_mock.add_response(json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcError):
            await call(client, "list")


@pytest.mark.parametrize("executions", [None, []])
async def test_empty_history(httpx_mock, async_mode, executions):
    httpx_mock.add_response(json={"executions": executions})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "get_executions", 7, limit=3) == []
    assert httpx_mock.get_request().url.params["limit"] == "3"


@pytest.mark.parametrize("executions", [False, 0, "", {}])
async def test_malformed_history_rejected(httpx_mock, async_mode, executions):
    httpx_mock.add_response(json={"executions": executions})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcError):
            await call(client, "get_executions", 7)


@pytest.mark.parametrize("measurement", [CLEAR, ""])
async def test_sparse_update_single_put(httpx_mock, async_mode, measurement):
    httpx_mock.add_response(method="PUT", json={**DEFINITION, "is_active": False})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        result = await call(
            client,
            "update",
            7,
            name="",
            retention_days=0,
            measurement=measurement,
            buffer_days=0,
            is_active=False,
            database="other",
        )
        assert result.is_active is False
    requests = httpx_mock.get_requests()
    assert len(requests) == 1
    assert requests[0].method == "PUT"
    assert requests[0].url.path == "/api/v1/retention/7"
    assert json.loads(requests[0].content) == {
        "name": "",
        "retention_days": 0,
        "measurement": None if measurement is CLEAR else "",
        "buffer_days": 0,
        "is_active": False,
        "database": "other",
    }


async def test_none_omits_and_positional_arguments_stay_compatible(httpx_mock, async_mode):
    httpx_mock.add_response(method="PUT", json={"policy": DEFINITION})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        await call(client, "update", 7, None, None, None, None, None, database=None)
    assert json.loads(httpx_mock.get_request().content) == {}


@pytest.mark.parametrize(
    "field", ["name", "database", "retention_days", "buffer_days", "is_active"]
)
async def test_clear_nonnullable_rejected_before_http(httpx_mock, async_mode, field):
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, "update", 7, **{field: CLEAR})
    assert httpx_mock.get_requests() == []


@pytest.mark.parametrize("measurement", [None, "cpu"])
async def test_replace_complete_single_put(httpx_mock, async_mode, measurement):
    values = {**REPLACEMENT, "measurement": measurement, "buffer_days": 0, "is_active": False}
    httpx_mock.add_response(method="PUT", json={"id": 7, **values})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        result = await call(client, "replace", 7, **values)
        assert result.measurement == measurement
    requests = httpx_mock.get_requests()
    assert len(requests) == 1
    assert requests[0].method == "PUT"
    assert json.loads(requests[0].content) == values


@pytest.mark.parametrize("field", list(REPLACEMENT))
async def test_replace_requires_every_field(httpx_mock, async_mode, field):
    values = {key: value for key, value in REPLACEMENT.items() if key != field}
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(TypeError):
            await call(client, "replace", 7, **values)
    assert httpx_mock.get_requests() == []


@pytest.mark.parametrize(
    "method,args,kwargs",
    [
        ("create", (), REPLACEMENT),
        ("get", (7,), {}),
        ("list", (), {}),
        ("update", (7,), {"is_active": False}),
        ("replace", (7,), REPLACEMENT),
        ("delete", (7,), {}),
        ("execute", (7,), {}),
        ("get_executions", (7,), {}),
    ],
)
async def test_permissions_remain_typed_without_fallback(
    httpx_mock, async_mode, method, args, kwargs
):
    httpx_mock.add_response(status_code=403, json={"error": "Permission denied: read required"})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcPermissionError):
            await call(client, method, *args, **kwargs)
    assert len(httpx_mock.get_requests()) == 1


async def test_execution_diagnostics_and_null_measurements(httpx_mock, async_mode):
    diagnostics = {
        "skipped_files": 2,
        "skipped_reason": "unaddressable files",
        "cold_files_deleted": 3,
        "cold_rows_deleted": 42,
        "cold_files_skipped": 4,
        "cold_skipped_reason": "cold tier unavailable",
    }
    httpx_mock.add_response(
        json={
            "policy_id": 7,
            "policy_name": "cleanup",
            "affected_measurements": None,
            "dry_run": True,
            **diagnostics,
        }
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        result = await call(client, "execute", 7)
        assert result.affected_measurements == []
        assert {field: getattr(result, field) for field in diagnostics} == diagnostics
    assert json.loads(httpx_mock.get_request().content) == {"dry_run": True, "confirm": False}


async def test_older_execution_defaults(httpx_mock, async_mode):
    httpx_mock.add_response(json={"policy_id": 7, "policy_name": "cleanup"})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        result = await call(client, "execute", 7)
        assert result.skipped_files == result.cold_files_deleted == result.cold_rows_deleted == 0
        assert result.cold_files_skipped == 0
        assert result.skipped_reason == result.cold_skipped_reason == ""


@pytest.mark.parametrize("measurements", [False, 0, "", {}])
async def test_malformed_measurements_rejected(httpx_mock, async_mode, measurements):
    httpx_mock.add_response(
        json={
            "policy_id": 7,
            "policy_name": "cleanup",
            "affected_measurements": measurements,
        }
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcError):
            await call(client, "execute", 7)
