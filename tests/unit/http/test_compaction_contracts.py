"""Compaction wire contracts shared by sync and async public clients."""

from __future__ import annotations

import inspect
from contextlib import AsyncExitStack

import pytest

from arc_client import ArcClient, AsyncArcClient
from arc_client.exceptions import ArcNotFoundError, ArcPermissionError, ArcValidationError

BASE = "http://localhost:8000/api/v1/compaction"


async def call(client, method, *args, **kwargs):
    result = getattr(client.compaction, method)(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def async_mode(request):
    return request.param


async def enter_client(stack, async_mode):
    if async_mode:
        return await stack.enter_async_context(AsyncArcClient(token="t", max_retries=0))
    return stack.enter_context(ArcClient(token="t", max_retries=0))


@pytest.mark.parametrize(
    ("kwargs", "params"),
    [
        ({}, {}),
        ({"database": "db"}, {"database": "db"}),
        (
            {"tier": "hourly", "database": "db", "measurement": "cpu"},
            {"tier": "hourly", "database": "db", "measurement": "cpu"},
        ),
    ],
)
async def test_trigger_params(httpx_mock, async_mode, kwargs, params):
    httpx_mock.add_response(method="POST", json={"message": "ok", "cycle_id": 3})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert (await call(client, "trigger", **kwargs))["cycle_id"] == 3
    request = httpx_mock.get_request()
    assert request.url.path == "/api/v1/compaction/trigger"
    assert dict(request.url.params) == params


async def test_trigger_measurement_requires_database(httpx_mock, async_mode):
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, "trigger", measurement="cpu")
    assert httpx_mock.get_requests() == []


@pytest.mark.parametrize(
    ("kwargs", "params"),
    [({}, {}), ({"limit": 5}, {"limit": "5"}), ({"cycle_id": 7}, {"cycle_id": "7"})],
)
async def test_history_params(httpx_mock, async_mode, kwargs, params):
    httpx_mock.add_response(method="GET", json={"recent_jobs": [], "total_jobs": 0})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        await call(client, "history", **kwargs)
    assert dict(httpx_mock.get_request().url.params) == params


@pytest.mark.parametrize("bad", [0, -1, True])
async def test_cycle_id_must_be_positive(httpx_mock, async_mode, bad):
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, "history", cycle_id=bad)
        with pytest.raises(ArcValidationError):
            await call(client, "cycle", bad)
    assert httpx_mock.get_requests() == []


async def test_cycles_and_cycle(httpx_mock, async_mode):
    httpx_mock.add_response(method="GET", url=f"{BASE}/cycles?limit=2", json={"cycles": []})
    httpx_mock.add_response(method="GET", url=f"{BASE}/cycles/4", json={"cycle_id": 4})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert (await call(client, "cycles", limit=2)) == {"cycles": []}
        assert (await call(client, "cycle", 4))["cycle_id"] == 4


async def test_cycle_not_found_keeps_diagnostics(httpx_mock, async_mode):
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/cycles/99",
        status_code=404,
        json={"error": "no retained record", "cycle_id": 99, "lease_holder": "n2"},
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcNotFoundError):
            await call(client, "cycle", 99)


async def test_too_wide_list_and_clear(httpx_mock, async_mode):
    httpx_mock.add_response(method="GET", url=f"{BASE}/too-wide", json={"partitions": []})
    httpx_mock.add_response(method="DELETE", url=f"{BASE}/too-wide", json={"cleared": 3})
    httpx_mock.add_response(
        method="DELETE", url=f"{BASE}/too-wide?tier=daily&partition=db%2Fcpu", json={"cleared": 1}
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert (await call(client, "too_wide"))["partitions"] == []
        assert (await call(client, "clear_too_wide"))["cleared"] == 3
        result = await call(client, "clear_too_wide", tier="daily", partition="db/cpu")
        assert result["cleared"] == 1


async def test_reads_are_admin_only(httpx_mock, async_mode):
    httpx_mock.add_response(method="GET", status_code=403, json={"error": "admin required"})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcPermissionError):
            await call(client, "cycles")


async def test_trigger_wait_sends_a_duration_and_covers_it_with_the_timeout(httpx_mock, async_mode):
    httpx_mock.add_response(
        method="POST", json={"cycle_id": 1, "status": "completed", "boot_id": "ab12"}
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        result = await call(client, "trigger", database="db", wait=90)
    assert result["status"] == "completed"
    request = httpx_mock.get_request()
    assert dict(request.url.params) == {"database": "db", "wait": "90000ms"}
    # The request outlives the wait (Arc caps it at 10 minutes).
    assert request.extensions["timeout"]["read"] >= 120


async def test_trigger_wait_accepts_a_timedelta_and_caps_the_timeout(httpx_mock, async_mode):
    from datetime import timedelta

    httpx_mock.add_response(method="POST", json={"cycle_id": 1, "status": "running"})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        await call(client, "trigger", wait=timedelta(hours=1))
    request = httpx_mock.get_request()
    assert request.url.params["wait"] == "3600000ms"
    assert request.extensions["timeout"]["read"] == 630


@pytest.mark.parametrize("bad", [0, -5, True, "5m"])
async def test_trigger_wait_must_be_positive_seconds(httpx_mock, async_mode, bad):
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, "trigger", wait=bad)
    assert httpx_mock.get_requests() == []


async def test_cycle_and_history_send_boot_id(httpx_mock, async_mode):
    httpx_mock.add_response(method="GET", url=f"{BASE}/cycles/4?boot_id=ab12", json={"cycle_id": 4})
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/history?cycle_id=4&boot_id=ab12", json={"recent_jobs": []}
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert (await call(client, "cycle", 4, boot_id="ab12"))["cycle_id"] == 4
        await call(client, "history", cycle_id=4, boot_id="ab12")


async def test_cycle_from_another_process_raises_boot_mismatch(httpx_mock, async_mode):
    from arc_client import ArcCycleBootMismatchError

    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/cycles/1?boot_id=ab12",
        status_code=404,
        json={
            "error": "no record",
            "cycle_id": 1,
            "boot_id": "cd34",
            "boot_id_mismatch": True,
            "lease_holder": "n2",
        },
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcCycleBootMismatchError) as exc:
            await call(client, "cycle", 1, boot_id="ab12")
    err = exc.value
    assert isinstance(err, ArcNotFoundError)
    assert (err.cycle_id, err.boot_id, err.server_boot_id, err.lease_holder) == (
        1,
        "ab12",
        "cd34",
        "n2",
    )
    assert err.response_body["boot_id_mismatch"] is True


async def test_trigger_while_stopping_keeps_the_marker(httpx_mock, async_mode):
    from arc_client.exceptions import ArcServerError

    httpx_mock.add_response(
        method="POST",
        status_code=503,
        json={"error": "the server is shutting down", "stopping": True},
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcServerError) as exc:
            await call(client, "trigger")
    assert exc.value.response_body["stopping"] is True
