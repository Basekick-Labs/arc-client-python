"""Reconciliation wire contracts (Arc internal/api/reconciliation.go)."""

from __future__ import annotations

import inspect
from contextlib import AsyncExitStack
from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcClient, AsyncArcClient
from arc_client.exceptions import (
    ArcLicenseError,
    ArcNotFoundError,
    ArcPermissionError,
    ArcServerError,
    ArcValidationError,
)
from arc_client.models.reconciliation import ReconciliationRun

BASE = "http://localhost:8000/api/v1/reconciliation"
RUN = {
    "id": "run-1",
    "started_at": "2027-01-05T10:00:00Z",
    "finished_at": "0001-01-01T00:00:00Z",
    "dry_run": True,
    "backend_kind": "s3",
    "role": "compactor",
    "manifest_file_count": 10,
    "storage_file_count": 12,
    "orphan_manifest_count": 0,
    "orphan_storage_count": 2,
    "manifest_deletes": 0,
    "storage_deletes": 0,
    "skipped_grace": 1,
    "skipped_recheck": 0,
    "skipped_pending": 0,
    "skipped_transient": 0,
    "skipped_invalid_path": 0,
    "orphan_storage_sample": ["db/cpu/a.parquet"],
    "aborted": False,
    "manifest_sweep_held": False,
    "cap_hit": False,
    "walk_partial": False,
    "future_counter": 3,
}


async def call(client: Any, method: str, *args: Any, **kwargs: Any) -> Any:
    result = getattr(client.reconciliation, method)(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def async_mode(request: Any) -> bool:
    return bool(request.param)


async def enter_client(stack: AsyncExitStack, async_mode: bool) -> Any:
    if async_mode:
        return await stack.enter_async_context(AsyncArcClient(token="t", max_retries=0))
    return stack.enter_context(ArcClient(token="t", max_retries=0))


async def test_status_returns_raw_dict(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"enabled": True, "running": True, "schedule": "0 */6 * * *", "last_run": RUN}
    httpx_mock.add_response(method="GET", url=f"{BASE}/status", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "status") == body


async def test_status_disabled_is_not_an_error(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"enabled": False, "reason": "Reconciliation requires Enterprise clustering"}
    httpx_mock.add_response(method="GET", url=f"{BASE}/status", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert (await call(client, "status"))["enabled"] is False


async def test_trigger_defaults_to_dry_run(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(method="POST", url=f"{BASE}/trigger?dry_run=true", json={"run": RUN})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        run = await call(client, "trigger")
    assert isinstance(run, ReconciliationRun)
    assert run.id == "run-1"
    assert run.dry_run is True
    assert run.orphan_storage_sample == ["db/cpu/a.parquet"]
    assert run.finished_at is not None and run.finished_at.year == 1
    assert run.model_extra == {"future_counter": 3}


async def test_act_mode_requires_confirm(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, "trigger", dry_run=False)
    assert httpx_mock.get_requests() == []


async def test_act_mode_sends_both_flags(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/trigger?dry_run=false&act=true",
        json={"run": {**RUN, "dry_run": False, "storage_deletes": 2}},
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        run = await call(client, "trigger", dry_run=False, confirm=True)
    assert run.dry_run is False
    assert run.storage_deletes == 2


async def test_confirm_alone_stays_dry_run(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(method="POST", url=f"{BASE}/trigger?dry_run=true", json={"run": RUN})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert (await call(client, "trigger", confirm=True)).dry_run is True


@pytest.mark.parametrize(
    ("status", "body", "exc"),
    [
        (409, {"error": "a reconciliation run is already in progress"}, ArcServerError),
        (422, {"error": "manifest too large", "run": RUN}, ArcServerError),
        (503, {"error": "reconciliation feature is disabled"}, ArcServerError),
        (403, {"error": "this node is not permitted to run reconciliation"}, ArcPermissionError),
        (
            403,
            {
                "error": "Reconciliation requires an enterprise license with the "
                "'clustering' feature enabled"
            },
            ArcLicenseError,
        ),
    ],
)
async def test_trigger_errors_keep_diagnostics(
    httpx_mock: HTTPXMock, async_mode: bool, status: int, body: dict[str, Any], exc: type
) -> None:
    httpx_mock.add_response(method="POST", status_code=status, json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(exc) as info:
            await call(client, "trigger")
    assert info.value.response_body == body
    if isinstance(info.value, ArcServerError):
        assert info.value.status_code == status


async def test_get_run(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(method="GET", url=f"{BASE}/runs/run-1", json=RUN)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        run = await call(client, "get_run", "run-1")
    assert run.id == "run-1"
    assert run.storage_file_count == 12


async def test_get_run_not_found(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/runs/gone",
        status_code=404,
        json={"error": "run not found in recent-runs buffer"},
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcNotFoundError):
            await call(client, "get_run", "gone")


async def test_get_run_requires_id(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, "get_run", "")


async def test_get_run_does_not_retry_permanent_503(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    # Found live: standalone Arc answers 503 "not running" with no Retry-After.
    httpx_mock.add_response(status_code=503, json={"error": "reconciliation is not running"})
    async with AsyncExitStack() as stack:
        client = (
            await stack.enter_async_context(AsyncArcClient(token="t", max_retries=3))
            if async_mode
            else stack.enter_context(ArcClient(token="t", max_retries=3))
        )
        with pytest.raises(ArcServerError):
            await call(client, "get_run", 7)
    assert len(httpx_mock.get_requests()) == 1
