"""HTTP contract tests for scheduler inspection and explicit controls."""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcClient, AsyncArcClient
from arc_client.exceptions import (
    ArcPermissionError,
    ArcServerError,
    ArcValidationError,
)


@pytest.mark.parametrize("async_mode", [False, True])
async def test_scheduler_status(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    expected = {
        "cq_scheduler": {"enabled": True, "job_count": 2},
        "retention_scheduler": {"enabled": False, "reason": "not configured"},
    }
    httpx_mock.add_response(json=expected)

    if async_mode:
        async with AsyncArcClient(host="h", token="t", max_retries=0) as client:
            result = await client.schedulers.status()
    else:
        with ArcClient(host="h", token="t", max_retries=0) as client:
            result = client.schedulers.status()

    assert result == expected
    request = httpx_mock.get_requests()[0]
    assert request.method == "GET"
    assert request.url.path == "/api/v1/schedulers/"


@pytest.mark.parametrize("async_mode", [False, True])
async def test_scheduler_controls(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(json={"message": "CQ scheduler reloaded", "job_count": 3})
    httpx_mock.add_response(json={"message": "Retention triggered successfully"})

    if async_mode:
        async with AsyncArcClient(host="h", token="t", max_retries=0) as client:
            reload_result = await client.schedulers.reload_continuous_queries()
            retention_result = await client.schedulers.trigger_retention(confirm=True)
    else:
        with ArcClient(host="h", token="t", max_retries=0) as client:
            reload_result = client.schedulers.reload_continuous_queries()
            retention_result = client.schedulers.trigger_retention(confirm=True)

    assert reload_result["job_count"] == 3
    assert retention_result["message"] == "Retention triggered successfully"
    requests = httpx_mock.get_requests()
    assert [(request.method, request.url.path) for request in requests] == [
        ("POST", "/api/v1/schedulers/cq/reload"),
        ("POST", "/api/v1/schedulers/retention/trigger"),
    ]


@pytest.mark.parametrize("async_mode", [False, True])
async def test_retention_trigger_requires_explicit_confirmation(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    if async_mode:
        async with AsyncArcClient(host="h", token="t", max_retries=0) as client:
            with pytest.raises(ArcValidationError, match="confirm=True"):
                await client.schedulers.trigger_retention()
    else:
        with ArcClient(host="h", token="t", max_retries=0) as client, pytest.raises(
            ArcValidationError, match="confirm=True"
        ):
            client.schedulers.trigger_retention()

    assert httpx_mock.get_requests() == []


@pytest.mark.parametrize("async_mode", [False, True])
async def test_scheduler_controls_preserve_permission_and_server_errors(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(status_code=403, json={"error": "Admin permission required"})
    httpx_mock.add_response(status_code=503, json={"error": "Retention scheduler is not running"})

    if async_mode:
        async with AsyncArcClient(host="h", token="t", max_retries=0) as client:
            with pytest.raises(ArcPermissionError):
                await client.schedulers.reload_continuous_queries()
            with pytest.raises(ArcServerError) as error:
                await client.schedulers.trigger_retention(confirm=True)
    else:
        with ArcClient(host="h", token="t", max_retries=0) as client:
            with pytest.raises(ArcPermissionError):
                client.schedulers.reload_continuous_queries()
            with pytest.raises(ArcServerError) as error:
                client.schedulers.trigger_retention(confirm=True)

    assert error.value.status_code == 503


@pytest.mark.parametrize("async_mode", [False, True])
async def test_scheduler_license_required_503_remains_server_error(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(
        status_code=503, json={"error": "CQ scheduler is not running - enterprise license required"}
    )

    if async_mode:
        async with AsyncArcClient(host="h", token="t", max_retries=0) as client:
            with pytest.raises(ArcServerError) as error:
                await client.schedulers.reload_continuous_queries()
    else:
        with ArcClient(host="h", token="t", max_retries=0) as client, pytest.raises(
            ArcServerError
        ) as error:
            client.schedulers.reload_continuous_queries()

    assert error.value.status_code == 503
