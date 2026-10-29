"""DELETE diagnostics remain available without changing partial success semantics."""

from __future__ import annotations

import json
from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcClient, AsyncArcClient
from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcServerError
from arc_client.http.async_http import AsyncHTTPClient
from arc_client.http.sync_http import SyncHTTPClient

ARRAY_FIELDS = (
    "files_processed",
    "failed_files",
    "unreachable_files",
    "unflushed_nodes",
    "cold_deleted_files",
    "cold_files",
)
DIAGNOSTICS = {
    "files_processed": ["rewritten.parquet"],
    "failed_files": ["failed.parquet"],
    "unreachable_files": ["missing.parquet"],
    "unflushed_nodes": ["writer-2"],
    "cold_deleted_files": ["cold-deleted.parquet"],
    "cold_files": ["cold-partial.parquet"],
    "cold_rows_not_deleted": 9,
    "cold_not_examined": "bucket unavailable",
}
DELETE_ARGS = {"database": "db", "measurement": "cpu", "where": "value < 0"}


def add_delete_response(httpx_mock: HTTPXMock, status: int, body: dict[str, Any]) -> None:
    httpx_mock.add_response(
        method="POST", url="http://h:8000/api/v1/delete", status_code=status, json=body
    )


def assert_diagnostics(result: Any, body: dict[str, Any]) -> None:
    for field, value in body.items():
        assert getattr(result, field) == value


@pytest.mark.parametrize("status", [200, 207])
async def test_delete_diagnostics_public_clients(httpx_mock: HTTPXMock, status: int) -> None:
    body = {
        "success": status == 200,
        "deleted_count": 4,
        "affected_files": 3,
        "rewritten_files": 1,
        "execution_time_ms": 2.5,
        "dry_run": False,
        "error": None if status == 200 else "partial delete",
        **DIAGNOSTICS,
    }
    add_delete_response(httpx_mock, status, body)
    with ArcClient(host="h", max_retries=0) as client:
        result = client.delete.delete(**DELETE_ARGS, dry_run=False, confirm=True)
    assert_diagnostics(result, body)

    add_delete_response(httpx_mock, status, body)
    async with AsyncArcClient(host="h", max_retries=0) as client:
        result = await client.delete.delete(**DELETE_ARGS, dry_run=False, confirm=True)
    assert_diagnostics(result, body)
    requests = httpx_mock.get_requests()
    assert len(requests) == 2
    for request in requests:
        assert json.loads(request.content) == {**DELETE_ARGS, "dry_run": False, "confirm": True}


@pytest.mark.parametrize("array_value", ["omitted", None, []], ids=["omitted", "null", "empty"])
async def test_delete_optional_arrays(httpx_mock: HTTPXMock, array_value: Any) -> None:
    body = {"success": True}
    if array_value != "omitted":
        body.update(dict.fromkeys(ARRAY_FIELDS, array_value))
    add_delete_response(httpx_mock, 200, body)
    with ArcClient(host="h", max_retries=0) as client:
        result = client.delete.delete(**DELETE_ARGS)
    for field in ARRAY_FIELDS:
        assert getattr(result, field) == []
    assert result.cold_rows_not_deleted == 0
    assert result.cold_not_examined == ""

    add_delete_response(httpx_mock, 200, body)
    async with AsyncArcClient(host="h", max_retries=0) as client:
        result = await client.delete.delete(**DELETE_ARGS)
    for field in ARRAY_FIELDS:
        assert getattr(result, field) == []
    assert result.cold_rows_not_deleted == 0
    assert result.cold_not_examined == ""


@pytest.mark.parametrize(
    "invalid",
    [
        {"failed_files": "file.parquet"},
        {"unflushed_nodes": [1]},
        {"cold_rows_not_deleted": "many"},
        {"cold_not_examined": []},
    ],
)
async def test_delete_invalid_diagnostics_fail(
    httpx_mock: HTTPXMock, invalid: dict[str, Any]
) -> None:
    body = {"success": False, **invalid}
    add_delete_response(httpx_mock, 207, body)
    with (
        ArcClient(host="h", max_retries=0) as client,
        pytest.raises(ArcError, match="Delete operation failed"),
    ):
        client.delete.delete(**DELETE_ARGS)
    add_delete_response(httpx_mock, 207, body)
    async with AsyncArcClient(host="h", max_retries=0) as client:
        with pytest.raises(ArcError, match="Delete operation failed"):
            await client.delete.delete(**DELETE_ARGS)


async def test_raw_delete_207_preserves_status_and_body(httpx_mock: HTTPXMock) -> None:
    body = {"success": False, "error": "partial delete", **DIAGNOSTICS}
    add_delete_response(httpx_mock, 207, body)
    with SyncHTTPClient(ClientConfig(host="h", max_retries=0)) as http:
        response = http.post("/api/v1/delete", json={**DELETE_ARGS, "dry_run": True})
    assert response.status_code == 207
    assert response.json() == body
    add_delete_response(httpx_mock, 207, body)
    async with AsyncHTTPClient(ClientConfig(host="h", max_retries=0)) as http:
        response = await http.post("/api/v1/delete", json={**DELETE_ARGS, "dry_run": True})
    assert response.status_code == 207
    assert response.json() == body


@pytest.mark.httpx_mock(can_send_already_matched_responses=True)
async def test_delete_503_preserves_diagnostics_without_retry(httpx_mock: HTTPXMock) -> None:
    body = {
        "success": False,
        "deleted_count": 4,
        "affected_files": 3,
        "rewritten_files": 1,
        "execution_time_ms": 2.5,
        "dry_run": False,
        "error": "Delete partially completed",
        **DIAGNOSTICS,
    }
    add_delete_response(httpx_mock, 503, body)
    with ArcClient(host="h", retry_backoff=0) as client, pytest.raises(ArcServerError) as caught:
        client.delete.delete(**DELETE_ARGS, dry_run=False, confirm=True)
    assert caught.value.status_code == 503
    assert caught.value.response_body == body
    assert len(httpx_mock.get_requests()) == 1

    add_delete_response(httpx_mock, 503, body)
    async with AsyncArcClient(host="h", retry_backoff=0) as client:
        with pytest.raises(ArcServerError) as caught:
            await client.delete.delete(**DELETE_ARGS, dry_run=False, confirm=True)
    assert caught.value.status_code == 503
    assert caught.value.response_body == body
    assert len(httpx_mock.get_requests()) == 2
