"""Deletes outlast Arc's 30 s internal waits; timeouts are reported honestly.

Found live on a 4-node cluster: a non-dry-run delete with allow_unreachable
took 30.1 s inside Arc (internal/api/delete_records.go deleteLocalWait), the
same as the default client timeout, so a delete that ran was reported as
"Failed to connect".
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from pytest_httpx import HTTPXMock

from arc_client.exceptions import ArcConnectionError
from arc_client.management.delete import DELETE_TIMEOUT_FLOOR

from ._parity import MODES, call, make_client

DELETE_OK = {"success": True, "deleted_count": 1, "affected_files": 1}


def read_timeout(request: httpx.Request) -> Any:
    return request.extensions["timeout"]["read"]


@MODES
async def test_delete_gets_timeout_floor(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(json=DELETE_OK)
    async with make_client(async_mode, timeout=30.0, max_retries=0) as client:
        await call(
            client.delete.delete(
                "db", "cpu", "host = 'a'", dry_run=False, confirm=True, allow_unreachable=True
            )
        )
    assert read_timeout(httpx_mock.get_request()) == DELETE_TIMEOUT_FLOOR


@MODES
async def test_dry_run_keeps_client_timeout(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(json=DELETE_OK)
    async with make_client(async_mode, timeout=30.0, max_retries=0) as client:
        await call(client.delete.delete("db", "cpu", "host = 'a'"))
    assert read_timeout(httpx_mock.get_request()) == 30.0


@MODES
async def test_larger_client_timeout_wins(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(json=DELETE_OK)
    async with make_client(async_mode, timeout=300.0, max_retries=0) as client:
        await call(client.delete.delete("db", "cpu", "host = 'a'", dry_run=False, confirm=True))
    assert read_timeout(httpx_mock.get_request()) == 300.0


@MODES
async def test_database_delete_gets_timeout_floor(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(json={"message": "deleted", "files_deleted": 1})
    async with make_client(async_mode, timeout=30.0, max_retries=0) as client:
        await call(client.databases.delete("olddb", confirm=True))
    assert read_timeout(httpx_mock.get_request()) == DELETE_TIMEOUT_FLOOR


@MODES
async def test_read_timeout_is_not_reported_as_connect_failure(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_exception(httpx.ReadTimeout("timed out"))
    async with make_client(async_mode, max_retries=0) as client:
        with pytest.raises(ArcConnectionError) as info:
            await call(client.delete.delete("db", "cpu", "host = 'a'", dry_run=False, confirm=True))
    message = str(info.value)
    assert "Failed to connect" not in message
    assert "may have been applied" in message


@MODES
async def test_connect_error_still_says_failed_to_connect(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_exception(httpx.ConnectError("refused"))
    async with make_client(async_mode, max_retries=0) as client:
        with pytest.raises(ArcConnectionError, match="Failed to connect"):
            await call(client.health())


@MODES
async def test_remove_node_refused_on_standalone_raises(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    # Found live: a node without clustering answers 200 {"success": false},
    # which the SDK used to return as if the node had been removed.
    from arc_client.exceptions import ArcError

    httpx_mock.add_response(
        json={"enabled": False, "success": False, "reason": "Clustering not enabled"}
    )
    async with make_client(async_mode, max_retries=0) as client:
        with pytest.raises(ArcError, match="not removed"):
            await call(client.cluster.remove_node("node-a/b"))
    assert httpx_mock.get_request().url.raw_path.endswith(b"/api/v1/cluster/nodes/node-a%2Fb")
