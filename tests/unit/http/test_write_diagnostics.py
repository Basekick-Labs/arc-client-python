"""HTTP contract tests for the Line Protocol and MessagePack diagnostics on client.diagnostics."""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcClient, AsyncArcClient
from arc_client.exceptions import ArcAuthenticationError, ArcError, ArcServerError


@pytest.mark.parametrize("async_mode", [False, True])
async def test_write_protocol_diagnostics(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    expected = [
        {"status": "healthy", "service": "line_protocol_writer"},
        {
            "status": "success",
            "stats": {
                "total_requests": 12,
                "total_records": 34,
                "total_bytes": 1024,
                "total_bytes_gzipped": 512,
                "total_errors": 1,
            },
        },
        {"version": "2.0", "protocol": "MessagePack"},
        {"decoder": {"records": 5}, "buffer": {"buffers": 2}},
    ]
    paths = [
        "/api/v1/write/line-protocol/health",
        "/api/v1/write/line-protocol/stats",
        "/api/v1/write/msgpack/spec",
        "/api/v1/write/msgpack/stats",
    ]
    for payload in expected:
        httpx_mock.add_response(json=payload)

    if async_mode:
        async with AsyncArcClient(host="h", token="t", max_retries=0) as client:
            diagnostics = client.diagnostics
            results = [
                await diagnostics.line_protocol_health(),
                await diagnostics.line_protocol_stats(),
                await diagnostics.msgpack_spec(),
                await diagnostics.msgpack_stats(),
            ]
    else:
        with ArcClient(host="h", token="t", max_retries=0) as client:
            diagnostics = client.diagnostics
            results = [
                diagnostics.line_protocol_health(),
                diagnostics.line_protocol_stats(),
                diagnostics.msgpack_spec(),
                diagnostics.msgpack_stats(),
            ]

    assert results == expected
    requests = httpx_mock.get_requests()
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", path) for path in paths
    ]
    assert all(request.headers["Authorization"] == "Bearer t" for request in requests)


@pytest.mark.parametrize("async_mode", [False, True])
async def test_write_diagnostics_preserve_auth_and_server_errors(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(
        status_code=401, json={"success": False, "error": "Authentication required"}
    )
    httpx_mock.add_response(status_code=503, json={"error": "Writer unavailable"})

    if async_mode:
        async with AsyncArcClient(host="h", token="t", max_retries=0) as client:
            with pytest.raises(ArcAuthenticationError) as auth_error:
                await client.diagnostics.line_protocol_health()
            with pytest.raises(ArcServerError) as error:
                await client.diagnostics.msgpack_stats()
    else:
        with ArcClient(host="h", token="t", max_retries=0) as client:
            with pytest.raises(ArcAuthenticationError) as auth_error:
                client.diagnostics.line_protocol_health()
            with pytest.raises(ArcServerError) as error:
                client.diagnostics.msgpack_stats()

    assert auth_error.value.response_body == {"success": False, "error": "Authentication required"}
    assert error.value.status_code == 503
    assert error.value.response_body == {"error": "Writer unavailable"}


@pytest.mark.parametrize("async_mode", [False, True])
async def test_write_diagnostics_reject_non_object_json(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(json=["not", "an", "object"])

    with pytest.raises(ArcError, match="Unexpected response"):
        if async_mode:
            async with AsyncArcClient(host="h", token="t", max_retries=0) as client:
                await client.diagnostics.msgpack_spec()
        else:
            with ArcClient(host="h", token="t", max_retries=0) as client:
                client.diagnostics.msgpack_spec()
