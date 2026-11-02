"""Query endpoint contracts against Arc main (2027.01): retries, errors, options."""

from __future__ import annotations

import gzip
import io
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, ClassVar

import httpx
import msgpack
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
import zstandard
from pytest_httpx import HTTPXMock

from arc_client.exceptions import (
    ArcAuthenticationError,
    ArcLicenseError,
    ArcPermissionError,
    ArcQueryError,
    ArcRateLimitError,
    ArcScopedAccessError,
    ArcValidationError,
)

from ._parity import MODES, call, make_client

JSON_OK = {
    "success": True,
    "columns": ["host", "v"],
    "data": [["a", 1]],
    "row_count": 1,
    "execution_time_ms": 2,
    "timestamp": "2027-01-01T00:00:00Z",
}


def arrow_body() -> bytes:
    table = pa.table({"host": ["a", "a"], "v": [1, 2]})
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def msgpack_body(**extra: Any) -> bytes:
    body = {**JSON_OK, "data": [["a"], [1]], "types": ["utf8", "int64"], **extra}
    return msgpack.packb(body, use_bin_type=True)


ENDPOINTS = {
    "query": ("/api/v1/query", lambda c: c.query.query("SELECT 1")),
    "msgpack": ("/api/v1/query/msgpack", lambda c: c.query.query_msgpack("SELECT 1")),
    "arrow": ("/api/v1/query/arrow", lambda c: c.query.query_arrow("SELECT 1")),
    "estimate": ("/api/v1/query/estimate", lambda c: c.query.estimate("SELECT 1")),
}


def ok_response(httpx_mock: HTTPXMock, endpoint: str) -> None:
    path = ENDPOINTS[endpoint][0]
    url = f"http://h:8000{path}"
    if endpoint == "msgpack":
        httpx_mock.add_response(
            url=url, content=msgpack_body(), headers={"Content-Type": "application/msgpack"}
        )
    elif endpoint == "arrow":
        httpx_mock.add_response(
            url=url,
            content=arrow_body(),
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
    elif endpoint == "estimate":
        httpx_mock.add_response(url=url, json={"success": True, "estimated_rows": 3})
    else:
        httpx_mock.add_response(url=url, json=JSON_OK)


# --- Bug 1: query POSTs are reads and retry 503 (not governance 429) --------


@MODES
@pytest.mark.parametrize("endpoint", list(ENDPOINTS))
async def test_query_post_retries_503(
    httpx_mock: HTTPXMock, async_mode: bool, endpoint: str
) -> None:
    path, run = ENDPOINTS[endpoint]
    # Arc's replication catch-up gate (query.go checkReplicationReady).
    httpx_mock.add_response(
        url=f"http://h:8000{path}",
        status_code=503,
        headers={"Retry-After": "0"},
        json={"success": False, "error": "replication_catch_up_in_progress"},
    )
    ok_response(httpx_mock, endpoint)
    async with make_client(async_mode, max_retries=2) as client:
        await call(run(client))
    assert len(httpx_mock.get_requests()) == 2


@MODES
@pytest.mark.parametrize("endpoint", list(ENDPOINTS))
async def test_query_post_does_not_retry_governance_429(
    httpx_mock: HTTPXMock, async_mode: bool, endpoint: str
) -> None:
    # query.go checkQueryGovernance: 429 with no Retry-After (rate limit or quota).
    path, run = ENDPOINTS[endpoint]
    httpx_mock.add_response(
        url=f"http://h:8000{path}", status_code=429, json={"error": "rate limit exceeded"}
    )
    async with make_client(async_mode, max_retries=2) as client:
        with pytest.raises(ArcRateLimitError):
            await call(run(client))
    assert len(httpx_mock.get_requests()) == 1


@MODES
async def test_query_503_exhausted_raises_query_error_with_status(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(status_code=503, headers={"Retry-After": "0"}, is_reusable=True)
    async with make_client(async_mode, max_retries=2) as client:
        with pytest.raises(ArcQueryError) as info:
            await call(client.query.query("SELECT 1"))
    assert info.value.status_code == 503
    assert len(httpx_mock.get_requests()) == 3


@MODES
async def test_query_read_timeout_is_not_replayed(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    # Arc already ran (or is running) the query; replaying would multiply cost.
    httpx_mock.add_exception(httpx.ReadTimeout("slow"))
    async with make_client(async_mode, max_retries=3) as client:
        with pytest.raises(ArcQueryError):
            await call(client.query.query("SELECT 1"))
    assert len(httpx_mock.get_requests()) == 1


@MODES
async def test_query_bad_sql_is_still_query_error(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(status_code=400, json={"success": False, "error": "bad sql"})
    async with make_client(async_mode, max_retries=3) as client:
        with pytest.raises(ArcQueryError, match="bad sql") as info:
            await call(client.query.query("SELECT nope"))
    assert info.value.status_code == 400
    assert len(httpx_mock.get_requests()) == 1


# --- Bug 2: typed errors are not rewrapped ---------------------------------

TYPED = [
    pytest.param(401, "invalid token", ArcAuthenticationError, id="401"),
    pytest.param(403, "token does not have 'read' permission", ArcPermissionError, id="403"),
    pytest.param(
        403, "access denied: no read permission for database 'x'", ArcScopedAccessError, id="scoped"
    ),
    pytest.param(403, "requires an enterprise license", ArcLicenseError, id="license"),
    pytest.param(429, "slow down", ArcRateLimitError, id="429"),
]


@MODES
@pytest.mark.parametrize("endpoint", list(ENDPOINTS))
@pytest.mark.parametrize(("status", "message", "expected"), TYPED)
async def test_query_typed_errors_surface_unchanged(
    httpx_mock: HTTPXMock,
    async_mode: bool,
    endpoint: str,
    status: int,
    message: str,
    expected: type[Exception],
) -> None:
    _, run = ENDPOINTS[endpoint]
    httpx_mock.add_response(status_code=status, json={"error": message})
    async with make_client(async_mode, max_retries=0) as client:
        with pytest.raises(expected) as info:
            await call(run(client))
    assert not isinstance(info.value, ArcQueryError)


@MODES
async def test_list_measurements_typed_error_unchanged(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(status_code=401, json={"error": "expired"})
    async with make_client(async_mode, max_retries=0) as client:
        with pytest.raises(ArcAuthenticationError):
            await call(client.query.list_measurements())


# --- Item 9/10: profile and query ID ---------------------------------------

PROFILE = {
    "total_ms": 3.5,
    "planner_ms": 0.5,
    "execution_ms": 3.0,
    "rows_scanned": 10,
    "latency_ms": 2.9,
}


@MODES
async def test_json_query_profile_and_query_id(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(json={**JSON_OK, "profile": PROFILE}, headers={"X-Arc-Query-ID": "q-1"})
    async with make_client(async_mode) as client:
        result = await call(client.query.query("SELECT 1", profile=True))
    request = httpx_mock.get_request()
    assert request.headers["x-arc-profile"] == "true"
    assert result.profile is not None
    assert result.profile.rows_scanned == 10
    assert result.profile.total_ms == 3.5
    assert result.query_id == "q-1"


@MODES
async def test_query_without_profile_sends_no_header(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(json=JSON_OK)
    async with make_client(async_mode) as client:
        result = await call(client.query.query("SELECT 1"))
    assert "x-arc-profile" not in httpx_mock.get_request().headers
    assert result.profile is None
    assert result.query_id is None


@MODES
async def test_msgpack_query_profile_and_query_id(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(
        content=msgpack_body(profile=PROFILE),
        headers={"Content-Type": "application/msgpack", "X-Arc-Query-ID": "q-2"},
    )
    async with make_client(async_mode) as client:
        result = await call(client.query.query_msgpack("SELECT 1", profile=True))
    assert httpx_mock.get_request().headers["x-arc-profile"] == "true"
    assert result.profile is not None
    assert result.profile.planner_ms == 0.5
    assert result.query_id == "q-2"
    assert result.data == [["a", 1]]


@MODES
async def test_arrow_options_and_query_id(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(
        content=arrow_body(),
        headers={
            "Content-Type": "application/vnd.apache.arrow.stream",
            "X-Arc-Query-ID": "q-3",
        },
    )
    async with make_client(async_mode) as client:
        table = await call(
            client.query.query_arrow("SELECT 1", dictionary=True, compression="ZSTD")
        )
    request = httpx_mock.get_request()
    assert request.headers["x-arc-arrow-dictionary"] == "true"
    assert request.headers["x-arc-arrow-compression"] == "zstd"
    assert table.schema.metadata[b"arc.query_id"] == b"q-3"
    assert table.num_rows == 2


@MODES
async def test_arrow_defaults_send_no_encoding_headers(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(
        content=arrow_body(), headers={"Content-Type": "application/vnd.apache.arrow.stream"}
    )
    async with make_client(async_mode) as client:
        table = await call(client.query.query_arrow("SELECT 1"))
    headers = httpx_mock.get_request().headers
    assert "x-arc-arrow-dictionary" not in headers
    assert "x-arc-arrow-compression" not in headers
    assert b"arc.query_id" not in (table.schema.metadata or {})


@MODES
async def test_arrow_rejects_unknown_compression(async_mode: bool) -> None:
    async with make_client(async_mode) as client:
        with pytest.raises(ArcValidationError, match="zstd"):
            await call(client.query.query_arrow("SELECT 1", compression="gzip"))


@MODES
async def test_arrow_options_reach_polars(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    pytest.importorskip("polars")
    httpx_mock.add_response(
        content=arrow_body(), headers={"Content-Type": "application/vnd.apache.arrow.stream"}
    )
    async with make_client(async_mode) as client:
        frame = await call(client.query.query_polars("SELECT 1", compression="lz4"))
    assert httpx_mock.get_request().headers["x-arc-arrow-compression"] == "lz4"
    assert frame.height == 2


# --- Item 11: msgpack truncation is a decode failure, never partial rows ---


@MODES
async def test_msgpack_incomplete_body_raises(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = msgpack_body()
    httpx_mock.add_response(content=body[:-6], headers={"Content-Type": "application/msgpack"})
    async with make_client(async_mode) as client:
        with pytest.raises(ArcQueryError, match="incomplete"):
            await call(client.query.query_msgpack("SELECT 1"))


# --- Item 12: GET /api/v1/query/:measurement --------------------------------


@MODES
async def test_query_measurement_params(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(json=JSON_OK, headers={"X-Arc-Query-ID": "q-4"})
    async with make_client(async_mode, database="metrics") as client:
        result = await call(
            client.query.query_measurement(
                "cpu", limit=10, offset=5, order_by="time ASC", where="host = 'a'"
            )
        )
    request = httpx_mock.get_request()
    assert request.method == "GET"
    assert request.url.path == "/api/v1/query/cpu"
    assert dict(request.url.params) == {
        "database": "metrics",
        "limit": "10",
        "offset": "5",
        "order_by": "time ASC",
        "where": "host = 'a'",
    }
    assert result.data == [["a", 1]]
    assert result.query_id == "q-4"


@MODES
async def test_query_measurement_defaults_left_to_arc(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(json=JSON_OK)
    async with make_client(async_mode) as client:
        await call(client.query.query_measurement("cpu", "db1"))
    assert dict(httpx_mock.get_request().url.params) == {"database": "db1"}


@MODES
@pytest.mark.parametrize(
    "kwargs",
    [{"limit": -1}, {"limit": 1_000_001}, {"offset": -1}, {"measurement": "bad name"}],
)
async def test_query_measurement_validates(async_mode: bool, kwargs: dict[str, Any]) -> None:
    kwargs = dict(kwargs)
    measurement = kwargs.pop("measurement", "cpu")
    async with make_client(async_mode) as client:
        with pytest.raises(ArcValidationError):
            await call(client.query.query_measurement(measurement, **kwargs))


# --- Item 15: compressed JSON/msgpack responses ----------------------------
# A real HTTP/1.1 server: pytest-httpx pre-reads mocked bodies, which runs the
# content decoder twice and is not what a socket does.


class _CompressingHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    seen: ClassVar[list[str]] = []

    def do_POST(self) -> None:  # noqa: N802 - http.server API
        self.rfile.read(int(self.headers["Content-Length"]))
        accept = self.headers.get("Accept-Encoding", "")
        self.seen.append(accept)
        if self.path.endswith("/msgpack"):
            raw, content_type = msgpack_body(), "application/msgpack"
        else:
            raw, content_type = json.dumps(JSON_OK).encode(), "application/json"
        if "zstd" in accept:
            # Streamed blocks inside one frame, as Arc's pooled encoder writes.
            compressor = zstandard.ZstdCompressor().compressobj()
            data = compressor.compress(raw) + compressor.flush()
            encoding = "zstd"
        else:
            data, encoding = gzip.compress(raw), "gzip"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Encoding", encoding)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args: Any) -> None:
        pass


@MODES
@pytest.mark.parametrize("wire", ["json", "msgpack"])
async def test_compressed_query_responses_decode(async_mode: bool, wire: str) -> None:
    # Arc negotiates zstd (preferred) or gzip from Accept-Encoding on the JSON
    # and msgpack paths (internal/api/response_compression.go). httpx sends
    # "gzip, deflate" and, from 0.27.1 with zstandard installed, "zstd".
    server = ThreadingHTTPServer(("127.0.0.1", 0), _CompressingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        async with make_client(async_mode, host="127.0.0.1", port=server.server_address[1]) as c:
            run = c.query.query_msgpack if wire == "msgpack" else c.query.query
            result = await call(run("SELECT 1"))
    finally:
        server.shutdown()
        server.server_close()
    assert result.data == [["a", 1]]
    assert "gzip" in _CompressingHandler.seen[-1]


@MODES
async def test_query_measurement_governance_429_not_retried(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    # Found live: as a GET it used to retry Arc's governance 429 (5 requests).
    httpx_mock.add_response(status_code=429, json={"error": "rate limit exceeded"})
    async with make_client(async_mode, max_retries=3) as client:
        with pytest.raises(ArcRateLimitError):
            await call(client.query.query_measurement("cpu"))
    assert len(httpx_mock.get_requests()) == 1


@MODES
async def test_query_measurement_retries_503(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(status_code=503, headers={"Retry-After": "0"}, json={})
    httpx_mock.add_response(json=JSON_OK)
    async with make_client(async_mode, max_retries=2) as client:
        await call(client.query.query_measurement("cpu"))
    assert len(httpx_mock.get_requests()) == 2
