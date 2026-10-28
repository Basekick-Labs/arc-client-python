"""Query metadata survives actual JSON and MessagePack HTTP envelopes."""

from __future__ import annotations

import json
from typing import Any

import msgpack
import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcClient, AsyncArcClient

METADATA_CASES = [
    pytest.param({"rows_capped": True, "row_cap": 2}, id="capped"),
    pytest.param({"rows_capped": False, "row_cap": 100}, id="uncapped"),
    pytest.param({}, id="omitted"),
    pytest.param({"truncated": True, "truncation_reason": "file disappeared"}, id="truncated"),
]


def add_query_response(httpx_mock: HTTPXMock, wire_format: str, metadata: dict[str, Any]) -> None:
    body = {
        "success": True,
        "columns": ["host", "value"],
        "types": ["utf8", "int64"],
        "data": [["a", 3], ["b", None]],
        "row_count": 2,
        "execution_time_ms": 1.25,
        "timestamp": "2026-10-27T00:00:00Z",
        **metadata,
    }
    if wire_format == "msgpack":
        body["data"] = [["a", "b"], [3, None]]
        content = msgpack.packb(body, use_bin_type=True)
        content_type = "application/msgpack"
        path = "/api/v1/query/msgpack"
    else:
        content = json.dumps(body).encode()
        content_type = "application/json"
        path = "/api/v1/query"
    httpx_mock.add_response(
        method="POST",
        url=f"http://h:8000{path}",
        content=content,
        headers={"Content-Type": content_type},
    )


def assert_query_response(result: Any, metadata: dict[str, Any]) -> None:
    assert result.success is True
    assert result.columns == ["host", "value"]
    assert result.types == ["utf8", "int64"]
    assert result.data == [["a", 3], ["b", None]]
    assert result.row_count == 2
    assert result.execution_time_ms == 1.25
    assert result.timestamp == "2026-10-27T00:00:00Z"
    assert result.rows_capped is metadata.get("rows_capped", False)
    assert result.row_cap == metadata.get("row_cap")
    assert result.truncated is metadata.get("truncated", False)
    assert result.truncation_reason == metadata.get("truncation_reason")


@pytest.mark.parametrize("wire_format", ["json", "msgpack"])
@pytest.mark.parametrize("metadata", METADATA_CASES)
def test_query_metadata_sync(
    httpx_mock: HTTPXMock, wire_format: str, metadata: dict[str, Any]
) -> None:
    add_query_response(httpx_mock, wire_format, metadata)
    with ArcClient(host="h", max_retries=0) as client:
        query = client.query.query_msgpack if wire_format == "msgpack" else client.query.query
        result = query("SELECT host, value FROM cpu")
    assert_query_response(result, metadata)
    assert json.loads(httpx_mock.get_request().content) == {"sql": "SELECT host, value FROM cpu"}


@pytest.mark.parametrize("wire_format", ["json", "msgpack"])
@pytest.mark.parametrize("metadata", METADATA_CASES)
async def test_query_metadata_async(
    httpx_mock: HTTPXMock, wire_format: str, metadata: dict[str, Any]
) -> None:
    add_query_response(httpx_mock, wire_format, metadata)
    async with AsyncArcClient(host="h", max_retries=0) as client:
        query = client.query.query_msgpack if wire_format == "msgpack" else client.query.query
        result = await query("SELECT host, value FROM cpu")
    assert_query_response(result, metadata)
    assert json.loads(httpx_mock.get_request().content) == {"sql": "SELECT host, value FROM cpu"}


TIMESTAMP_CASES = [
    pytest.param(1730000000, 123456789, id="nanoseconds"),
    pytest.param(-1, 999999999, id="negative-one-nanosecond"),
    pytest.param(0, 1, id="positive-one-nanosecond"),
    pytest.param(1730000000, 123456000, id="microseconds"),
    pytest.param(-1, 999999000, id="negative-one-microsecond"),
    pytest.param(0, 0, id="epoch"),
    pytest.param(253402300799, 999999000, id="datetime-maximum"),
    pytest.param(2**63 - 1, 0, id="beyond-datetime-maximum"),
    pytest.param(-(2**63), 0, id="below-datetime-minimum"),
]


def add_timestamp_response(httpx_mock: HTTPXMock, seconds: int, nanoseconds: int) -> None:
    timestamp = msgpack.Timestamp(seconds, nanoseconds)
    httpx_mock.add_response(
        method="POST",
        url="http://h:8000/api/v1/query/msgpack",
        content=msgpack.packb(
            {
                "success": True,
                "columns": ["time", "signed", "unsigned", "nested"],
                "types": ["timestamp[ns]", "int64", "uint64", "struct"],
                "data": [
                    [timestamp, None],
                    [-(2**63), 2**63 - 1],
                    [2**64 - 1, 0],
                    [{"times": [timestamp, None], "count": 2**64 - 1}, None],
                ],
                "row_count": 2,
            },
            use_bin_type=True,
        ),
        headers={"Content-Type": "application/msgpack"},
    )


def assert_lossless_timestamp(result: Any, seconds: int, nanoseconds: int) -> None:
    from datetime import datetime, timezone

    value = result.data[0][0]
    if nanoseconds % 1000 or not (-62135596800 <= seconds <= 253402300799):
        assert isinstance(value, msgpack.Timestamp)
        assert value.seconds == seconds
        assert value.nanoseconds == nanoseconds
        assert value.to_unix_nano() == seconds * 1_000_000_000 + nanoseconds
    else:
        assert isinstance(value, datetime)
        assert value.tzinfo == timezone.utc
        delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
        actual_ns = ((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds) * 1000
        assert actual_ns == seconds * 1_000_000_000 + nanoseconds
    assert result.data[0][1:] == [
        -(2**63),
        2**64 - 1,
        {"times": [value, None], "count": 2**64 - 1},
    ]
    assert result.data[1] == [None, 2**63 - 1, 0, None]
    assert result.types == ["timestamp[ns]", "int64", "uint64", "struct"]


@pytest.mark.parametrize("seconds,nanoseconds", TIMESTAMP_CASES)
def test_msgpack_timestamps_lossless_sync(
    httpx_mock: HTTPXMock, seconds: int, nanoseconds: int
) -> None:
    add_timestamp_response(httpx_mock, seconds, nanoseconds)
    with ArcClient(host="h", max_retries=0) as client:
        result = client.query.query_msgpack("SELECT * FROM cpu")
    assert_lossless_timestamp(result, seconds, nanoseconds)


@pytest.mark.parametrize("seconds,nanoseconds", TIMESTAMP_CASES)
async def test_msgpack_timestamps_lossless_async(
    httpx_mock: HTTPXMock, seconds: int, nanoseconds: int
) -> None:
    add_timestamp_response(httpx_mock, seconds, nanoseconds)
    async with AsyncArcClient(host="h", max_retries=0) as client:
        result = await client.query.query_msgpack("SELECT * FROM cpu")
    assert_lossless_timestamp(result, seconds, nanoseconds)
