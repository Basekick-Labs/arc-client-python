"""Write endpoint contracts against Arc main (2027.01).

Refusals (internal/api/routing.go respondIngestRefused/respondIngestFenced),
partial writes (internal/api/partial_write.go), line-protocol precision
(internal/api/lineprotocol.go) and msgpack tag_keys (internal/ingest/
msgpack_typed.go).
"""

from __future__ import annotations

from typing import Any

import msgpack
import pyarrow as pa
import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcIngestRefusedError, ArcPartialWriteError
from arc_client.compression import Compression
from arc_client.exceptions import (
    ArcAuthenticationError,
    ArcIngestionError,
    ArcPermissionError,
    ArcRateLimitError,
    ArcValidationError,
)

from ._parity import MODES, call, make_client

LP = "/api/v1/write/line-protocol"
MSGPACK = "/api/v1/write/msgpack"

PARTIAL_REST = (
    'partial write: 3 points dropped: 3 in "cpu" column "v": value type does not '
    "match the column; the other points were written"
)
PARTIAL_ALL = 'partial write: 2 points dropped: 2 in "cpu": field named time;'


def client_kwargs(**extra: Any) -> dict[str, Any]:
    return {"compression": Compression.NONE, **extra}


def write_lp(client: Any) -> Any:
    return client.write.write_line_protocol("cpu,host=a v=1i")


def write_columnar(client: Any) -> Any:
    return client.write.write_columnar("cpu", {"time": [1], "v": [1.0]})


WRITES = [pytest.param(write_lp, id="lp"), pytest.param(write_columnar, id="msgpack")]


# --- Bug 4: ingest refusals --------------------------------------------------


@MODES
@pytest.mark.parametrize("write", WRITES)
@pytest.mark.parametrize(
    "headers",
    [
        pytest.param({"X-Arc-Ingest-Refused": "wal_disk"}, id="wal_disk"),
        pytest.param({"X-Arc-Ingest-Fenced": "1"}, id="fenced"),
    ],
)
async def test_safe_refusal_is_resent(
    httpx_mock: HTTPXMock, async_mode: bool, write: Any, headers: dict[str, str]
) -> None:
    httpx_mock.add_response(
        status_code=503,
        headers={**headers, "Retry-After": "0"},
        json={"error": "Write rejected (retry): nothing was written"},
    )
    httpx_mock.add_response(status_code=204)
    async with make_client(async_mode, max_retries=2, **client_kwargs()) as client:
        await call(write(client))
    requests = httpx_mock.get_requests()
    assert len(requests) == 2
    assert requests[0].content == requests[1].content


def write_lp_multi(client: Any) -> Any:
    return client.write.write_line_protocol(["cpu,host=a v=1i", "mem,host=a used=2i"])


def write_records_multi(client: Any) -> Any:
    return client.write.write_records(
        [
            {"measurement": "cpu", "fields": {"v": 1}},
            {"measurement": "mem", "fields": {"used": 2}},
        ]
    )


@MODES
@pytest.mark.parametrize(
    "write",
    [pytest.param(write_lp_multi, id="lp"), pytest.param(write_records_multi, id="records")],
)
@pytest.mark.parametrize(
    "headers",
    [
        pytest.param({"X-Arc-Ingest-Refused": "wal_disk"}, id="wal_disk"),
        pytest.param({"X-Arc-Ingest-Fenced": "1"}, id="fenced"),
    ],
)
async def test_multi_measurement_refusal_is_not_resent(
    httpx_mock: HTTPXMock, async_mode: bool, write: Any, headers: dict[str, str]
) -> None:
    # Arc re-checks the fence per measurement (ingest.checkIngestGate), so the
    # refusal may follow measurements it already buffered: a resend duplicates.
    httpx_mock.add_response(
        status_code=503,
        headers={**headers, "Retry-After": "0"},
        json={"error": "Write rejected (retry)"},
    )
    async with make_client(async_mode, max_retries=3, **client_kwargs()) as client:
        with pytest.raises(ArcIngestRefusedError) as info:
            await call(write(client))
    assert info.value.retry_safe is False
    assert "several measurements" in str(info.value)
    assert len(httpx_mock.get_requests()) == 1


@pytest.mark.parametrize(
    ("body", "single"),
    [
        (b"cpu,host=a v=1i 1\ncpu v=2i 2\n", True),
        (b"# comment\n\ncpu v=1i\n", True),
        (b"cpu,host=a v=1i\nmem v=1i\n", False),
        (b"my\\ cpu,host=a v=1i\nmy\\ cpu v=2i\n", True),
        (b"my\\ cpu v=1i\nmy v=2i\n", False),
        (b"a\\,b v=1i\na v=1i\n", False),
        (b"", True),
    ],
)
def test_single_lp_measurement(body: bytes, single: bool) -> None:
    from arc_client.ingestion.write_support import single_lp_measurement

    assert single_lp_measurement(body) is single


@MODES
async def test_safe_refusal_exhausted_reports_reason(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(
        status_code=503,
        headers={"X-Arc-Ingest-Refused": "wal_disk", "Retry-After": "0"},
        json={"error": "Write rejected (retry): WAL volume disk pressure"},
        is_reusable=True,
    )
    async with make_client(async_mode, max_retries=1, **client_kwargs()) as client:
        with pytest.raises(ArcIngestRefusedError) as info:
            await call(write_lp(client))
    assert info.value.reason == "wal_disk"
    assert info.value.retry_safe is True
    assert info.value.status_code == 503
    assert isinstance(info.value, ArcIngestionError)
    assert len(httpx_mock.get_requests()) == 2


@MODES
@pytest.mark.parametrize("write", WRITES)
async def test_wal_not_durable_is_never_resent(
    httpx_mock: HTTPXMock, async_mode: bool, write: Any
) -> None:
    httpx_mock.add_response(
        status_code=503,
        headers={"X-Arc-Ingest-Refused": "wal_not_durable", "Retry-After": "5"},
        json={"error": "write refused: earlier parts of this request may already be stored"},
    )
    async with make_client(async_mode, max_retries=3, **client_kwargs()) as client:
        with pytest.raises(ArcIngestRefusedError) as info:
            await call(write(client))
    assert info.value.reason == "wal_not_durable"
    assert info.value.retry_safe is False
    assert len(httpx_mock.get_requests()) == 1


@MODES
async def test_plain_write_503_is_not_resent(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    # Schema churn / shutdown 503s carry no refusal header and may follow a
    # partially applied request: not resent.
    httpx_mock.add_response(status_code=503, json={"error": "Write rejected (schema churn)"})
    async with make_client(async_mode, max_retries=3, **client_kwargs()) as client:
        with pytest.raises(ArcIngestionError) as info:
            await call(write_lp(client))
    assert not isinstance(info.value, ArcIngestRefusedError)
    assert info.value.status_code == 503
    assert len(httpx_mock.get_requests()) == 1


# --- Item 8: partial writes ---------------------------------------------------


@MODES
@pytest.mark.parametrize("write", WRITES)
@pytest.mark.parametrize(
    ("message", "dropped", "others"),
    [
        pytest.param(PARTIAL_REST, 3, True, id="others-written"),
        pytest.param(PARTIAL_ALL, 2, False, id="all-dropped"),
    ],
)
async def test_partial_write_error(
    httpx_mock: HTTPXMock,
    async_mode: bool,
    write: Any,
    message: str,
    dropped: int,
    others: bool,
) -> None:
    httpx_mock.add_response(status_code=400, json={"error": message})
    async with make_client(async_mode, max_retries=3, **client_kwargs()) as client:
        with pytest.raises(ArcPartialWriteError) as info:
            await call(write(client))
    assert info.value.dropped == dropped
    assert info.value.others_written is others
    assert info.value.status_code == 400
    assert info.value.response_body == {"error": message}
    assert len(httpx_mock.get_requests()) == 1


@MODES
async def test_other_400_is_plain_ingestion_error(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(status_code=400, json={"error": "No valid records in request"})
    async with make_client(async_mode, **client_kwargs()) as client:
        with pytest.raises(ArcIngestionError) as info:
            await call(write_lp(client))
    assert not isinstance(info.value, ArcPartialWriteError)
    assert info.value.status_code == 400


# --- Bug 2 (writes): typed errors surface as themselves ---------------------


@MODES
@pytest.mark.parametrize(
    ("status", "message", "expected"),
    [
        (401, "invalid token", ArcAuthenticationError),
        (403, "token does not have 'write' permission", ArcPermissionError),
        (429, "slow down", ArcRateLimitError),
    ],
)
async def test_write_typed_errors_surface_unchanged(
    httpx_mock: HTTPXMock,
    async_mode: bool,
    status: int,
    message: str,
    expected: type[Exception],
) -> None:
    httpx_mock.add_response(status_code=status, json={"error": message})
    async with make_client(async_mode, max_retries=3, **client_kwargs()) as client:
        with pytest.raises(expected) as info:
            await call(write_lp(client))
    assert not isinstance(info.value, ArcIngestionError)
    assert len(httpx_mock.get_requests()) == 1


# --- Item 6: line-protocol precision ----------------------------------------


@MODES
async def test_line_protocol_precision_param(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(status_code=204)
    async with make_client(async_mode, **client_kwargs()) as client:
        await call(client.write.write_line_protocol("cpu v=1i 1700000000", precision="s"))
    request = httpx_mock.get_request()
    assert request.url.path == LP
    assert dict(request.url.params) == {"precision": "s"}
    assert request.content == b"cpu v=1i 1700000000"


@MODES
async def test_line_protocol_default_sends_no_precision(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(status_code=204)
    async with make_client(async_mode, **client_kwargs()) as client:
        await call(client.write.write_line_protocol("cpu v=1i"))
    assert "precision" not in httpx_mock.get_request().url.params


@MODES
async def test_write_point_precision_sends_timestamp_unconverted(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(status_code=204)
    async with make_client(async_mode, **client_kwargs()) as client:
        await call(client.write.write_point("cpu", {"v": 1}, timestamp=1700000000, precision="s"))
    request = httpx_mock.get_request()
    assert dict(request.url.params) == {"precision": "s"}
    assert request.content == b"cpu v=1i 1700000000"


@MODES
async def test_write_point_default_keeps_microseconds(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(status_code=204)
    async with make_client(async_mode, **client_kwargs()) as client:
        await call(client.write.write_point("cpu", {"v": 1}, timestamp=1_000_000))
    request = httpx_mock.get_request()
    assert "precision" not in request.url.params
    assert request.content == b"cpu v=1i 1000000000"


@MODES
@pytest.mark.parametrize("precision", ["n", "u", "NS", "h", ""])
async def test_invalid_precision_rejected(async_mode: bool, precision: str) -> None:
    async with make_client(async_mode, **client_kwargs()) as client:
        with pytest.raises(ArcValidationError):
            await call(client.write.write_line_protocol("cpu v=1i", precision=precision))


# --- Item 7: msgpack tag_keys -------------------------------------------------


def sent_payload(httpx_mock: HTTPXMock) -> dict[str, Any]:
    request = httpx_mock.get_request()
    assert request.url.path == MSGPACK
    payload: dict[str, Any] = msgpack.unpackb(request.content, raw=False)
    return payload


@MODES
async def test_write_columnar_sends_tag_keys(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(status_code=204)
    columns = {"time": [1], "host": ["a"], "region": ["eu"], "v": [1.0]}
    async with make_client(async_mode, **client_kwargs()) as client:
        await call(client.write.write_columnar("cpu", columns, tag_columns=["host", "region"]))
    payload = sent_payload(httpx_mock)
    assert payload["tag_keys"] == ["host", "region"]
    assert payload["columns"]["host"] == ["a"]


@MODES
async def test_write_columnar_without_tags_omits_key(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(status_code=204)
    async with make_client(async_mode, **client_kwargs()) as client:
        await call(write_columnar(client))
    assert "tag_keys" not in sent_payload(httpx_mock)


@MODES
async def test_write_dataframe_sends_tag_keys(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(status_code=204)
    table = pa.table({"ts": [1, 2], "host": ["a", "b"], "v": [1.0, 2.0]})
    async with make_client(async_mode, **client_kwargs()) as client:
        await call(
            client.write.write_dataframe(table, "cpu", time_column="ts", tag_columns=["host"])
        )
    payload = sent_payload(httpx_mock)
    assert payload["tag_keys"] == ["host"]
    assert payload["columns"]["time"] == [1, 2]


@MODES
@pytest.mark.parametrize("tags", [["time"], ["missing"], "host"])
async def test_invalid_tag_columns_rejected(async_mode: bool, tags: Any) -> None:
    columns = {"time": [1], "host": ["a"], "v": [1.0]}
    async with make_client(async_mode, **client_kwargs()) as client:
        with pytest.raises(ArcValidationError):
            await call(client.write.write_columnar("cpu", columns, tag_columns=tags))


@MODES
async def test_buffered_record_tags_become_tag_keys(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(status_code=204)
    async with make_client(async_mode, **client_kwargs()) as client:
        buffer = client.write.buffered(batch_size=100, flush_interval=3600)
        await call(
            buffer.write({"measurement": "cpu", "fields": {"v": 1.0}, "tags": {"host": "a"}})
        )
        await call(buffer.write({"measurement": "cpu", "fields": {"v": 2.0}}))
        await call(buffer.close())
    payload = sent_payload(httpx_mock)
    assert payload["tag_keys"] == ["host"]
    assert payload["columns"]["host"] == ["a", None]
