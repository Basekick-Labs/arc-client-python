"""HTTP round-trip tests: msgpack query endpoint."""

from __future__ import annotations

from datetime import datetime, timezone

import msgpack
import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcClient
from arc_client.exceptions import ArcNotSupportedError, ArcQueryError


@pytest.fixture
def client() -> ArcClient:
    c = ArcClient(host="h", token="t", database="db")
    yield c
    c.close()


def _columnar_envelope() -> bytes:
    ts = datetime(2021, 10, 1, tzinfo=timezone.utc)
    return msgpack.packb(
        {
            "success": True,
            "columns": ["time", "host", "usage"],
            "types": ["timestamp[us, tz=UTC]", "utf8", "double"],
            "data": [[ts, ts], ["s1", None], [1.5, 2.5]],
            "row_count": 2,
            "execution_time_ms": 4,
            "timestamp": "2021-10-01T00:00:00Z",
        },
        datetime=True,
    )


def test_query_msgpack_decodes_columnar_to_rows(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(
        content=_columnar_envelope(),
        headers={"Content-Type": "application/msgpack"},
    )

    result = client.query.query_msgpack("SELECT * FROM cpu")

    assert result.success
    assert result.columns == ["time", "host", "usage"]
    assert result.types == ["timestamp[us, tz=UTC]", "utf8", "double"]
    # Transposed to row-major.
    assert result.data[0][1] == "s1"
    assert result.data[1][1] is None  # null preserved
    assert isinstance(result.data[0][0], datetime)  # native timestamp decoded
    assert result.row_count == 2

    request = httpx_mock.get_requests()[0]
    assert request.url.path == "/api/v1/query/msgpack"
    assert request.headers["Accept"] == "application/msgpack"


def test_query_msgpack_501_raises_not_supported(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(
        status_code=501,
        json={"error": "msgpack query path requires the duckdb_arrow build tag"},
    )

    with pytest.raises(ArcNotSupportedError):
        client.query.query_msgpack("SELECT 1")


def test_query_msgpack_error_envelope_raises(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    err = msgpack.packb(
        {"success": False, "error": "bad sql", "execution_time_ms": 1, "timestamp": "x"}
    )
    httpx_mock.add_response(content=err, headers={"Content-Type": "application/msgpack"})

    with pytest.raises(ArcQueryError, match="bad sql"):
        client.query.query_msgpack("SELECT bad")


def test_query_msgpack_empty_sql_raises(client: ArcClient) -> None:
    from arc_client.exceptions import ArcValidationError

    with pytest.raises(ArcValidationError):
        client.query.query_msgpack("")
