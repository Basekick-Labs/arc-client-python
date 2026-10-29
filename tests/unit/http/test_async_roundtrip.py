"""HTTP round-trip tests for the async client (parity with the sync tests)."""

from __future__ import annotations

from datetime import datetime, timezone

import httpx
import msgpack
import pytest
from pytest_httpx import HTTPXMock

from arc_client import AsyncArcClient, Compression
from arc_client.compression import is_zstd
from arc_client.exceptions import ArcLicenseError, ArcNotSupportedError, ArcRateLimitError


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _noop(*_: object) -> None:
        return None

    monkeypatch.setattr("arc_client.http.async_http.asyncio.sleep", _noop)


async def test_async_zstd_write_no_content_encoding(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=204)
    client = AsyncArcClient(host="h", token="t", compression=Compression.ZSTD)

    await client.write.write_columnar("cpu", {"time": [1], "v": [1.0]})

    request = httpx_mock.get_requests()[0]
    assert is_zstd(request.read())
    assert "Content-Encoding" not in request.headers
    await client.close()


async def test_async_query_msgpack_decodes(httpx_mock: HTTPXMock) -> None:
    ts = datetime(2021, 10, 1, tzinfo=timezone.utc)
    content = msgpack.packb(
        {
            "success": True,
            "columns": ["time", "v"],
            "types": ["timestamp[us, tz=UTC]", "double"],
            "data": [[ts], [1.5]],
            "row_count": 1,
            "execution_time_ms": 2,
            "timestamp": "x",
        },
        datetime=True,
    )
    httpx_mock.add_response(content=content, headers={"Content-Type": "application/msgpack"})
    client = AsyncArcClient(host="h", token="t")

    result = await client.query.query_msgpack("SELECT * FROM cpu")

    assert result.columns == ["time", "v"]
    assert isinstance(result.data[0][0], datetime)
    await client.close()


async def test_async_query_msgpack_501(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=501, json={"error": "no build tag"})
    client = AsyncArcClient(host="h", token="t")

    with pytest.raises(ArcNotSupportedError):
        await client.query.query_msgpack("SELECT 1")
    await client.close()


async def test_async_import_csv(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        json={
            "status": "ok",
            "result": {"database": "db", "measurement": "cpu", "rows_imported": 7},
        }
    )
    client = AsyncArcClient(host="h", token="t", database="db")

    result = await client.import_.import_csv(b"time,v\n1,2\n", measurement="cpu")

    assert result.rows_imported == 7
    await client.close()


async def test_async_databases_create(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=201, json={"name": "newdb", "measurement_count": 0})
    client = AsyncArcClient(host="h", token="t")

    result = await client.databases.create("newdb")

    assert result.name == "newdb"
    await client.close()


async def test_async_retry_429(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=429)
    httpx_mock.add_response(json={"status": "ok"})
    client = AsyncArcClient(host="h", token="t", max_retries=2)

    await client.health()

    assert len(httpx_mock.get_requests()) == 2
    await client.close()


async def test_async_retry_gives_up(httpx_mock: HTTPXMock) -> None:
    for _ in range(3):
        httpx_mock.add_response(status_code=429)
    client = AsyncArcClient(host="h", token="t", max_retries=2)

    with pytest.raises(ArcRateLimitError):
        await client.health()
    assert len(httpx_mock.get_requests()) == 3
    await client.close()


async def test_async_connection_error_recovers(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_exception(httpx.ConnectError("refused"))
    httpx_mock.add_response(json={"status": "ok"})
    client = AsyncArcClient(host="h", token="t", max_retries=2)

    await client.health()

    assert len(httpx_mock.get_requests()) == 2
    await client.close()


async def test_async_enterprise_license_error(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        status_code=403,
        json={"error": "RBAC requires an enterprise license with the 'rbac' feature enabled"},
    )
    client = AsyncArcClient(host="h", token="t")

    with pytest.raises(ArcLicenseError):
        await client.rbac.list_organizations()
    await client.close()
