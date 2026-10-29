"""HTTP round-trip tests: write compression and Content-Encoding behavior."""

from __future__ import annotations

from pytest_httpx import HTTPXMock

from arc_client import ArcClient, Compression
from arc_client.compression import decompress_gzip, decompress_zstd, is_gzipped, is_zstd


def _make_client(compression: object) -> ArcClient:
    return ArcClient(host="testhost", token="t", compression=compression)


def test_zstd_write_sends_zstd_magic_and_no_content_encoding(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=204)
    client = _make_client(Compression.ZSTD)

    client.write.write_columnar("cpu", {"time": [1, 2], "v": [1.0, 2.0]})

    request = httpx_mock.get_requests()[0]
    body = request.read()
    assert is_zstd(body), "body should carry the zstd magic bytes"
    assert "Content-Encoding" not in request.headers
    assert request.headers["Content-Type"] == "application/msgpack"
    # Round-trips back to a valid msgpack payload.
    assert decompress_zstd(body)
    client.close()


def test_gzip_write_sends_gzip_magic_and_no_content_encoding(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=204)
    client = _make_client("gzip")

    client.write.write_columnar("cpu", {"time": [1], "v": [1.0]})

    request = httpx_mock.get_requests()[0]
    body = request.read()
    assert is_gzipped(body)
    assert "Content-Encoding" not in request.headers
    assert decompress_gzip(body)
    client.close()


def test_no_compression_sends_raw_msgpack(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=204)
    client = _make_client("none")

    client.write.write_columnar("cpu", {"time": [1], "v": [1.0]})

    body = httpx_mock.get_requests()[0].read()
    assert not is_gzipped(body)
    assert not is_zstd(body)
    client.close()


def test_per_call_compress_override(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=204)
    # Client default zstd, but override to gzip for this call.
    client = _make_client(Compression.ZSTD)

    client.write.write_columnar("cpu", {"time": [1], "v": [1.0]}, compress="gzip")

    body = httpx_mock.get_requests()[0].read()
    assert is_gzipped(body)
    client.close()


def test_bool_true_compress_is_gzip_backcompat(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=204)
    client = _make_client(True)

    client.write.write_columnar("cpu", {"time": [1], "v": [1.0]})

    assert is_gzipped(httpx_mock.get_requests()[0].read())
    client.close()


def test_line_protocol_write_targets_correct_endpoint(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=204)
    client = _make_client("zstd")

    client.write.write_line_protocol("cpu,host=s1 usage=1.0")

    request = httpx_mock.get_requests()[0]
    assert request.url.path == "/api/v1/write/line-protocol"
    assert request.headers["Content-Type"] == "text/plain"
    assert is_zstd(request.read())
    client.close()


def test_write_tle_sends_measurement_header(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=204)
    client = _make_client("none")

    client.write.write_tle("1 25544U ...\n2 25544 ...", measurement="iss_tle")

    request = httpx_mock.get_requests()[0]
    assert request.url.path == "/api/v1/write/tle"
    assert request.headers["x-arc-measurement"] == "iss_tle"
    client.close()
