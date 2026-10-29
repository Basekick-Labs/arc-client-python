"""HTTP round-trip tests: retry/backoff behavior."""

from __future__ import annotations

import httpx
import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcClient
from arc_client.exceptions import ArcConnectionError, ArcRateLimitError, ArcServerError


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make retry backoff instant."""
    monkeypatch.setattr("arc_client.http.sync_http.time.sleep", lambda *_: None)


def test_retries_429_then_succeeds(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=429, headers={"Retry-After": "0"})
    httpx_mock.add_response(status_code=429)
    httpx_mock.add_response(json={"status": "ok"})
    client = ArcClient(host="h", token="t", max_retries=3)

    client.health()  # should succeed on the 3rd attempt

    assert len(httpx_mock.get_requests()) == 3
    client.close()


def test_retries_503_then_succeeds(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=503)
    httpx_mock.add_response(json={"status": "ok"})
    client = ArcClient(host="h", token="t", max_retries=2)

    client.health()

    assert len(httpx_mock.get_requests()) == 2
    client.close()


def test_gives_up_after_max_retries_on_429(httpx_mock: HTTPXMock) -> None:
    # max_retries=2 -> 3 total attempts, all 429 -> raises.
    for _ in range(3):
        httpx_mock.add_response(status_code=429)
    client = ArcClient(host="h", token="t", max_retries=2)

    with pytest.raises(ArcRateLimitError):
        client.health()

    assert len(httpx_mock.get_requests()) == 3
    client.close()


def test_no_retry_when_disabled(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=503)
    client = ArcClient(host="h", token="t", max_retries=0)

    with pytest.raises(ArcServerError):
        client.health()

    assert len(httpx_mock.get_requests()) == 1
    client.close()


def test_retries_connection_error_then_raises(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_exception(httpx.ConnectError("refused"))
    httpx_mock.add_exception(httpx.ConnectError("refused"))
    client = ArcClient(host="h", token="t", max_retries=1)

    with pytest.raises(ArcConnectionError):
        client.health()

    assert len(httpx_mock.get_requests()) == 2
    client.close()


def test_connection_error_recovers_on_retry(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_exception(httpx.ConnectError("refused"))
    httpx_mock.add_response(json={"status": "ok"})
    client = ArcClient(host="h", token="t", max_retries=2)

    client.health()

    assert len(httpx_mock.get_requests()) == 2
    client.close()
