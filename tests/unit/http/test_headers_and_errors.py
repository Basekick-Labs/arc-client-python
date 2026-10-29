"""HTTP round-trip tests: header assembly and error mapping."""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcClient
from arc_client.exceptions import (
    ArcAuthenticationError,
    ArcLicenseError,
    ArcNotFoundError,
    ArcNotSupportedError,
    ArcRateLimitError,
    ArcServerError,
)
from arc_client.http.base import USER_AGENT


@pytest.fixture
def client() -> ArcClient:
    c = ArcClient(host="testhost", port=8000, token="secret-token", database="mydb")
    yield c
    c.close()


def test_headers_include_auth_db_and_versioned_user_agent(
    httpx_mock: HTTPXMock, client: ArcClient
) -> None:
    httpx_mock.add_response(json={"status": "ok"})

    client.health()

    request = httpx_mock.get_requests()[0]
    assert request.headers["Authorization"] == "Bearer secret-token"
    assert request.headers["x-arc-database"] == "mydb"
    assert request.headers["User-Agent"] == USER_AGENT
    assert USER_AGENT == "arc-client-python/0.2.0"


def test_user_agent_matches_package_version(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    import arc_client

    assert f"arc-client-python/{arc_client.__version__}" == USER_AGENT


@pytest.mark.parametrize(
    ("status", "exc"),
    [
        (401, ArcAuthenticationError),
        (403, ArcAuthenticationError),
        (404, ArcNotFoundError),
        (429, ArcRateLimitError),
        (500, ArcServerError),
        (502, ArcServerError),
        (501, ArcNotSupportedError),
    ],
)
def test_error_status_mapping(httpx_mock: HTTPXMock, status: int, exc: type) -> None:
    # Test the HTTP layer directly: sub-clients rewrap into domain errors, but
    # the status->exception mapping lives in the shared HTTP error handler.
    # max_retries=0 so retryable statuses (429/503) raise immediately.
    c = ArcClient(host="testhost", token="t", max_retries=0)
    httpx_mock.add_response(status_code=status, json={"error": "boom"})

    with pytest.raises(exc):
        c._get_http().get("/api/v1/anything")
    c.close()


def test_license_error_mapped_from_403(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(
        status_code=403,
        json={
            "success": False,
            "error": "RBAC requires an enterprise license with the 'rbac' feature enabled",
        },
    )

    with pytest.raises(ArcLicenseError):
        client.rbac.list_organizations()


def test_license_error_is_authentication_error_subclass() -> None:
    assert issubclass(ArcLicenseError, ArcAuthenticationError)


def test_per_call_database_override(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(json={"success": True, "columns": [], "data": [], "row_count": 0})

    client.query.query("SELECT 1", database="otherdb")

    request = httpx_mock.get_requests()[0]
    assert request.headers["x-arc-database"] == "otherdb"
