"""HTTP round-trip tests: header assembly and error mapping."""

from __future__ import annotations

import contextlib

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
    assert USER_AGENT.startswith("arc-client-python/0.2.1 (python/")


def test_user_agent_matches_package_version(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    import arc_client

    assert USER_AGENT.startswith(f"arc-client-python/{arc_client.__version__} ")


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


# arc-client-python#35: Arc rejects db.table names while x-arc-database is
# set, and resolves bare names to "default" without it. Queries therefore send
# the header only for a database the caller chose; writes keep the default.


def _query_ok(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(json={"success": True, "columns": [], "data": [], "row_count": 0})


def test_query_without_a_chosen_database_sends_no_database_header(
    httpx_mock: HTTPXMock,
) -> None:
    _query_ok(httpx_mock)
    with ArcClient(host="testhost", port=8000) as c:
        c.query.query("SELECT * FROM default.cpu")

    assert "x-arc-database" not in httpx_mock.get_requests()[0].headers


def test_estimate_and_arrow_without_a_chosen_database_send_no_database_header(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(json={"success": True, "estimated_rows": 1})
    httpx_mock.add_response(status_code=200, content=b"")
    with ArcClient(host="testhost", port=8000) as c:
        c.query.estimate("SELECT * FROM default.cpu")
        # The empty body is not an Arrow stream; only the request matters.
        with contextlib.suppress(Exception):
            c.query.query_arrow("SELECT * FROM default.cpu")

    requests = httpx_mock.get_requests()
    assert len(requests) == 2
    for request in requests:
        assert "x-arc-database" not in request.headers, request.url


def test_query_with_a_client_database_sends_it(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    _query_ok(httpx_mock)
    client.query.query("SELECT 1")

    assert httpx_mock.get_requests()[0].headers["x-arc-database"] == "mydb"


def test_explicit_default_database_is_still_sent(httpx_mock: HTTPXMock) -> None:
    _query_ok(httpx_mock)
    with ArcClient(host="testhost", port=8000, database="default") as c:
        c.query.query("SELECT 1")

    assert httpx_mock.get_requests()[0].headers["x-arc-database"] == "default"


def test_write_without_a_chosen_database_still_targets_default(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(status_code=204)
    with ArcClient(host="testhost", port=8000) as c:
        c.write.write_line_protocol("cpu,host=a usage=1")

    assert httpx_mock.get_requests()[0].headers["x-arc-database"] == "default"


def test_unset_database_survives_a_config_round_trip() -> None:
    from arc_client.config import ClientConfig

    config = ClientConfig(host="testhost")
    for rebuilt in (
        ClientConfig.model_validate(config.model_dump()),
        ClientConfig.model_validate_json(config.model_dump_json()),
        config.model_copy(update={"host": "other"}),
    ):
        assert rebuilt.query_database is None
        assert rebuilt.default_database == "default"
