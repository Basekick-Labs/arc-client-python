"""HTTP round-trip tests: representative enterprise wrappers."""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcClient
from arc_client.exceptions import ArcLicenseError


@pytest.fixture
def client() -> ArcClient:
    c = ArcClient(host="h", token="t")
    yield c
    c.close()


def test_rbac_create_organization(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(
        status_code=201, json={"success": True, "organization": {"id": 1, "name": "acme"}}
    )

    result = client.rbac.create_organization("acme", description="Acme Inc")

    assert result["organization"]["name"] == "acme"
    request = httpx_mock.get_requests()[0]
    assert request.url.path == "/api/v1/rbac/organizations"
    import json

    assert json.loads(request.read()) == {"name": "acme", "description": "Acme Inc"}


def test_governance_usage(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(json={"success": True, "usage": {"queries_this_hour": 5}})

    result = client.governance.usage(42)

    assert result["usage"]["queries_this_hour"] == 5
    assert httpx_mock.get_requests()[0].url.path == "/api/v1/governance/usage/42"


def test_queries_active(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(json={"success": True, "queries": [], "count": 0})

    result = client.queries.active()

    assert result["count"] == 0
    assert httpx_mock.get_requests()[0].url.path == "/api/v1/queries/active"


def test_enterprise_license_error(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(
        status_code=403,
        json={
            "success": False,
            "error": (
                "Query governance requires an enterprise license with the "
                "'query_governance' feature enabled"
            ),
        },
    )

    with pytest.raises(ArcLicenseError):
        client.governance.list_policies()


def test_tiering_set_policy(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(json={"message": "ok", "database": "db", "policy": {}})

    client.tiering.set_policy("db", hot_only=True, hot_max_age_days=7)

    request = httpx_mock.get_requests()[0]
    assert request.method == "PUT"
    assert request.url.path == "/api/v1/tiering/policies/db"
    import json

    assert json.loads(request.read()) == {"hot_only": True, "hot_max_age_days": 7}


def test_mqtt_start_subscription(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(json={"success": True, "message": "started"})

    client.mqtt.start_subscription("sub-1")

    request = httpx_mock.get_requests()[0]
    assert request.method == "POST"
    assert request.url.path == "/api/v1/mqtt/subscriptions/sub-1/start"
