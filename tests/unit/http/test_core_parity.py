"""Auth, health/readiness and measurement-schema contracts against Arc main."""

from __future__ import annotations

import json

import pytest
from pytest_httpx import HTTPXMock

from arc_client.exceptions import (
    ArcConnectionError,
    ArcNotFoundError,
    ArcRateLimitError,
    ArcServerError,
    ArcValidationError,
)

from ._parity import MODES, call, make_client

# --- Bug 3: create_token(permissions=[]) -----------------------------------


@MODES
async def test_create_token_sends_empty_permissions(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    # Arc: absent -> read,write; [] -> RBAC-only token (auth_routes.go).
    httpx_mock.add_response(json={"success": True, "token": "tok"})
    async with make_client(async_mode) as client:
        await call(client.auth.create_token("rbac-only", permissions=[]))
    assert json.loads(httpx_mock.get_request().content) == {
        "name": "rbac-only",
        "permissions": [],
    }


@MODES
async def test_create_token_omits_unset_permissions(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(json={"success": True, "token": "tok"})
    async with make_client(async_mode) as client:
        await call(client.auth.create_token("default"))
    assert json.loads(httpx_mock.get_request().content) == {"name": "default"}


@MODES
async def test_update_token_sends_empty_permissions(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(json={"success": True})
    async with make_client(async_mode) as client:
        await call(client.auth.update_token(7, permissions=[]))
    assert json.loads(httpx_mock.get_request().content) == {"permissions": []}


@MODES
async def test_auth_rate_limit_not_reported_as_auth_failure(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(status_code=429, json={"error": "slow down"}, is_reusable=True)
    async with make_client(async_mode, max_retries=0) as client:
        with pytest.raises(ArcRateLimitError):
            await call(client.auth.list_tokens())
        with pytest.raises(ArcRateLimitError):
            await call(client.auth.create_token("x"))


# --- Item 14: /health, /ready, /ready/write ----------------------------------


@MODES
async def test_health_models_arc_fields(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(
        json={
            "status": "ok",
            "time": "2027-01-01T00:00:00Z",
            "uptime": "1h0m0s",
            "uptime_sec": 3600.5,
            "storage": {
                "cold": {
                    "backend": "s3",
                    "credentials": "refresher",
                    "state": "expired",
                    "consecutive_errors": 3,
                }
            },
            "license": {"tier": "enterprise", "status": "active", "source": "file"},
        }
    )
    async with make_client(async_mode) as client:
        health = await call(client.health())
    assert health.status == "ok"
    assert health.uptime_sec == 3600.5
    assert health.time == "2027-01-01T00:00:00Z"
    assert health.storage is not None
    assert health.storage["cold"].state == "expired"
    assert health.license is not None
    assert health.license.tier == "enterprise"
    assert health.version is None


@MODES
async def test_health_minimal_oss(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(json={"status": "ok", "uptime": "1s", "uptime_sec": 1})
    async with make_client(async_mode) as client:
        health = await call(client.health())
    assert health.storage is None
    assert health.license is None


@MODES
@pytest.mark.parametrize(
    ("path", "method"), [("/ready", "readiness"), ("/ready/write", "write_readiness")]
)
async def test_not_ready_reason_and_single_probe(
    httpx_mock: HTTPXMock, async_mode: bool, path: str, method: str
) -> None:
    httpx_mock.add_response(
        url=f"http://h:8000{path}",
        status_code=503,
        json={
            "status": "not_ready",
            "time": "2027-01-01T00:00:00Z",
            "reason": "storage_credentials_expired",
            "tier": "cold",
        },
    )
    async with make_client(async_mode, max_retries=3) as client:
        result = await call(getattr(client, method)())
    assert result.ready is False
    assert result.status == "not_ready"
    assert result.reason == "storage_credentials_expired"
    assert result.message == "storage_credentials_expired"
    assert result.tier == "cold"
    assert len(httpx_mock.get_requests()) == 1


@MODES
async def test_ready_write_not_write_target(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(
        status_code=503,
        json={"status": "not_write_target", "reason": "route writes to a writer"},
    )
    async with make_client(async_mode) as client:
        result = await call(client.write_readiness())
    assert result.ready is False
    assert result.status == "not_write_target"


@MODES
async def test_ready_ok(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(json={"status": "ready", "time": "t", "uptime_sec": 5})
    async with make_client(async_mode) as client:
        detail = await call(client.readiness())
    assert detail.ready is True
    assert detail.uptime_sec == 5
    assert httpx_mock.get_request().url.path == "/ready"


@MODES
async def test_ready_bool_is_single_probe(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(status_code=503, json={"status": "not_ready", "reason": "starting"})
    async with make_client(async_mode, max_retries=3) as client:
        assert await call(client.ready()) is False
    assert len(httpx_mock.get_requests()) == 1


@MODES
async def test_readiness_other_errors_raise(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(status_code=500, json={"error": "boom"})
    async with make_client(async_mode, max_retries=0) as client:
        with pytest.raises(ArcServerError):
            await call(client.readiness())


@MODES
async def test_readiness_unreachable_raises(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    import httpx

    httpx_mock.add_exception(httpx.ConnectError("refused"))
    async with make_client(async_mode, max_retries=0) as client:
        with pytest.raises(ArcConnectionError):
            await call(client.readiness())


# --- Item 13: measurement schema ---------------------------------------------


@MODES
async def test_get_measurement_schema(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(
        json={
            "database": "db",
            "measurement": "cpu",
            "fields": [{"name": "time", "type": "TIMESTAMP"}, {"name": "v", "type": "DOUBLE"}],
        }
    )
    async with make_client(async_mode) as client:
        schema = await call(client.databases.get_measurement_schema("db", "cpu"))
    request = httpx_mock.get_request()
    assert request.method == "GET"
    assert request.url.path == "/api/v1/databases/db/measurements/cpu/schema"
    assert [(f.name, f.type) for f in schema.fields] == [("time", "TIMESTAMP"), ("v", "DOUBLE")]


@MODES
async def test_get_measurement_schema_registry_disabled(
    httpx_mock: HTTPXMock, async_mode: bool
) -> None:
    httpx_mock.add_response(
        status_code=404, json={"error": "Field schema registry is disabled (query.stable_schema)"}
    )
    async with make_client(async_mode) as client:
        with pytest.raises(ArcNotFoundError):
            await call(client.databases.get_measurement_schema("db", "cpu"))


@MODES
async def test_rebuild_measurement_schema(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(
        status_code=202, json={"status": "queued", "database": "db", "measurement": "cpu"}
    )
    async with make_client(async_mode) as client:
        result = await call(client.databases.rebuild_measurement_schema("db", "cpu"))
    request = httpx_mock.get_request()
    assert request.method == "POST"
    assert request.url.path == "/api/v1/databases/db/measurements/cpu/schema/rebuild"
    assert result.status == "queued"


@MODES
async def test_rebuild_conflict_is_not_retried(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(status_code=409, json={"error": "already queued"})
    async with make_client(async_mode, max_retries=3) as client:
        with pytest.raises(ArcServerError) as info:
            await call(client.databases.rebuild_measurement_schema("db", "cpu"))
    assert info.value.status_code == 409


@MODES
@pytest.mark.parametrize("name", ["", "a/b", ".hidden"])
async def test_schema_rejects_unsafe_segments(async_mode: bool, name: str) -> None:
    async with make_client(async_mode) as client:
        with pytest.raises(ArcValidationError):
            await call(client.databases.get_measurement_schema("db", name))
