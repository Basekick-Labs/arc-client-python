"""Arc's answers for the database listing endpoints.

The three ``GET /api/v1/databases*`` routes require read permission and, on a
server that restricts reads per database, a grant for the database asked for.
A 401, the two different 403s and a 404 are four different answers and must
reach callers as four different exceptions.
"""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcClient, AsyncArcClient
from arc_client.exceptions import (
    ArcAuthenticationError,
    ArcError,
    ArcNotFoundError,
    ArcPermissionDataUnavailableError,
    ArcPermissionError,
    ArcScopedAccessError,
)

# The bodies the server actually sends. Every 403 here shares a status code
# and differs only in message, so the message is the whole discriminator.
NO_TOKEN = {"success": False, "error": "Authentication required"}
BAD_TOKEN = {"success": False, "error": "Invalid or expired token"}
# Coarse refusals: the token carries no read permission at all.
NO_READ_PERMISSION = {"success": False, "error": "token does not have 'read' permission"}
OSS_NO_READ = {"success": False, "error": "Permission denied: read required"}
# The read middleware's RBAC denial, which is what these routes return in
# practice. The name is empty on the list-everything route, which sends none.
SCOPED_ALL = {"success": False, "error": "no permission for read on database ''"}
SCOPED_NAMED = {"success": False, "error": "no permission for read on database 'metrics'"}
# The handler gates' own wording, reachable when they check a different
# database/measurement pair than the middleware did.
GATE_SCOPED_ALL = {"error": "access denied: no read permission for database '*'"}
GATE_SCOPED_NAMED = {"error": "access denied: no read permission for database 'metrics'"}
# Arc could not load the token's grants and refused rather than guess.
PERMISSION_DATA_UNAVAILABLE = {"success": False, "error": "permission data unavailable"}
NOT_FOUND = {"error": "Database 'ghost' not found"}


@pytest.fixture
def client() -> ArcClient:
    c = ArcClient(host="h", token="t", database="db", max_retries=0)
    yield c
    c.close()


# --- Exception hierarchy -----------------------------------------------------


def test_scoped_access_is_a_permission_error() -> None:
    """Callers that only care about "refused" can catch one class."""
    assert issubclass(ArcScopedAccessError, ArcPermissionError)
    assert issubclass(ArcPermissionError, ArcAuthenticationError)
    assert issubclass(ArcPermissionError, ArcError)


def test_scoped_access_carries_the_database() -> None:
    error = ArcScopedAccessError("refused", database="metrics")
    assert error.database == "metrics"
    assert ArcScopedAccessError("refused").database is None


# --- list() ------------------------------------------------------------------


def test_list_raises_scoped_for_a_tenant_scoped_token(
    httpx_mock: HTTPXMock, client: ArcClient
) -> None:
    """Arc refuses the list-everything route rather than filtering it."""
    httpx_mock.add_response(status_code=403, json=SCOPED_ALL)

    with pytest.raises(ArcScopedAccessError) as excinfo:
        client.databases.list()

    # Arc sends no database name on this route, so the advice must be
    # "name one", not "you have no grant for ''".
    assert excinfo.value.database == ""
    message = str(excinfo.value)
    assert "scoped to specific databases" in message
    assert "will not list them all" in message
    assert "''" not in message.replace("database ''", "")


def test_list_raises_permission_error_without_read(
    httpx_mock: HTTPXMock, client: ArcClient
) -> None:
    """A token with no read permission is a different problem."""
    httpx_mock.add_response(status_code=403, json=NO_READ_PERMISSION)

    with pytest.raises(ArcPermissionError) as excinfo:
        client.databases.list()

    assert not isinstance(excinfo.value, ArcScopedAccessError)
    assert "Permission denied" in str(excinfo.value)


@pytest.mark.parametrize("body", [NO_TOKEN, BAD_TOKEN])
def test_list_raises_authentication_error_on_401(
    httpx_mock: HTTPXMock, client: ArcClient, body: dict
) -> None:
    httpx_mock.add_response(status_code=401, json=body)

    with pytest.raises(ArcAuthenticationError) as excinfo:
        client.databases.list()

    # 401 must not surface as a permission problem: re-authenticating fixes it,
    # a different token's permissions do not.
    assert not isinstance(excinfo.value, ArcPermissionError)
    assert "Authentication failed" in str(excinfo.value)


def test_list_success_unchanged(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(
        json={"databases": [{"name": "metrics", "measurement_count": 2}], "count": 1}
    )

    result = client.databases.list()

    assert result.count == 1
    assert result.databases[0].name == "metrics"


# --- get() and list_measurements() ------------------------------------------


@pytest.mark.parametrize("method", ["get", "list_measurements"])
def test_per_database_routes_raise_scoped_and_name_the_database(
    httpx_mock: HTTPXMock, client: ArcClient, method: str
) -> None:
    httpx_mock.add_response(status_code=403, json=SCOPED_NAMED)

    with pytest.raises(ArcScopedAccessError) as excinfo:
        getattr(client.databases, method)("metrics")

    assert excinfo.value.database == "metrics"
    assert "metrics" in str(excinfo.value)


@pytest.mark.parametrize("method", ["get", "list_measurements"])
def test_per_database_routes_keep_404_separate(
    httpx_mock: HTTPXMock, client: ArcClient, method: str
) -> None:
    """ "You may not read it" and "it is not there" are different answers."""
    httpx_mock.add_response(status_code=404, json=NOT_FOUND)

    with pytest.raises(ArcNotFoundError) as excinfo:
        getattr(client.databases, method)("ghost")

    assert not isinstance(excinfo.value, ArcPermissionError)
    assert "not found" in str(excinfo.value)


@pytest.mark.parametrize("method", ["get", "list_measurements"])
def test_per_database_routes_raise_permission_error_without_read(
    httpx_mock: HTTPXMock, client: ArcClient, method: str
) -> None:
    httpx_mock.add_response(status_code=403, json=NO_READ_PERMISSION)

    with pytest.raises(ArcPermissionError) as excinfo:
        getattr(client.databases, method)("metrics")

    assert not isinstance(excinfo.value, ArcScopedAccessError)


# --- 403s that are not the per-database denial -------------------------------


def test_unknown_403_body_is_not_reported_as_scoped(
    httpx_mock: HTTPXMock, client: ArcClient
) -> None:
    """Telling a caller to name a database it already named is worse than
    naming the permission, so only the recognised shape classifies as scoped."""
    httpx_mock.add_response(status_code=403, json={"error": "Delete operations are disabled."})

    with pytest.raises(ArcPermissionError) as excinfo:
        client.databases.get("metrics")

    assert not isinstance(excinfo.value, ArcScopedAccessError)


def test_license_403_still_maps_to_license_error(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    """The pre-existing enterprise-license 403 keeps its own exception."""
    from arc_client.exceptions import ArcLicenseError

    httpx_mock.add_response(
        status_code=403,
        json={"error": "This feature requires an enterprise license"},
    )

    with pytest.raises(ArcLicenseError):
        client.databases.list()


# --- async twin --------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_list_raises_scoped(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=403, json=SCOPED_ALL)

    c = AsyncArcClient(host="h", token="t", database="db", max_retries=0)
    try:
        with pytest.raises(ArcScopedAccessError) as excinfo:
            await c.databases.list()
        assert excinfo.value.database == ""
    finally:
        await c.close()


@pytest.mark.asyncio
async def test_async_list_measurements_raises_scoped_and_names_database(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(status_code=403, json=SCOPED_NAMED)

    c = AsyncArcClient(host="h", token="t", database="db", max_retries=0)
    try:
        with pytest.raises(ArcScopedAccessError) as excinfo:
            await c.databases.list_measurements("metrics")
        assert excinfo.value.database == "metrics"
    finally:
        await c.close()


# --- the full 403 message matrix ---------------------------------------------


@pytest.mark.parametrize(
    ("body", "expected", "expected_database"),
    [
        # Arc's read middleware, which is what these routes use.
        (SCOPED_NAMED, ArcScopedAccessError, "metrics"),
        (SCOPED_ALL, ArcScopedAccessError, ""),
        # The handler gates' own wording.
        (GATE_SCOPED_NAMED, ArcScopedAccessError, "metrics"),
        (GATE_SCOPED_ALL, ArcScopedAccessError, "*"),
        # The token carries no read permission at all — a different problem,
        # and naming a database will not help.
        (NO_READ_PERMISSION, ArcPermissionError, None),
        (OSS_NO_READ, ArcPermissionError, None),
        # Arc could not read its own grants: a server fault, not the token's.
        (PERMISSION_DATA_UNAVAILABLE, ArcPermissionDataUnavailableError, None),
        # An unrecognised 403 falls back to the permission message, which is
        # safe to say about any refusal.
        ({"error": "Delete operations are disabled."}, ArcPermissionError, None),
    ],
)
def test_every_forbidden_body_maps_to_its_own_exception(
    httpx_mock: HTTPXMock,
    client: ArcClient,
    body: dict,
    expected: type,
    expected_database: object,
) -> None:
    httpx_mock.add_response(status_code=403, json=body)

    with pytest.raises(expected) as excinfo:
        client.databases.get("metrics")

    # The mapping must be exact, not just "a subclass of something".
    assert type(excinfo.value) is expected
    if expected_database is not None:
        assert excinfo.value.database == expected_database


def test_unavailable_is_not_reported_as_scoped(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    """A broken permission store must not send the caller after a database."""
    httpx_mock.add_response(status_code=403, json=PERMISSION_DATA_UNAVAILABLE)

    with pytest.raises(ArcPermissionDataUnavailableError) as excinfo:
        client.databases.list()

    assert not isinstance(excinfo.value, ArcScopedAccessError)
    message = str(excinfo.value)
    assert "server-side fault" in message
    assert "scoped" not in message
