"""HTTP round-trip tests: bulk imports and databases CRUD."""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcClient
from arc_client.exceptions import ArcIngestionError, ArcValidationError


@pytest.fixture
def client() -> ArcClient:
    c = ArcClient(host="h", token="t", database="db")
    yield c
    c.close()


# --- Imports ---


def test_import_csv_multipart_and_params(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(
        json={
            "status": "ok",
            "result": {
                "database": "db",
                "measurement": "cpu",
                "rows_imported": 42,
                "partitions_created": 2,
                "columns": ["time", "usage"],
                "duration_ms": 10,
            },
        }
    )

    result = client.import_.import_csv(b"time,usage\n1,2.0\n", measurement="cpu", delimiter=",")

    assert result.rows_imported == 42
    assert result.measurement == "cpu"
    request = httpx_mock.get_requests()[0]
    assert request.url.path == "/api/v1/import/csv"
    assert b'name="file"' in request.read()  # multipart field
    assert request.url.params["measurement"] == "cpu"
    assert request.headers["x-arc-database"] == "db"


def test_import_tle_sends_measurement_header(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(
        json={
            "status": "ok",
            "result": {
                "database": "db",
                "measurement": "satellite_tle",
                "satellite_count": 3,
                "rows_imported": 3,
                "duration_ms": 5,
            },
        }
    )

    result = client.import_.import_tle(b"1 ...\n2 ...\n")

    assert result.satellite_count == 3
    request = httpx_mock.get_requests()[0]
    assert request.headers["x-arc-measurement"] == "satellite_tle"


def test_import_lp_parses_result(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(
        json={
            "status": "ok",
            "result": {
                "database": "db",
                "measurements": ["cpu", "mem"],
                "rows_imported": 100,
                "precision": "ns",
                "duration_ms": 12,
            },
        }
    )

    result = client.import_.import_lp(b"cpu v=1 1\n", precision="ns")

    assert result.measurements == ["cpu", "mem"]
    assert result.rows_imported == 100


def test_import_unexpected_response_raises(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(json={"status": "ok"})  # no "result"

    with pytest.raises(ArcIngestionError):
        client.import_.import_csv(b"x", measurement="cpu")


# --- Databases ---


def test_databases_list(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(
        json={
            "databases": [{"name": "db", "measurement_count": 3}],
            "count": 1,
        }
    )

    result = client.databases.list()

    assert result.count == 1
    assert result.databases[0].name == "db"


def test_databases_create(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(
        status_code=201, json={"name": "newdb", "measurement_count": 0, "created_at": "now"}
    )

    result = client.databases.create("newdb")

    assert result.name == "newdb"
    request = httpx_mock.get_requests()[0]
    assert request.url.path == "/api/v1/databases"


def test_databases_create_invalid_name(client: ArcClient) -> None:
    with pytest.raises(ArcValidationError):
        client.databases.create("1bad name!")


def test_databases_delete_requires_confirm(client: ArcClient) -> None:
    with pytest.raises(ArcValidationError):
        client.databases.delete("db", confirm=False)


def test_databases_delete_sends_confirm(httpx_mock: HTTPXMock, client: ArcClient) -> None:
    httpx_mock.add_response(json={"message": "deleted", "files_deleted": 5})

    result = client.databases.delete("db", confirm=True)

    assert result["files_deleted"] == 5
    request = httpx_mock.get_requests()[0]
    assert request.method == "DELETE"
    assert request.url.params["confirm"] == "true"
