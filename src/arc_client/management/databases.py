"""Synchronous databases management client for Arc."""

from __future__ import annotations

from typing import Any

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcValidationError
from arc_client.http.sync_http import SyncHTTPClient
from arc_client.models.database import (
    DatabaseInfo,
    DatabaseListResponse,
    MeasurementListResponse,
)
from arc_client.validation import validate_name


class DatabaseClient:
    """Manage Arc databases (list/get/create/delete).

    Create and delete require an admin token. Delete additionally requires the
    server to have delete operations enabled and an explicit ``confirm=True``.

    Example:
        >>> client.databases.create("metrics")
        >>> for db in client.databases.list().databases:
        ...     print(db.name, db.measurement_count)
    """

    def __init__(self, http: SyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    def list(self) -> DatabaseListResponse:
        """List all databases."""
        try:
            response = self._http.get("/api/v1/databases")
            return DatabaseListResponse.model_validate(response.json())
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to list databases: {e}") from e

    def get(self, name: str) -> DatabaseInfo:
        """Get a single database by name."""
        validate_name(name, "database")
        try:
            response = self._http.get(f"/api/v1/databases/{name}")
            return DatabaseInfo.model_validate(response.json())
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to get database {name!r}: {e}") from e

    def list_measurements(self, name: str) -> MeasurementListResponse:
        """List measurements within a database."""
        validate_name(name, "database")
        try:
            response = self._http.get(f"/api/v1/databases/{name}/measurements")
            return MeasurementListResponse.model_validate(response.json())
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to list measurements for {name!r}: {e}") from e

    def create(self, name: str) -> DatabaseInfo:
        """Create a database (admin).

        Raises:
            ArcValidationError: If the name is invalid.
            ArcError: If creation fails (e.g. the database already exists, 409).
        """
        validate_name(name, "database")
        try:
            response = self._http.post("/api/v1/databases", json={"name": name})
            return DatabaseInfo.model_validate(response.json())
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to create database {name!r}: {e}") from e

    def delete(self, name: str, confirm: bool = False) -> dict[str, Any]:
        """Delete a database (admin).

        Requires ``confirm=True`` and server-side delete operations to be
        enabled (otherwise the server returns 403).

        Returns:
            The server's response dict, e.g. ``{"message": ..., "files_deleted": N}``.
        """
        validate_name(name, "database")
        if not confirm:
            raise ArcValidationError(
                "Refusing to delete database without confirm=True. This is destructive."
            )
        try:
            response = self._http.delete(f"/api/v1/databases/{name}", params={"confirm": "true"})
            body: dict[str, Any] = response.json()
            return body
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to delete database {name!r}: {e}") from e
