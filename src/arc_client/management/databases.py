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
    MeasurementSchema,
    SchemaRebuildResponse,
)
from arc_client.validation import path_segment, validate_name


class DatabaseClient:
    """Manage Arc databases (list/get/create/delete).

    Create and delete require an admin token. Delete additionally requires the
    server to have delete operations enabled and an explicit ``confirm=True``.

    The three read methods need a token with read permission. On a server that
    restricts reads per database they also need a grant for the database in
    question, and :meth:`list` needs one covering every database — Arc refuses
    rather than returning a filtered list, matching ``SHOW DATABASES``. That
    refusal raises :class:`~arc_client.exceptions.ArcScopedAccessError`, which
    is a normal state for a tenant-scoped token rather than a fault to retry;
    a token with no read permission at all raises
    :class:`~arc_client.exceptions.ArcPermissionError`.

    Example:
        >>> client.databases.create("metrics")
        >>> for db in client.databases.list().databases:
        ...     print(db.name, db.measurement_count)
    """

    def __init__(self, http: SyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    def list(self) -> DatabaseListResponse:
        """List all databases.

        Needs a token with read permission. Where the server restricts reads
        per database, it also needs a grant covering every database: Arc
        refuses this call for a token scoped to particular databases rather
        than returning a filtered list, the same way ``SHOW DATABASES`` does.

        Raises:
            ArcScopedAccessError: The token is scoped to specific databases
                (``database`` is ``"*"``). Expected for a tenant-scoped
                token — name a database with :meth:`get` or
                :meth:`list_measurements` instead of retrying.
            ArcPermissionError: The token carries no read permission.
            ArcAuthenticationError: The token is missing, invalid or expired.
        """
        try:
            response = self._http.get("/api/v1/databases")
            return DatabaseListResponse.model_validate(response.json())
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to list databases: {e}") from e

    def get(self, name: str) -> DatabaseInfo:
        """Get a single database by name.

        Raises:
            ArcScopedAccessError: The token holds no read grant for ``name``
                (carried on ``database``). It may be able to read others.
            ArcPermissionError: The token carries no read permission.
            ArcAuthenticationError: The token is missing, invalid or expired.
            ArcNotFoundError: No such database. Kept separate from the
                permission errors: "you may not read it" and "it is not
                there" are different answers.
            ArcValidationError: The name is not a valid database name.
        """
        validate_name(name, "database")
        try:
            response = self._http.get(f"/api/v1/databases/{name}")
            return DatabaseInfo.model_validate(response.json())
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to get database {name!r}: {e}") from e

    def list_measurements(self, name: str) -> MeasurementListResponse:
        """List measurements within a database.

        Raises:
            ArcScopedAccessError: The token holds no read grant for ``name``
                (carried on ``database``). Ask for a database it is granted.
            ArcPermissionError: The token carries no read permission.
            ArcAuthenticationError: The token is missing, invalid or expired.
            ArcNotFoundError: No such database.
            ArcValidationError: The name is not a valid database name.
        """
        validate_name(name, "database")
        try:
            response = self._http.get(f"/api/v1/databases/{name}/measurements")
            return MeasurementListResponse.model_validate(response.json())
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to list measurements for {name!r}: {e}") from e

    def get_measurement_schema(self, database: str, measurement: str) -> MeasurementSchema:
        """Get a measurement's registered field schema.

        ``GET /api/v1/databases/:database/measurements/:measurement/schema``
        returns the columns and types every query binds for the measurement,
        regardless of the time range it reads (Arc's ``query.stable_schema``
        registry, built by ingest or by :meth:`rebuild_measurement_schema`).
        Needs read permission on the measurement.

        Raises:
            ArcNotFoundError: The registry is disabled on the server, or no
                schema is registered for this measurement yet.
            ArcPermissionError: The token may not read the measurement.
            ArcServerError: HTTP 503 when the registry could not be read.
            ArcValidationError: A name cannot be a URL path segment.
        """
        path = (
            f"/api/v1/databases/{path_segment(database, 'database')}"
            f"/measurements/{path_segment(measurement, 'measurement')}/schema"
        )
        try:
            response = self._http.get(path)
            return MeasurementSchema.model_validate(response.json())
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to get schema for {database}.{measurement}: {e}") from e

    def rebuild_measurement_schema(self, database: str, measurement: str) -> SchemaRebuildResponse:
        """Queue a rebuild of a measurement's registered field schema (admin).

        ``POST .../measurements/:measurement/schema/rebuild`` samples a bounded
        set of the measurement's files and merges what it finds into the
        registered schema (fields are added, never dropped). Arc answers 202
        once the rebuild is queued; it runs in the background.

        Raises:
            ArcNotFoundError: The registry is disabled on the server.
            ArcServerError: HTTP 409 when a rebuild for this measurement is
                already queued or running; HTTP 503 when the rebuild queue is
                full or rebuilds are unavailable on this node.
            ArcPermissionError: The token is not an admin token.
            ArcValidationError: A name cannot be a URL path segment.
        """
        path = (
            f"/api/v1/databases/{path_segment(database, 'database')}"
            f"/measurements/{path_segment(measurement, 'measurement')}/schema/rebuild"
        )
        try:
            response = self._http.post(path)
            return SchemaRebuildResponse.model_validate(response.json())
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to rebuild schema for {database}.{measurement}: {e}") from e

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
