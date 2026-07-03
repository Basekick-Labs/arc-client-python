"""Asynchronous databases management client for Arc."""

from __future__ import annotations

from typing import Any

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcValidationError
from arc_client.http.async_http import AsyncHTTPClient
from arc_client.models.database import (
    DatabaseInfo,
    DatabaseListResponse,
    MeasurementListResponse,
)
from arc_client.validation import validate_name


class AsyncDatabaseClient:
    """Async twin of :class:`arc_client.management.databases.DatabaseClient`."""

    def __init__(self, http: AsyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    async def list(self) -> DatabaseListResponse:
        """List all databases."""
        try:
            response = await self._http.get("/api/v1/databases")
            return DatabaseListResponse.model_validate(response.json())
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to list databases: {e}") from e

    async def get(self, name: str) -> DatabaseInfo:
        """Get a single database by name."""
        validate_name(name, "database")
        try:
            response = await self._http.get(f"/api/v1/databases/{name}")
            return DatabaseInfo.model_validate(response.json())
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to get database {name!r}: {e}") from e

    async def list_measurements(self, name: str) -> MeasurementListResponse:
        """List measurements within a database."""
        validate_name(name, "database")
        try:
            response = await self._http.get(f"/api/v1/databases/{name}/measurements")
            return MeasurementListResponse.model_validate(response.json())
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to list measurements for {name!r}: {e}") from e

    async def create(self, name: str) -> DatabaseInfo:
        """Create a database (admin)."""
        validate_name(name, "database")
        try:
            response = await self._http.post("/api/v1/databases", json={"name": name})
            return DatabaseInfo.model_validate(response.json())
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to create database {name!r}: {e}") from e

    async def delete(self, name: str, confirm: bool = False) -> dict[str, Any]:
        """Delete a database (admin). Requires confirm=True."""
        validate_name(name, "database")
        if not confirm:
            raise ArcValidationError(
                "Refusing to delete database without confirm=True. This is destructive."
            )
        try:
            response = await self._http.delete(
                f"/api/v1/databases/{name}", params={"confirm": "true"}
            )
            body: dict[str, Any] = response.json()
            return body
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to delete database {name!r}: {e}") from e
