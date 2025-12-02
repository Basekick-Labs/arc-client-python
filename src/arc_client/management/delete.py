"""Synchronous delete client for Arc."""

from __future__ import annotations

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcValidationError
from arc_client.http.sync_http import SyncHTTPClient
from arc_client.models.delete import DeleteConfigResponse, DeleteResponse


class DeleteClient:
    """Synchronous client for data deletion operations.

    Delete operations use a file-rewrite strategy. Always use dry_run=True first.
    """

    def __init__(self, http: SyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    def delete(
        self,
        database: str,
        measurement: str,
        where: str,
        dry_run: bool = True,
        confirm: bool = False,
    ) -> DeleteResponse:
        """Delete data matching the WHERE clause."""
        if not database:
            raise ArcValidationError("database is required")
        if not measurement:
            raise ArcValidationError("measurement is required")
        if not where or not where.strip():
            raise ArcValidationError(
                "where clause is required. Use '1=1' with confirm=True for full delete"
            )

        payload = {
            "database": database,
            "measurement": measurement,
            "where": where,
            "dry_run": dry_run,
            "confirm": confirm,
        }

        try:
            response = self._http.post("/api/v1/delete", json=payload)
            data = response.json()
            return DeleteResponse(**data)
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Delete operation failed: {e}") from e

    def get_config(self) -> DeleteConfigResponse:
        """Get delete configuration settings."""
        try:
            response = self._http.get("/api/v1/delete/config")
            data = response.json()
            return DeleteConfigResponse(**data)
        except Exception as e:
            raise ArcError(f"Failed to get delete config: {e}") from e
