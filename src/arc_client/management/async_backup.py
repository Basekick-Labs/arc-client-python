"""Asynchronous backup/restore client for Arc (enterprise).

Wraps ``/api/v1/backup``. Requires an admin token. Backup/restore run
asynchronously server-side; poll :meth:`status` for progress.
"""

from __future__ import annotations

from typing import Any, Optional

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcValidationError
from arc_client.http.async_http import AsyncHTTPClient


class AsyncBackupClient:
    """Create, list, inspect, delete backups and restore from them (enterprise)."""

    def __init__(self, http: AsyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    async def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            response = await getattr(self._http, method)(path, **kwargs)
            return response.json()
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Backup request failed ({method.upper()} {path}): {e}") from e

    async def create(
        self,
        include_metadata: Optional[bool] = None,
        include_config: Optional[bool] = None,
    ) -> Any:
        """Start a backup (async). Returns 202 with a running status."""
        body: dict[str, Any] = {}
        if include_metadata is not None:
            body["include_metadata"] = include_metadata
        if include_config is not None:
            body["include_config"] = include_config
        return await self._json("post", "/api/v1/backup/", json=body or None)

    async def list(self) -> Any:
        """List available backups."""
        return await self._json("get", "/api/v1/backup/")

    async def status(self) -> Any:
        """Get the current backup/restore progress (or idle)."""
        return await self._json("get", "/api/v1/backup/status")

    async def get(self, backup_id: str) -> Any:
        """Get a backup manifest by id."""
        return await self._json("get", f"/api/v1/backup/{backup_id}")

    async def delete(self, backup_id: str) -> Any:
        """Delete a backup by id."""
        return await self._json("delete", f"/api/v1/backup/{backup_id}")

    async def restore(
        self,
        backup_id: str,
        confirm: bool = False,
        restore_data: Optional[bool] = None,
        restore_metadata: Optional[bool] = None,
        restore_config: Optional[bool] = None,
    ) -> Any:
        """Restore from a backup (async). Requires confirm=True.

        Raises:
            ArcValidationError: If confirm is not True (restore is destructive).
        """
        if not confirm:
            raise ArcValidationError(
                "Refusing to restore without confirm=True. This overwrites data."
            )
        body: dict[str, Any] = {"backup_id": backup_id, "confirm": True}
        if restore_data is not None:
            body["restore_data"] = restore_data
        if restore_metadata is not None:
            body["restore_metadata"] = restore_metadata
        if restore_config is not None:
            body["restore_config"] = restore_config
        return await self._json("post", "/api/v1/backup/restore", json=body)
