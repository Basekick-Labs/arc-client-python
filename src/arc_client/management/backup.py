"""Synchronous backup/restore client for Arc (enterprise).

Wraps ``/api/v1/backup``. Requires an admin token. Backup/restore run
asynchronously server-side; poll :meth:`status` for progress. Responses are
returned as raw dicts (operator diagnostics, not a promised contract).
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcValidationError
from arc_client.http.sync_http import SyncHTTPClient

RESTORE_MODES = ("merge", "replace")


def _scope(databases: Optional[Sequence[str]]) -> Optional[list[str]]:
    if databases is None:
        return None
    if isinstance(databases, str):
        raise ArcValidationError("databases must be a list of names, not a string")
    names = list(databases)
    if not names:
        # Arc reads an empty list as a whole-instance backup; refuse it rather
        # than silently widen a scope the caller meant to narrow.
        raise ArcValidationError("databases must name at least one database (omit it for all)")
    if any(not isinstance(n, str) or not n for n in names):
        raise ArcValidationError("databases must contain non-empty names")
    return names


def _restore_mode(mode: Optional[str]) -> Optional[str]:
    if mode is None:
        return None
    normalized = mode.strip().lower()
    if normalized not in RESTORE_MODES:
        raise ArcValidationError(f"mode must be one of {RESTORE_MODES}, got {mode!r}")
    return normalized


class BackupClient:
    """Create, list, inspect, delete backups and restore from them (enterprise)."""

    def __init__(self, http: SyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            response = getattr(self._http, method)(path, **kwargs)
            return response.json()
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Backup request failed ({method.upper()} {path}): {e}") from e

    def create(
        self,
        include_metadata: Optional[bool] = None,
        include_config: Optional[bool] = None,
        databases: Optional[Sequence[str]] = None,
    ) -> Any:
        """Start a backup (async). Returns 202 with a running status.

        Args:
            include_metadata: Include the SQLite metadata database. Default
                true for a whole-instance backup; a scoped backup cannot
                carry it (Arc answers 400 to ``True`` with ``databases``).
            include_config: Include ``arc.toml``. Default true, except false
                for a scoped backup or a remote backup target.
            databases: Scope the backup to these databases (at most 256).
                Omit for a whole-instance backup. Needs Arc 2027.01.1+; an
                older server ignores the field and backs up everything.

        Raises:
            ArcValidationError: ``databases`` is empty or holds an empty name.
        """
        scope = _scope(databases)
        body: dict[str, Any] = {}
        if include_metadata is not None:
            body["include_metadata"] = include_metadata
        if include_config is not None:
            body["include_config"] = include_config
        if scope is not None:
            body["databases"] = scope
        return self._json("post", "/api/v1/backup/", json=body or None)

    def list(self, include_foreign: Optional[bool] = None) -> Any:
        """List available backups.

        Args:
            include_foreign: Also list backups another Arc instance wrote to
                the same destination, each marked ``foreign_owner``. Needed to
                find a backup after restoring onto fresh hardware, where every
                backup reads as foreign; the response's ``filtered_foreign``
                counts the ones left out by default. Needs Arc 2027.01.1+.
        """
        params = {"include_foreign": "true"} if include_foreign else None
        return self._json("get", "/api/v1/backup/", params=params)

    def status(self) -> Any:
        """Get the current backup/restore progress (or idle)."""
        return self._json("get", "/api/v1/backup/status")

    def get(self, backup_id: str) -> Any:
        """Get a backup manifest by id."""
        return self._json("get", f"/api/v1/backup/{backup_id}")

    def delete(self, backup_id: str) -> Any:
        """Delete a backup by id."""
        return self._json("delete", f"/api/v1/backup/{backup_id}")

    def restore(
        self,
        backup_id: str,
        confirm: bool = False,
        restore_data: Optional[bool] = None,
        restore_metadata: Optional[bool] = None,
        restore_config: Optional[bool] = None,
        mode: Optional[str] = None,
    ) -> Any:
        """Restore from a backup (async). Requires confirm=True.

        Args:
            mode: ``"merge"`` adds the backup's files to the current ones
                (files deleted since the backup come back). ``"replace"``
                first removes the current files of each restored database
                through the cluster manifest; cluster nodes only (a standalone
                server answers 400). Omitted, Arc picks: merge, except that a
                scoped backup restored on a cluster node defaults to replace.
                The response echoes the effective mode. Needs Arc 2027.01.1+.

        Raises:
            ArcValidationError: If confirm is not True (restore is destructive)
                or ``mode`` is not ``"merge"`` or ``"replace"``.
        """
        if not confirm:
            raise ArcValidationError(
                "Refusing to restore without confirm=True. This overwrites data."
            )
        restore_mode = _restore_mode(mode)
        body: dict[str, Any] = {"backup_id": backup_id, "confirm": True}
        if restore_data is not None:
            body["restore_data"] = restore_data
        if restore_metadata is not None:
            body["restore_metadata"] = restore_metadata
        if restore_config is not None:
            body["restore_config"] = restore_config
        if restore_mode is not None:
            body["mode"] = restore_mode
        return self._json("post", "/api/v1/backup/restore", json=body)
