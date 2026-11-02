"""Synchronous delete client for Arc."""

from __future__ import annotations

from typing import Any

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcValidationError
from arc_client.http.sync_http import SyncHTTPClient
from arc_client.models.delete import DeleteConfigResponse, DeleteResponse

# Arc may wait up to 30 s inside a delete (internal/api/delete_records.go
# deleteLocalWait; the database delete's peer flush), so a client timeout near
# 30 s reports a delete that ran as a failure. Deletes get at least this much.
DELETE_TIMEOUT_FLOOR = 90.0


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
        allow_unreachable: bool = False,
    ) -> DeleteResponse:
        """Delete data matching the WHERE clause.

        Args:
            database: Database holding the measurement.
            measurement: Measurement to delete from.
            where: SQL WHERE clause selecting the rows.
            dry_run: Preview only (default). Set False to delete.
            confirm: Required by Arc for any delete that is not a dry run.
                In a cluster, send it to the primary writer: other nodes
                refuse a data delete (503 "not primary writer") instead of
                forwarding it, unlike writes.
            allow_unreachable: Cluster only; relaxes a safety check. Without
                it, Arc refuses the DELETE (503) when files the cluster
                manifest lists are not on this node yet (stream takeover), or
                when other nodes of a shared-storage cluster could not flush
                the rows they buffer for this measurement first. With it, the
                DELETE runs anyway and the rows in those files or on those
                nodes are NOT deleted; the response names them in
                ``unreachable_files`` and ``unflushed_nodes``. Sent only when
                True. Needs Arc 2027.01.1+; older servers ignore it. Either
                way Arc first waits for those files or nodes (about 30-40 s
                for a node that does not answer); a real delete gets a
                request timeout of at least 90 s to outlast that.
        """
        if not database:
            raise ArcValidationError("database is required")
        if not measurement:
            raise ArcValidationError("measurement is required")
        if not where or not where.strip():
            raise ArcValidationError(
                "where clause is required. Use '1=1' with confirm=True for full delete"
            )

        payload: dict[str, Any] = {
            "database": database,
            "measurement": measurement,
            "where": where,
            "dry_run": dry_run,
            "confirm": confirm,
        }
        if allow_unreachable:
            payload["allow_unreachable"] = True

        try:
            timeout = None if dry_run else max(self._config.timeout, DELETE_TIMEOUT_FLOOR)
            response = self._http.post("/api/v1/delete", json=payload, timeout=timeout)
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
