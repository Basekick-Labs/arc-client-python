"""Synchronous compaction-management client for Arc.

Wraps ``/api/v1/compaction``. Every route requires an admin token, including
the read-only ones: Arc made the reads admin-only because ``candidates``,
``jobs`` and ``history`` name the databases and measurements of every tenant
(Arc ``internal/api/compaction.go`` ``RegisterRoutes``). A non-admin token
raises :class:`~arc_client.exceptions.ArcPermissionError`.

Responses are returned as raw dicts: they are operator diagnostics whose
fields Arc extends between releases, not a promised contract.
"""

from __future__ import annotations

from typing import Any, Optional

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcValidationError
from arc_client.http.sync_http import SyncHTTPClient


def _check_positive(value: Optional[int], name: str) -> None:
    if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 1):
        raise ArcValidationError(f"{name} must be a positive integer")


class CompactionClient:
    """Inspect and trigger compaction (admin)."""

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
            raise ArcError(f"Compaction request failed ({method.upper()} {path}): {e}") from e

    def status(self) -> Any:
        """Get compaction manager and scheduler status."""
        return self._json("get", "/api/v1/compaction/status")

    def stats(self) -> Any:
        """Get raw compaction statistics."""
        return self._json("get", "/api/v1/compaction/stats")

    def candidates(self) -> Any:
        """List partitions that are candidates for compaction."""
        return self._json("get", "/api/v1/compaction/candidates")

    def jobs(self) -> Any:
        """List active compaction jobs."""
        return self._json("get", "/api/v1/compaction/jobs")

    def history(self, limit: Optional[int] = None, cycle_id: Optional[int] = None) -> Any:
        """Get recent compaction job attempts (default 10).

        Args:
            limit: Maximum records to return. Arc caps it at its history size.
            cycle_id: Only the attempts of this compaction cycle (the id
                :meth:`trigger` returns). Needs Arc 2027.01.1+; older servers
                ignore it and return every recent job.

        Raises:
            ArcValidationError: ``cycle_id`` is not a positive integer.
        """
        _check_positive(cycle_id, "cycle_id")
        params = {k: v for k, v in {"limit": limit, "cycle_id": cycle_id}.items() if v is not None}
        return self._json("get", "/api/v1/compaction/history", params=params or None)

    def cycles(self, limit: Optional[int] = None) -> Any:
        """List the retained compaction cycles, newest first.

        History is held in memory on the node that ran the cycles (the
        compactor lease holder on a cluster) and is lost on restart.
        Needs Arc 2027.01.1+.
        """
        params = {"limit": limit} if limit is not None else None
        return self._json("get", "/api/v1/compaction/cycles", params=params)

    def cycle(self, cycle_id: int) -> Any:
        """Get one compaction cycle by id. Needs Arc 2027.01.1+.

        Raises:
            ArcValidationError: ``cycle_id`` is not a positive integer.
            ArcNotFoundError: This node retains no record of the cycle. Arc's
                404 body names the retained window and, on a cluster, the
                compactor ``lease_holder`` where the cycle ran.
        """
        _check_positive(cycle_id, "cycle_id")
        return self._json("get", f"/api/v1/compaction/cycles/{cycle_id}")

    def too_wide(self) -> Any:
        """List partitions this node remembers as too wide to compact.

        Compaction skips them until the settings change or they are cleared
        with :meth:`clear_too_wide`. On a cluster, the compactor node's list
        is the one cycles use. Needs Arc 2027.01.1+.
        """
        return self._json("get", "/api/v1/compaction/too-wide")

    def clear_too_wide(self, tier: Optional[str] = None, partition: Optional[str] = None) -> Any:
        """Forget remembered too-wide partitions so the next cycle retries them.

        Clears every entry, or only those matching ``tier`` and/or
        ``partition``. No data is deleted: this resets a skip list, so no
        ``confirm`` is needed. Returns ``{"cleared": n}``.
        Needs Arc 2027.01.1+.
        """
        params = {k: v for k, v in {"tier": tier, "partition": partition}.items() if v}
        return self._json("delete", "/api/v1/compaction/too-wide", params=params or None)

    def trigger(
        self,
        tier: Optional[str] = None,
        database: Optional[str] = None,
        measurement: Optional[str] = None,
    ) -> Any:
        """Trigger a compaction cycle.

        Args:
            tier: Comma-separated tiers (e.g. "hourly,daily"). Default all enabled.
            database: Restrict to a single database.
            measurement: Restrict to one measurement of ``database``, which
                must also be given. Needs Arc 2026.09.2+.

        Raises:
            ArcValidationError: ``measurement`` was given without ``database``.
        """
        if measurement and not database:
            raise ArcValidationError("measurement requires database")
        params = {
            k: v
            for k, v in {"tier": tier, "database": database, "measurement": measurement}.items()
            if v
        }
        return self._json("post", "/api/v1/compaction/trigger", params=params or None)
