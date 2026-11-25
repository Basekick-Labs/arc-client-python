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

from datetime import timedelta
from typing import Any, Optional, Union

from arc_client.config import ClientConfig
from arc_client.exceptions import (
    ArcCycleBootMismatchError,
    ArcError,
    ArcNotFoundError,
    ArcValidationError,
)
from arc_client.http.sync_http import SyncHTTPClient


def _check_positive(value: Optional[int], name: str) -> None:
    if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 1):
        raise ArcValidationError(f"{name} must be a positive integer")


# Arc caps a trigger's wait at 10 minutes (internal/api/compaction.go
# triggerWaitMax); the request's timeout leaves this margin beyond it.
_TRIGGER_WAIT_MAX = 600.0
_TRIGGER_WAIT_MARGIN = 30.0


def _wait_param(wait: Union[float, int, timedelta]) -> tuple[str, float]:
    """``wait`` as Arc's duration parameter, and the request timeout it needs."""
    seconds = wait.total_seconds() if isinstance(wait, timedelta) else wait
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not seconds > 0:
        raise ArcValidationError("wait must be a positive number of seconds or a timedelta")
    ms = max(1, round(seconds * 1000))
    return f"{ms}ms", min(seconds, _TRIGGER_WAIT_MAX) + _TRIGGER_WAIT_MARGIN


def _boot_mismatch(err: ArcNotFoundError, cycle_id: int, boot_id: Optional[str]) -> ArcError:
    """Arc's 404 for another process's cycle id, as its own error."""
    body = err.response_body or {}
    if boot_id and body.get("boot_id_mismatch"):
        mismatch = ArcCycleBootMismatchError(
            cycle_id, boot_id, body.get("boot_id"), body.get("lease_holder")
        )
        mismatch.response_body = body
        return mismatch
    return err


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

    def history(
        self,
        limit: Optional[int] = None,
        cycle_id: Optional[int] = None,
        boot_id: Optional[str] = None,
    ) -> Any:
        """Get recent compaction job attempts (default 10).

        Args:
            limit: Maximum records to return. Arc caps it at its history size.
            cycle_id: Only the attempts of this compaction cycle (the id
                :meth:`trigger` returns). Needs Arc 2027.01.1+; older servers
                ignore it and return every recent job.
            boot_id: The ``boot_id`` the trigger returned with ``cycle_id``
                (Arc 2027.01.1+). When it is not the answering process's, Arc
                returns no jobs and ``"boot_id_mismatch": true``: the jobs
                there with that number belong to another cycle.

        Raises:
            ArcValidationError: ``cycle_id`` is not a positive integer.
        """
        _check_positive(cycle_id, "cycle_id")
        params = {
            k: v
            for k, v in {"limit": limit, "cycle_id": cycle_id, "boot_id": boot_id or None}.items()
            if v is not None
        }
        return self._json("get", "/api/v1/compaction/history", params=params or None)

    def cycles(self, limit: Optional[int] = None) -> Any:
        """List the retained compaction cycles, newest first.

        History is held in memory on the node that ran the cycles (the
        compactor lease holder on a cluster) and is lost on restart.
        Needs Arc 2027.01.1+.
        """
        params = {"limit": limit} if limit is not None else None
        return self._json("get", "/api/v1/compaction/cycles", params=params)

    def cycle(self, cycle_id: int, boot_id: Optional[str] = None) -> Any:
        """Get one compaction cycle by id. Needs Arc 2027.01.1+.

        Args:
            cycle_id: The id :meth:`trigger` returned.
            boot_id: The ``boot_id`` :meth:`trigger` returned with it (Arc
                2027.01.1+). Cycle ids restart at 1 with each Arc process;
                with ``boot_id`` a lookup that reaches another process (a
                restart, another node) raises
                :class:`~arc_client.exceptions.ArcCycleBootMismatchError`
                instead of returning another cycle's record.

        Raises:
            ArcValidationError: ``cycle_id`` is not a positive integer.
            ArcCycleBootMismatchError: ``boot_id`` is not the answering
                process's.
            ArcNotFoundError: This node retains no record of the cycle. Arc's
                404 body names the retained window and, on a cluster, the
                compactor ``lease_holder`` where the cycle ran.
        """
        _check_positive(cycle_id, "cycle_id")
        params = {"boot_id": boot_id} if boot_id else None
        try:
            return self._json("get", f"/api/v1/compaction/cycles/{cycle_id}", params=params)
        except ArcNotFoundError as e:
            raise _boot_mismatch(e, cycle_id, boot_id) from e

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
        wait: Optional[Union[float, int, timedelta]] = None,
    ) -> Any:
        """Trigger a compaction cycle.

        The answer carries ``cycle_id`` and, from Arc 2027.01.1, ``boot_id``.
        Cycle ids restart at 1 with each Arc process, so keep both and pass
        ``boot_id`` to :meth:`cycle` and :meth:`history`.

        Args:
            tier: Comma-separated tiers (e.g. "hourly,daily"). Default all enabled.
            database: Restrict to a single database.
            measurement: Restrict to one measurement of ``database``, which
                must also be given. Needs Arc 2026.09.2+.
            wait: Seconds (or a ``timedelta``) to wait for the cycle to end,
                Arc 2027.01.1+; Arc caps it at 10 minutes. The answer is the
                cycle's record when it finished (read its ``status``: a failed
                cycle answers too), or the trigger's answer with
                ``"status": "running"`` when the wait ran out (``"unknown"``
                when Arc began shutting down). The request's timeout is raised
                to cover the wait. Keep it under a load balancer's idle
                timeout. Older servers ignore it and answer at once.

        Raises:
            ArcValidationError: ``measurement`` was given without ``database``,
                or ``wait`` is not a positive duration.
            ArcServerError: HTTP 503. With ``"stopping": true`` in
                ``response_body``, Arc is shutting down and takes no new cycle
                (Arc 2027.01.1+).
        """
        if measurement and not database:
            raise ArcValidationError("measurement requires database")
        params = {
            k: v
            for k, v in {"tier": tier, "database": database, "measurement": measurement}.items()
            if v
        }
        timeout = None
        if wait is not None:
            params["wait"], timeout = _wait_param(wait)
            timeout = max(timeout, self._config.timeout)
        return self._json(
            "post", "/api/v1/compaction/trigger", params=params or None, timeout=timeout
        )
