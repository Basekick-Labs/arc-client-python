"""Synchronous operational-diagnostics client for Arc.

Wraps Arc's JSON metrics, Prometheus metrics, buffered application logs, and
the admin memory diagnostics under ``/api/v1/debug``. Readiness probes live on
the client itself (``readiness()`` / ``write_readiness()``).

Everything returns raw dicts: these
are operator diagnostics whose fields follow Arc's internal counters and Go
runtime, not a stable contract.

Access (Arc ``internal/api/server.go``, ``cmd/arc/main.go``,
``internal/api/debug.go``):

* ``/ready``, ``/ready/write``, ``/metrics`` and ``/api/v1/metrics/*`` are
  public; no token is needed (they are meant for load balancers, scrapers and
  dashboards).
* ``/api/v1/logs`` and ``/api/v1/debug/*`` require an admin token.
"""

from __future__ import annotations

from typing import Any, Optional

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcValidationError
from arc_client.http.sync_http import SyncHTTPClient

TIMESERIES_TYPES = ("system", "application", "api")


def _bounded(name: str, value: Optional[int], low: int, high: int) -> Optional[int]:
    """Reject values Arc would silently replace with its default."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ArcValidationError(f"{name} must be an integer from {low} to {high}")
    return value


def _logs_params(
    limit: Optional[int], level: Optional[str], since_minutes: Optional[int]
) -> Optional[dict[str, Any]]:
    params: dict[str, Any] = {}
    if _bounded("limit", limit, 1, 1000) is not None:
        params["limit"] = limit
    if level:
        params["level"] = level
    if _bounded("since_minutes", since_minutes, 1, 1440) is not None:
        params["since_minutes"] = since_minutes
    return params or None


def _timeseries_request(
    metric_type: str, duration_minutes: Optional[int]
) -> tuple[str, Optional[dict[str, Any]]]:
    if metric_type not in TIMESERIES_TYPES:
        raise ArcValidationError(f"metric_type must be one of {', '.join(TIMESERIES_TYPES)}")
    duration = _bounded("duration_minutes", duration_minutes, 1, 1440)
    params = {"duration_minutes": duration} if duration is not None else None
    return f"/api/v1/metrics/timeseries/{metric_type}", params


def _free_os_memory_guard(confirm: bool) -> None:
    if not confirm:
        raise ArcValidationError(
            "Refusing to call free-os-memory without confirm=True. It forces a "
            "stop-the-world garbage collection that pauses ingestion and queries."
        )


class DiagnosticsClient:
    """Metrics, logs and memory diagnostics."""

    def __init__(self, http: SyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    def _json(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        try:
            response = getattr(self._http, method)(path, **kwargs)
            data = response.json()
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Diagnostics request failed ({method.upper()} {path}): {e}") from e
        if not isinstance(data, dict):
            raise ArcError(f"Unexpected response from {path}: {data!r}")
        return data

    # Metrics -------------------------------------------------------------

    def metrics(self) -> dict[str, Any]:
        """Get every metric counter as JSON, plus ``timestamp`` (public)."""
        return self._json("get", "/api/v1/metrics")

    def memory_metrics(self) -> dict[str, Any]:
        """Get Go runtime memory and GC metrics: ``memory`` and ``runtime`` (public)."""
        return self._json("get", "/api/v1/metrics/memory")

    def query_pool_metrics(self) -> dict[str, Any]:
        """Get DuckDB connection-pool and query counters under ``pool`` (public)."""
        return self._json("get", "/api/v1/metrics/query-pool")

    def endpoint_metrics(self) -> dict[str, Any]:
        """Get HTTP and query request statistics with average latencies (public)."""
        return self._json("get", "/api/v1/metrics/endpoints")

    def timeseries(
        self, metric_type: str, duration_minutes: Optional[int] = None
    ) -> dict[str, Any]:
        """Get recent sampled metric points (public).

        Args:
            metric_type: ``"system"``, ``"application"`` or ``"api"``.
            duration_minutes: Window, 1-1440. Server default 30.

        Raises:
            ArcValidationError: Unknown type or out-of-range duration (Arc
                would silently fall back to 30 minutes).
        """
        path, params = _timeseries_request(metric_type, duration_minutes)
        return self._json("get", path, params=params)

    def prometheus(self) -> str:
        """Get ``/metrics`` in Prometheus text exposition format (public)."""
        try:
            return str(self._http.get("/metrics").text)
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Diagnostics request failed (GET /metrics): {e}") from e

    # Logs (admin) --------------------------------------------------------

    def logs(
        self,
        limit: Optional[int] = None,
        level: Optional[str] = None,
        since_minutes: Optional[int] = None,
    ) -> dict[str, Any]:
        """Get recent application logs from the node's in-memory buffer (admin).

        Args:
            limit: Max entries, 1-1000. Server default 100.
            level: Minimum level, case-insensitive: ``debug``, ``info``,
                ``warn``, ``error`` or ``fatal`` (entries at or above it).
                Any other value matches that level name exactly.
            since_minutes: Look-back window, 1-1440. Server default 60.

        Returns a raw dict with ``logs`` (newest first; ``timestamp``,
        ``level``, ``component``, ``message``, ``caller``), ``count`` and the
        effective filters.

        Raises:
            ArcValidationError: ``limit``/``since_minutes`` out of range (Arc
                would silently use its default instead).
        """
        return self._json("get", "/api/v1/logs", params=_logs_params(limit, level, since_minutes))

    # Memory debugging (admin) -------------------------------------------

    def memstats(self) -> dict[str, Any]:
        """Attribute process memory to Go heap, glibc and DuckDB (admin).

        Returns ``go``, and when the platform reports them ``process``,
        ``glibc_heap`` and ``duckdb_summary``. Intended for support cases
        about unexplained memory growth.
        """
        return self._json("get", "/api/v1/debug/memstats")

    def duckdb_memory(self) -> dict[str, Any]:
        """Get DuckDB memory use per tag: ``total_bytes``, ``by_tag`` (admin)."""
        return self._json("get", "/api/v1/debug/duckdb-memory")

    def free_os_memory(self, confirm: bool = False) -> dict[str, Any]:
        """Force a GC and return freed heap to the OS (admin).

        Stop-the-world: it pauses ingestion and queries while it runs, so it
        requires ``confirm=True``. Arc allows one call per 30 seconds; a
        throttled call raises :class:`~arc_client.exceptions.ArcRateLimitError`
        whose ``response_body`` may carry ``retry_after_seconds``.

        Returns ``duration_ms`` and ``heap_released_before`` /
        ``heap_released_after`` / ``heap_released_delta``.
        """
        _free_os_memory_guard(confirm)
        return self._json("post", "/api/v1/debug/free-os-memory")
