"""Asynchronous operational-diagnostics client for Arc.

Wraps Arc's readiness probes, JSON metrics, Prometheus metrics, buffered
application logs, and the admin memory diagnostics under ``/api/v1/debug``.

Everything except :meth:`DiagnosticsClient.readiness` returns raw dicts: these
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
from arc_client.exceptions import ArcError, ArcServerError
from arc_client.http.async_http import AsyncHTTPClient
from arc_client.management.diagnostics import (
    _free_os_memory_guard,
    _logs_params,
    _readiness_from_error,
    _readiness_path,
    _timeseries_request,
)
from arc_client.models.diagnostics import ReadinessStatus


class AsyncDiagnosticsClient:
    """Readiness, metrics, logs and memory diagnostics."""

    def __init__(self, http: AsyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    async def _json(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        try:
            response = await getattr(self._http, method)(path, **kwargs)
            data = response.json()
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Diagnostics request failed ({method.upper()} {path}): {e}") from e
        if not isinstance(data, dict):
            raise ArcError(f"Unexpected response from {path}: {data!r}")
        return data

    # Readiness -----------------------------------------------------------

    async def readiness(self, write: bool = False) -> ReadinessStatus:
        """Ask whether this node should receive traffic (public).

        ``write=False`` probes ``/ready``: is the node ready at all.
        ``write=True`` probes ``/ready/write``: should a load balancer send
        *writes* here. It is strictly narrower — a reader, or a non-primary
        node in local-storage mode, is ready but answers
        ``status="not_write_target"``.

        Unlike :meth:`AsyncArcClient.ready`, the reason is kept: a 503 from the
        probe is returned as a :class:`ReadinessStatus` with ``ready ==
        False``, not raised. Note that the SDK retries a GET that returns 503
        up to ``max_retries`` times before answering; construct the client
        with ``max_retries=0`` for a probe that must answer immediately.
        """
        try:
            data = await self._json("get", _readiness_path(write))
        except ArcServerError as e:
            return _readiness_from_error(e)
        return ReadinessStatus.model_validate(data)

    # Metrics -------------------------------------------------------------

    async def metrics(self) -> dict[str, Any]:
        """Get every metric counter as JSON, plus ``timestamp`` (public)."""
        return await self._json("get", "/api/v1/metrics")

    async def memory_metrics(self) -> dict[str, Any]:
        """Get Go runtime memory and GC metrics: ``memory`` and ``runtime`` (public)."""
        return await self._json("get", "/api/v1/metrics/memory")

    async def query_pool_metrics(self) -> dict[str, Any]:
        """Get DuckDB connection-pool and query counters under ``pool`` (public)."""
        return await self._json("get", "/api/v1/metrics/query-pool")

    async def endpoint_metrics(self) -> dict[str, Any]:
        """Get HTTP and query request statistics with average latencies (public)."""
        return await self._json("get", "/api/v1/metrics/endpoints")

    async def timeseries(
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
        return await self._json("get", path, params=params)

    async def prometheus(self) -> str:
        """Get ``/metrics`` in Prometheus text exposition format (public)."""
        try:
            response = await self._http.get("/metrics")
            return str(response.text)
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Diagnostics request failed (GET /metrics): {e}") from e

    # Logs (admin) --------------------------------------------------------

    async def logs(
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
        return await self._json(
            "get", "/api/v1/logs", params=_logs_params(limit, level, since_minutes)
        )

    # Memory debugging (admin) -------------------------------------------

    async def memstats(self) -> dict[str, Any]:
        """Attribute process memory to Go heap, glibc and DuckDB (admin).

        Returns ``go``, and when the platform reports them ``process``,
        ``glibc_heap`` and ``duckdb_summary``. Intended for support cases
        about unexplained memory growth.
        """
        return await self._json("get", "/api/v1/debug/memstats")

    async def duckdb_memory(self) -> dict[str, Any]:
        """Get DuckDB memory use per tag: ``total_bytes``, ``by_tag`` (admin)."""
        return await self._json("get", "/api/v1/debug/duckdb-memory")

    async def free_os_memory(self, confirm: bool = False) -> dict[str, Any]:
        """Force a GC and return freed heap to the OS (admin).

        Stop-the-world: it pauses ingestion and queries while it runs, so it
        requires ``confirm=True``. Arc allows one call per 30 seconds; a
        throttled call raises :class:`~arc_client.exceptions.ArcRateLimitError`
        whose ``response_body`` may carry ``retry_after_seconds``.

        Returns ``duration_ms`` and ``heap_released_before`` /
        ``heap_released_after`` / ``heap_released_delta``.
        """
        _free_os_memory_guard(confirm)
        return await self._json("post", "/api/v1/debug/free-os-memory")
