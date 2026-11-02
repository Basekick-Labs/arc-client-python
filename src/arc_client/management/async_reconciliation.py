"""Asynchronous reconciliation client for Arc (enterprise clustering).

Wraps ``/api/v1/reconciliation``. Reconciliation compares the cluster manifest
with what is in storage and, in act mode, deletes orphans on both sides.

Gates (Arc ``internal/api/reconciliation.go``):

* ``status`` — any token with read permission; not license-gated, so a
  cluster without the license still reports ``{"enabled": false, ...}``.
* ``trigger`` and ``get_run`` — admin token plus the ``clustering`` license
  feature (403 :class:`~arc_client.exceptions.ArcLicenseError` otherwise).
  503 when reconciliation is not running on the node.
"""

from __future__ import annotations

from typing import Any

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcValidationError
from arc_client.http.async_http import AsyncHTTPClient
from arc_client.http.base import never_retry_status
from arc_client.management.reconciliation import _run_from, _trigger_params
from arc_client.models.reconciliation import ReconciliationRun


class AsyncReconciliationClient:
    """Inspect and trigger manifest/storage reconciliation (enterprise)."""

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
            raise ArcError(f"Reconciliation request failed ({method.upper()} {path}): {e}") from e

    async def status(self) -> dict[str, Any]:
        """Get the reconciliation scheduler status.

        Returned as a raw dict (operator diagnostics): ``enabled``, ``running``,
        ``schedule``, ``backend_kind``, limits, ``in_flight``, and when
        present ``next_run``, ``last_run`` and ``recent_runs``. When
        reconciliation is unavailable on the node the server answers 200 with
        ``{"enabled": false, "reason": ...}``.
        """
        data = await self._json("get", "/api/v1/reconciliation/status")
        if not isinstance(data, dict):
            raise ArcError(f"Unexpected reconciliation status response: {data!r}")
        return data

    async def trigger(self, dry_run: bool = True, confirm: bool = False) -> ReconciliationRun:
        """Run one reconciliation cycle now and wait for its report (admin).

        Dry run (the default) only reports what would be deleted. Act mode
        (``dry_run=False``) deletes orphans, bounded by the server's
        ``max_deletes_per_run`` and grace window, and requires
        ``confirm=True``.

        Raises:
            ArcValidationError: ``dry_run=False`` without ``confirm=True``
                (raised before any request is sent).
            ArcServerError: 409 when a run is already in progress, 422 when
                the manifest is too large, 503 when reconciliation is
                disabled. For 422/500 the partial run is kept in
                ``error.response_body["run"]``.
            ArcPermissionError: 403 when the node is role-gated.
            ArcLicenseError: the ``clustering`` license feature is missing.
        """
        params = _trigger_params(dry_run, confirm)
        path = "/api/v1/reconciliation/trigger"
        return _run_from(await self._json("post", path, params=params), path)

    async def get_run(self, run_id: str) -> ReconciliationRun:
        """Get a recent run by id from the server's in-memory buffer (admin).

        Raises:
            ArcNotFoundError: The run is not (or no longer) in the buffer.
        """
        if not run_id:
            raise ArcValidationError("run_id is required")
        path = f"/api/v1/reconciliation/runs/{run_id}"
        # Arc answers a node without reconciliation with a permanent 503
        # and no Retry-After (reconciliation.go): raise at once.
        return _run_from(await self._json("get", path, retry_if=never_retry_status), path)
