"""Synchronous reconciliation client for Arc (enterprise clustering).

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
from arc_client.http.base import never_retry_status
from arc_client.http.sync_http import SyncHTTPClient
from arc_client.models.reconciliation import ReconciliationRun


def _trigger_params(dry_run: bool, confirm: bool) -> dict[str, str]:
    if dry_run:
        return {"dry_run": "true"}
    if not confirm:
        raise ArcValidationError(
            "Refusing to run reconciliation in act mode without confirm=True. "
            "Act mode deletes orphaned files and manifest entries cluster-wide."
        )
    # Arc requires both flags for act mode.
    return {"dry_run": "false", "act": "true"}


def _run_from(data: Any, path: str) -> ReconciliationRun:
    if isinstance(data, dict) and isinstance(data.get("run"), dict):
        data = data["run"]
    if not isinstance(data, dict):
        raise ArcError(f"Unexpected reconciliation response from {path}: {data!r}")
    return ReconciliationRun.model_validate(data)


class ReconciliationClient:
    """Inspect and trigger manifest/storage reconciliation (enterprise)."""

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
            raise ArcError(f"Reconciliation request failed ({method.upper()} {path}): {e}") from e

    def status(self) -> dict[str, Any]:
        """Get the reconciliation scheduler status.

        Returned as a raw dict (operator diagnostics): ``enabled``, ``running``,
        ``schedule``, ``backend_kind``, limits, ``in_flight``, and when
        present ``next_run``, ``last_run`` and ``recent_runs``. When
        reconciliation is unavailable on the node the server answers 200 with
        ``{"enabled": false, "reason": ...}``.
        """
        data = self._json("get", "/api/v1/reconciliation/status")
        if not isinstance(data, dict):
            raise ArcError(f"Unexpected reconciliation status response: {data!r}")
        return data

    def trigger(self, dry_run: bool = True, confirm: bool = False) -> ReconciliationRun:
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
        return _run_from(self._json("post", path, params=params), path)

    def get_run(self, run_id: str) -> ReconciliationRun:
        """Get a recent run by id from the server's in-memory buffer (admin).

        Raises:
            ArcNotFoundError: The run is not (or no longer) in the buffer.
        """
        if not run_id:
            raise ArcValidationError("run_id is required")
        path = f"/api/v1/reconciliation/runs/{run_id}"
        # Arc answers a node without reconciliation with a permanent 503
        # and no Retry-After (reconciliation.go): raise at once.
        return _run_from(self._json("get", path, retry_if=never_retry_status), path)
