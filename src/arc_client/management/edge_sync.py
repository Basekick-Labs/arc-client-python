"""Synchronous edge-sync administration client for Arc.

Edge sync moves data from edge nodes (*spokes*) to a central node (the
*hub*), over the network or on removable media ("air-gap bundles"). Methods
here are grouped by which node they must be sent to; a route that is not
enabled on the node answers 404.

Hub (``internal/api/edgesync_admin.go``, ``edgesync_import.go``):

* ``/api/v1/sync-spokes`` — spoke registrations. Mounted when
  ``edge_sync.enabled`` or ``edge_sync.import.enabled``.
* ``/api/v1/bundle-import`` — air-gap import. Mounted when
  ``edge_sync.import.enabled``.

Spoke (``internal/api/edgesync_spoke.go``):

* ``/api/v1/spoke-sync`` — manual sync, ledger and bundle export. Mounted
  when ``edge_sync.spoke.enabled`` or ``edge_sync.spoke.bundle.enabled``.
  Routes needing the transport that is not enabled answer 503.

Every route requires an admin token. None is license-gated (only Arc's
automatic spoke scheduler is). The spoke-to-hub transport routes
(``/api/v1/sync/file``, ``/api/v1/sync/reconcile``) are HMAC-authenticated
per spoke and called by Arc itself; they are deliberately not wrapped.

Spoke registrations are returned as :class:`~arc_client.models.edge_sync.Spoke`
models; the other responses are raw dicts (operator diagnostics).
"""

from __future__ import annotations

from typing import Any, List, Optional
from urllib.parse import quote

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcValidationError
from arc_client.http.base import never_retry_status
from arc_client.http.sync_http import SyncHTTPClient
from arc_client.models.edge_sync import Spoke, SpokeSecret

SPOKES = "/api/v1/sync-spokes"
BUNDLE_IMPORT = "/api/v1/bundle-import"
SPOKE_SYNC = "/api/v1/spoke-sync"
LEDGER_STATES = ("pending", "in_flight", "synced", "exported", "failed", "skipped")


def _segment(name: str, value: str) -> str:
    if not value:
        raise ArcValidationError(f"{name} is required")
    return quote(value, safe="")


def _limit(value: Optional[int], high: Optional[int] = 1000) -> Optional[dict[str, Any]]:
    if value is None:
        return None
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 1
        or (high is not None and value > high)
    ):
        bound = f"from 1 to {high}" if high is not None else "of at least 1"
        raise ArcValidationError(f"limit must be an integer {bound}")
    return {"limit": value}


def _require_confirm(confirm: bool, message: str) -> None:
    if not confirm:
        raise ArcValidationError(message)


def _ledger_params(limit: Optional[int], state: Optional[str]) -> Optional[dict[str, Any]]:
    params = dict(_limit(limit) or {})
    if state is not None:
        if state not in LEDGER_STATES:
            raise ArcValidationError(f"state must be one of {', '.join(LEDGER_STATES)}")
        params["state"] = state
    return params or None


def _selector(path: Optional[str], all_failed: bool) -> dict[str, Any]:
    if bool(path) == bool(all_failed):
        raise ArcValidationError("Provide exactly one of path or all_failed=True")
    return {"path": path} if path else {"all": True}


def _path_body(path: str, what: str) -> dict[str, Any]:
    if not path:
        raise ArcValidationError(f"path is required: {what}")
    return {"path": path}


def _spokes(data: Any) -> List[Spoke]:
    spokes = data.get("spokes") if isinstance(data, dict) else None
    if not isinstance(spokes, list):
        raise ArcError(f"Unexpected spoke list response: {data!r}")
    return [Spoke.model_validate(s) for s in spokes]


ROTATE_REFUSAL = (
    "Refusing to rotate without confirm=True. The current secret stops "
    "authenticating immediately; the spoke cannot sync until reconfigured."
)
DELETE_REFUSAL = (
    "Refusing to delete the spoke registration without confirm=True. Its "
    "counters and secret are removed and the spoke can no longer sync."
)
DISMISS_REFUSAL = (
    "Refusing to dismiss without confirm=True. Dismissed entries are no longer "
    "delivered and are pruned after the ledger retention period."
)


class EdgeSyncClient:
    """Administer edge sync on a hub or a spoke (admin token)."""

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
            raise ArcError(f"Edge sync request failed ({method.upper()} {path}): {e}") from e

    def _dict(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        data = self._json(method, path, **kwargs)
        if not isinstance(data, dict):
            raise ArcError(f"Unexpected response from {path}: {data!r}")
        return data

    # Hub: spoke registrations ------------------------------------------

    def list_spokes(self) -> List[Spoke]:
        """List spokes registered with this hub (hub)."""
        return _spokes(self._json("get", f"{SPOKES}/"))

    def get_spoke(self, spoke_id: str) -> Spoke:
        """Get one registered spoke (hub). 404 if not registered."""
        return Spoke.model_validate(self._dict("get", f"{SPOKES}/{_segment('spoke_id', spoke_id)}"))

    def register_spoke(self, spoke_id: str, name: Optional[str] = None) -> SpokeSecret:
        """Register a spoke and get its HMAC secret (hub).

        The secret is returned only this once; store it before discarding the
        result. 409 if the ID is already registered (Arc refuses to reissue).
        ``name`` defaults to ``spoke_id``: Arc requires a non-empty name
        (internal/edgesync/registry.go).
        """
        _segment("spoke_id", spoke_id)
        body = {"spoke_id": spoke_id, "name": name or spoke_id}
        return SpokeSecret.model_validate(self._dict("post", f"{SPOKES}/", json=body))

    def rotate_spoke_secret(self, spoke_id: str, confirm: bool = False) -> SpokeSecret:
        """Replace a spoke's secret (hub). Requires ``confirm=True``.

        The previous secret stops authenticating at once, so a live spoke is
        locked out until it is reconfigured with the new one.
        """
        segment = _segment("spoke_id", spoke_id)
        _require_confirm(confirm, ROTATE_REFUSAL)
        return SpokeSecret.model_validate(self._dict("post", f"{SPOKES}/{segment}/rotate"))

    def enable_spoke(self, spoke_id: str) -> dict[str, Any]:
        """Allow a disabled spoke to sync again (hub)."""
        return self._dict("post", f"{SPOKES}/{_segment('spoke_id', spoke_id)}/enable")

    def disable_spoke(self, spoke_id: str) -> dict[str, Any]:
        """Stop accepting syncs from a spoke (hub).

        Reversible with :meth:`enable_spoke`; counters, history and the secret
        survive, so no ``confirm`` is required.
        """
        return self._dict("post", f"{SPOKES}/{_segment('spoke_id', spoke_id)}/disable")

    def delete_spoke(self, spoke_id: str, confirm: bool = False) -> dict[str, Any]:
        """Remove a spoke's registration (hub). Requires ``confirm=True``.

        Files already received from the spoke are kept. Re-adding the spoke
        means registering it again and reprovisioning a secret.
        """
        segment = _segment("spoke_id", spoke_id)
        _require_confirm(confirm, DELETE_REFUSAL)
        return self._dict("delete", f"{SPOKES}/{segment}")

    # Hub: air-gap import ------------------------------------------------

    def import_bundle(self, path: str) -> dict[str, Any]:
        """Import an air-gap bundle from a directory on the hub (hub).

        ``path`` is resolved on the hub and must be inside
        ``edge_sync.import.allowed_dirs``. The bundle is verified in full
        before anything is written; paths holding different content are
        reported in ``conflicts`` and never overwritten.

        Synchronous and possibly long (Arc allows up to 4 hours): construct
        the client with a large ``timeout``. If the client gives up first the
        import keeps running, and a repeat call answers 409.

        Raises:
            ArcServerError: 409 when another import is running or the bundle
                was already imported (``response_body["reason"]`` says which),
                422 when the bundle is invalid.
        """
        return self._dict(
            "post", f"{BUNDLE_IMPORT}/", json=_path_body(path, "the bundle directory to import")
        )

    def import_history(self, spoke_id: str, limit: Optional[int] = None) -> dict[str, Any]:
        """List bundles this hub has imported from a spoke, newest first (hub).

        Args:
            limit: 1-1000; server default 100.
        """
        segment = _segment("spoke_id", spoke_id)
        return self._dict("get", f"{BUNDLE_IMPORT}/history/{segment}", params=_limit(limit))

    # Spoke: sync control and ledger ------------------------------------

    def spoke_status(self) -> dict[str, Any]:
        """Get ledger counts and sync lag on this spoke (spoke).

        ``pending``, ``in_flight``, ``exported``, ``synced``, ``failed``,
        ``skipped``, ``pending_bytes`` and, once anything has synced,
        ``last_synced_at`` / ``seconds_since_last_sync``. Files are entered
        in the ledger by a sync or export pass, so ``pending`` stays 0 for
        new data until one runs.
        """
        return self._dict("get", f"{SPOKE_SYNC}/status", retry_if=never_retry_status)

    def spoke_ledger(
        self, limit: Optional[int] = None, state: Optional[str] = None
    ) -> dict[str, Any]:
        """List ledger entries on this spoke (spoke).

        By default the unfinished entries (including failed ones). ``state``
        lists one state instead: ``pending``, ``in_flight``, ``synced``,
        ``exported``, ``failed`` or ``skipped``. ``limit`` is 1-1000 (server
        default 100).
        """
        return self._dict(
            "get",
            f"{SPOKE_SYNC}/ledger",
            params=_ledger_params(limit, state),
            retry_if=never_retry_status,
        )

    def run_sync(self) -> dict[str, Any]:
        """Run one network sync pass now and wait for its result (spoke).

        Requires ``edge_sync.spoke.enabled`` (503 otherwise); 409 if a pass
        is already running. Synchronous and possibly long (Arc allows up to
        2 hours): construct the client with a large ``timeout``.
        """
        return self._dict("post", f"{SPOKE_SYNC}/run")

    def requeue_failed(
        self, path: Optional[str] = None, all_failed: bool = False
    ) -> dict[str, Any]:
        """Return failed or dismissed entries to pending (spoke).

        Give exactly one of ``path`` or ``all_failed=True``. Returns
        ``{"requeued": n}``; 404 when a named path matched nothing.
        """
        return self._dict("post", f"{SPOKE_SYNC}/ledger/requeue", json=_selector(path, all_failed))

    def dismiss_failed(
        self, path: Optional[str] = None, all_failed: bool = False, confirm: bool = False
    ) -> dict[str, Any]:
        """Mark failed entries as operator-dismissed (spoke). Requires ``confirm=True``.

        Dismissed entries are no longer delivered and are pruned after the
        ledger retention period; until then :meth:`requeue_failed` reverses
        it. Give exactly one of ``path`` or ``all_failed=True``. Returns
        ``{"dismissed": n}``.
        """
        body = _selector(path, all_failed)
        _require_confirm(confirm, DISMISS_REFUSAL)
        return self._dict("post", f"{SPOKE_SYNC}/ledger/dismiss", json=body)

    # Spoke: air-gap export ---------------------------------------------

    def export_bundle(self, path: str, limit: Optional[int] = None) -> dict[str, Any]:
        """Write pending files to an air-gap bundle under ``path`` (spoke).

        ``path`` is on the spoke and must be inside the configured allow-list.
        ``limit`` caps the number of files (server maximum applies). Requires
        ``edge_sync.spoke.bundle.enabled`` (503 otherwise). Exported files
        become ``synced`` only after :meth:`apply_ack`. An empty backlog is
        ``{"exported": false, "reason": "nothing to export"}``, not an error.
        Possibly long (up to 2 hours): use a large client ``timeout``.
        """
        body = _path_body(path, "the directory to write the bundle into")
        if limit is not None:
            body.update(_limit(limit, high=None) or {})
        return self._dict("post", f"{SPOKE_SYNC}/export", json=body)

    def revert_export(self, bundle_id: str) -> dict[str, Any]:
        """Return a lost or undelivered bundle's files to pending (spoke).

        Scoped to one bundle; the files are carried again by a later bundle or
        sync pass (the hub skips copies it already holds). Returns
        ``{"bundle_id": ..., "reverted": n}``.
        """
        segment = _segment("bundle_id", bundle_id)
        return self._dict("post", f"{SPOKE_SYNC}/export/{segment}/revert")

    def apply_ack(self, path: str) -> dict[str, Any]:
        """Apply the hub's acknowledgment from a returned bundle (spoke).

        Advances the files the hub confirmed from ``exported`` to ``synced``.
        A bundle with no acknowledgment yet answers
        ``{"applied": false, ...}``, not an error.
        """
        return self._dict(
            "post", f"{SPOKE_SYNC}/ack", json=_path_body(path, "the returned bundle directory")
        )
