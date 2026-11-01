"""Synchronous scheduler inspection and control for Arc.

Wraps ``/api/v1/schedulers``. Status is available to authenticated tokens;
control operations require an administrator token and the corresponding
enterprise scheduler feature. Arc reports a missing scheduler license as
``ArcLicenseError`` even when the route responds with HTTP 503.
"""

from __future__ import annotations

from typing import Any

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcValidationError
from arc_client.http.sync_http import SyncHTTPClient


class SchedulerClient:
    """Inspect scheduler status and perform explicit scheduler controls.

    Methods return raw JSON values because scheduler status and counters are
    server-defined operator diagnostics, not a stable typed response schema.
    """

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
            raise ArcError(f"Scheduler request failed ({method.upper()} {path}): {e}") from e

    def status(self) -> Any:
        """Return the status of the CQ and retention schedulers.

        Available with any authenticated token. Returns raw JSON with each
        scheduler's enabled state and, when unavailable, its reason.
        """
        return self._json("get", "/api/v1/schedulers/")

    def reload_continuous_queries(self) -> Any:
        """Reload active CQ schedules (admin; raises ArcLicenseError without license).

        Returns the raw JSON response from Arc.
        """
        return self._json("post", "/api/v1/schedulers/cq/reload")

    def trigger_retention(self, confirm: bool = False) -> Any:
        """Run all active retention policies immediately (administrator only).

        This can delete data according to the configured retention policies.
        Pass ``confirm=True`` to explicitly authorize the immediate run.
        Returns the raw JSON response from Arc.

        Raises:
            ArcValidationError: If ``confirm`` is not True.
            ArcLicenseError: If the scheduler feature is not licensed.
        """
        if not confirm:
            raise ArcValidationError(
                "Refusing to trigger retention without confirm=True. "
                "This may permanently delete data."
            )
        return self._json("post", "/api/v1/schedulers/retention/trigger")
