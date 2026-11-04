"""Asynchronous scheduler inspection and control for Arc.

Wraps ``/api/v1/schedulers``. Status is available to authenticated tokens;
control operations require an administrator token and the corresponding
enterprise scheduler feature. Since Arc 27.01.1 a missing, expired or
insufficient license is HTTP 403, raised as ``ArcLicenseError``; HTTP 503
means the scheduler is off in config or failed to start, and ``status()``
gives the reason. Earlier Arc versions answer 503 in both cases.
"""

from __future__ import annotations

from typing import Any

from arc_client.config import ClientConfig
from arc_client.exceptions import (
    ArcError,
    ArcLicenseError,
    ArcPermissionError,
    ArcValidationError,
)
from arc_client.http.async_http import AsyncHTTPClient
from arc_client.management.delete import DELETE_TIMEOUT_FLOOR


class AsyncSchedulerClient:
    """Inspect scheduler status and perform explicit scheduler controls.

    Methods return raw JSON values because scheduler status and counters are
    server-defined operator diagnostics, not a stable typed response schema.
    """

    def __init__(self, http: AsyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    async def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            response = await getattr(self._http, method)(path, **kwargs)
            return response.json()
        except ArcPermissionError as e:
            # Arc refuses a scheduler control with 403 when the license is
            # missing, expired or of a tier without the scheduler. Only the
            # "Enterprise license required" form matches the generic 403
            # license detection; the tier and refused-reload forms name the
            # license without that phrase.
            message = str(e)
            if "license" in message.lower():
                detail = message.split("Permission denied: ", 1)[-1]
                raise ArcLicenseError(f"Enterprise license required: {detail}") from e
            raise
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Scheduler request failed ({method.upper()} {path}): {e}") from e

    async def status(self) -> Any:
        """Return the status of the CQ and retention schedulers.

        Available with any authenticated token. Returns raw JSON with each
        scheduler's enabled state and, when unavailable, its reason.
        """
        return await self._json("get", "/api/v1/schedulers/")

    async def reload_continuous_queries(self) -> Any:
        """Reload active CQ schedules (administrator and licensed feature required).

        Returns the raw JSON response from Arc. A license that is missing,
        expired or does not include the scheduler raises ``ArcLicenseError``
        (HTTP 403). HTTP 503 means the scheduler is off in config or failed to
        start; call ``status()`` for the reason.
        """
        return await self._json("post", "/api/v1/schedulers/cq/reload")

    async def trigger_retention(self, confirm: bool = False) -> Any:
        """Run all active retention policies immediately (administrator only).

        This can delete data according to the configured retention policies.
        Pass ``confirm=True`` to explicitly authorize the immediate run. The
        request allows at least 90 seconds because Arc runs every active policy
        synchronously. In a cluster, send it to the primary writer: another node
        refuses it, as Arc does while a retention cycle is already running.
        Since Arc 27.01.1 both refusals are HTTP 409, raised as
        ``ArcServerError`` with ``status_code == 409``; earlier versions
        answer HTTP 500.

        Returns the raw JSON response from Arc. A license that is missing,
        expired or does not include the scheduler raises ``ArcLicenseError``
        (HTTP 403). HTTP 503 means the scheduler is off in config or failed to
        start; call ``status()`` for the reason.

        Raises:
            ArcValidationError: If ``confirm`` is not True.
        """
        if not confirm:
            raise ArcValidationError(
                "Refusing to trigger retention without confirm=True. "
                "This may permanently delete data."
            )
        return await self._json(
            "post",
            "/api/v1/schedulers/retention/trigger",
            timeout=max(self._config.timeout, DELETE_TIMEOUT_FLOOR),
        )
