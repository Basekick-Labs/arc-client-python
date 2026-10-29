"""Synchronous audit-log client for Arc (enterprise).

Wraps ``/api/v1/audit``. Requires an admin token and the ``audit_logging``
license feature.
"""

from __future__ import annotations

from typing import Any, Optional

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError
from arc_client.http.sync_http import SyncHTTPClient


class AuditClient:
    """Query the audit log (enterprise)."""

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
            raise ArcError(f"Audit request failed ({method.upper()} {path}): {e}") from e

    def logs(
        self,
        event_type: Optional[str] = None,
        actor: Optional[str] = None,
        database: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Any:
        """Query audit log entries.

        Args:
            event_type, actor, database: Optional exact-match filters.
            since, until: RFC3339 time bounds.
            limit, offset: Pagination.
        """
        params = {
            "event_type": event_type,
            "actor": actor,
            "database": database,
            "since": since,
            "until": until,
            "limit": limit,
            "offset": offset,
        }
        params = {k: v for k, v in params.items() if v is not None}
        return self._json("get", "/api/v1/audit/logs", params=params or None)

    def stats(self, since: Optional[str] = None) -> Any:
        """Get audit statistics (optionally since an RFC3339 timestamp)."""
        params = {"since": since} if since else None
        return self._json("get", "/api/v1/audit/stats", params=params)
