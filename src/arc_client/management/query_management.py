"""Synchronous query-management client for Arc (enterprise).

Wraps ``/api/v1/queries`` (active queries, history, inspect, kill). Requires an
admin token and the ``query_management`` license feature.
"""

from __future__ import annotations

from typing import Any, Optional

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError
from arc_client.http.sync_http import SyncHTTPClient


class QueryManagementClient:
    """Inspect and cancel running queries (enterprise)."""

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
            raise ArcError(f"Query-management request failed ({method.upper()} {path}): {e}") from e

    def active(self) -> Any:
        """List currently-executing queries."""
        return self._json("get", "/api/v1/queries/active")

    def history(self, limit: Optional[int] = None) -> Any:
        """List recently-completed queries (default 50, max 1000)."""
        params = {"limit": limit} if limit is not None else None
        return self._json("get", "/api/v1/queries/history", params=params)

    def get(self, query_id: str) -> Any:
        """Get details for a single query by id."""
        return self._json("get", f"/api/v1/queries/{query_id}")

    def kill(self, query_id: str) -> Any:
        """Cancel a running query by id."""
        return self._json("delete", f"/api/v1/queries/{query_id}")
