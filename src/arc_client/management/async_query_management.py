"""Asynchronous query-management client for Arc (enterprise).

Wraps ``/api/v1/queries`` (active queries, history, inspect, kill). Requires an
admin token and the ``query_management`` license feature.
"""

from __future__ import annotations

from typing import Any, Optional

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError
from arc_client.http.async_http import AsyncHTTPClient


class AsyncQueryManagementClient:
    """Inspect and cancel running queries (enterprise)."""

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
            raise ArcError(f"Query-management request failed ({method.upper()} {path}): {e}") from e

    async def active(self) -> Any:
        """List currently-executing queries."""
        return await self._json("get", "/api/v1/queries/active")

    async def history(self, limit: Optional[int] = None) -> Any:
        """List recently-completed queries (default 50, max 1000)."""
        params = {"limit": limit} if limit is not None else None
        return await self._json("get", "/api/v1/queries/history", params=params)

    async def get(self, query_id: str) -> Any:
        """Get details for a single query by id."""
        return await self._json("get", f"/api/v1/queries/{query_id}")

    async def kill(self, query_id: str) -> Any:
        """Cancel a running query by id."""
        return await self._json("delete", f"/api/v1/queries/{query_id}")
