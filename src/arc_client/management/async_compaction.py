"""Asynchronous compaction-management client for Arc.

Wraps ``/api/v1/compaction``. Reads are available to any valid token; ``trigger``
requires an admin token.
"""

from __future__ import annotations

from typing import Any, Optional

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError
from arc_client.http.async_http import AsyncHTTPClient


class AsyncCompactionClient:
    """Inspect and trigger compaction."""

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
            raise ArcError(f"Compaction request failed ({method.upper()} {path}): {e}") from e

    async def status(self) -> Any:
        """Get compaction manager and scheduler status."""
        return await self._json("get", "/api/v1/compaction/status")

    async def stats(self) -> Any:
        """Get raw compaction statistics."""
        return await self._json("get", "/api/v1/compaction/stats")

    async def candidates(self) -> Any:
        """List partitions that are candidates for compaction."""
        return await self._json("get", "/api/v1/compaction/candidates")

    async def jobs(self) -> Any:
        """List active compaction jobs."""
        return await self._json("get", "/api/v1/compaction/jobs")

    async def history(self, limit: Optional[int] = None) -> Any:
        """Get recent compaction jobs (default 10)."""
        params = {"limit": limit} if limit is not None else None
        return await self._json("get", "/api/v1/compaction/history", params=params)

    async def trigger(self, tier: Optional[str] = None, database: Optional[str] = None) -> Any:
        """Trigger a compaction cycle (admin).

        Args:
            tier: Comma-separated tiers (e.g. "hourly,daily"). Default all enabled.
            database: Restrict to a single database.
        """
        params = {k: v for k, v in {"tier": tier, "database": database}.items() if v}
        return await self._json("post", "/api/v1/compaction/trigger", params=params or None)
