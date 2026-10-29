"""Asynchronous cluster-management client for Arc (enterprise).

Wraps ``/api/v1/cluster``. Reads are available to any valid token; ``files`` and
node removal require an admin token. When clustering is disabled the server
returns 200 with ``{"enabled": false, ...}`` rather than an error.
"""

from __future__ import annotations

from typing import Any, Optional

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError
from arc_client.http.async_http import AsyncHTTPClient


class AsyncClusterClient:
    """Inspect and manage the Arc cluster (enterprise)."""

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
            raise ArcError(f"Cluster request failed ({method.upper()} {path}): {e}") from e

    async def status(self) -> Any:
        """Get overall cluster status (includes enabled/mode/license)."""
        return await self._json("get", "/api/v1/cluster")

    async def nodes(self, role: Optional[str] = None, state: Optional[str] = None) -> Any:
        """List cluster nodes, optionally filtered by role and/or state."""
        params = {k: v for k, v in {"role": role, "state": state}.items() if v}
        return await self._json("get", "/api/v1/cluster/nodes", params=params or None)

    async def node(self, node_id: str) -> Any:
        """Get a single node by id."""
        return await self._json("get", f"/api/v1/cluster/nodes/{node_id}")

    async def local(self) -> Any:
        """Get the local node's info and capabilities."""
        return await self._json("get", "/api/v1/cluster/local")

    async def health(self) -> Any:
        """Get cluster health counts."""
        return await self._json("get", "/api/v1/cluster/health")

    async def files(
        self,
        database: Optional[str] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Any:
        """List cluster files (admin). Supports cursor pagination."""
        params = {
            k: v
            for k, v in {"database": database, "cursor": cursor, "limit": limit}.items()
            if v is not None
        }
        return await self._json("get", "/api/v1/cluster/files", params=params or None)

    async def remove_node(self, node_id: str) -> Any:
        """Remove a node from the cluster via Raft (admin)."""
        return await self._json("delete", f"/api/v1/cluster/nodes/{node_id}")
