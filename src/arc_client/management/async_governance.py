"""Asynchronous query-governance client for Arc (enterprise).

Wraps ``/api/v1/governance`` (per-token rate limits and quotas). Requires an
admin token and the ``query_governance`` license feature.
"""

from __future__ import annotations

from typing import Any

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError
from arc_client.http.async_http import AsyncHTTPClient


class AsyncGovernanceClient:
    """Manage per-token query governance policies (enterprise)."""

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
            raise ArcError(f"Governance request failed ({method.upper()} {path}): {e}") from e

    async def list_policies(self) -> Any:
        """List all governance policies."""
        return await self._json("get", "/api/v1/governance/policies")

    async def get_policy(self, token_id: int) -> Any:
        """Get the policy for a token."""
        return await self._json("get", f"/api/v1/governance/policies/{token_id}")

    async def create_policy(self, token_id: int, **limits: Any) -> Any:
        """Create a policy for a token.

        Limit fields (all optional, 0 = unlimited): rate_limit_per_minute,
        rate_limit_per_hour, max_queries_per_hour, max_queries_per_day,
        max_rows_per_query, max_scan_duration_sec.
        """
        body = {"token_id": token_id, **limits}
        return await self._json("post", "/api/v1/governance/policies", json=body)

    async def update_policy(self, token_id: int, **limits: Any) -> Any:
        """Update the policy for a token (same limit fields as create)."""
        body = {"token_id": token_id, **limits}
        return await self._json("put", f"/api/v1/governance/policies/{token_id}", json=body)

    async def delete_policy(self, token_id: int) -> Any:
        """Delete the policy for a token."""
        return await self._json("delete", f"/api/v1/governance/policies/{token_id}")

    async def usage(self, token_id: int) -> Any:
        """Get current usage counters and the effective policy for a token."""
        return await self._json("get", f"/api/v1/governance/usage/{token_id}")
