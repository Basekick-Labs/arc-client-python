"""Asynchronous storage-tiering client for Arc (enterprise).

Wraps ``/api/v1/tiering`` and ``/api/v1/tiering/policies``. Requires an admin
token and the ``tiering`` license feature.
"""

from __future__ import annotations

from typing import Any, Optional

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError
from arc_client.http.async_http import AsyncHTTPClient
from arc_client.validation import validate_name


class AsyncTieringClient:
    """Manage hot/cold storage tiering and per-database policies (enterprise)."""

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
            raise ArcError(f"Tiering request failed ({method.upper()} {path}): {e}") from e

    async def status(self) -> Any:
        """Get tiering status."""
        return await self._json("get", "/api/v1/tiering/status")

    async def files(
        self,
        tier: Optional[str] = None,
        database: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Any:
        """List tiered files, optionally filtered by tier/database."""
        params = {
            k: v
            for k, v in {"tier": tier, "database": database, "limit": limit}.items()
            if v is not None
        }
        return await self._json("get", "/api/v1/tiering/files", params=params or None)

    async def migrate(
        self,
        from_tier: Optional[str] = None,
        to_tier: Optional[str] = None,
        database: Optional[str] = None,
        measurement: Optional[str] = None,
        dry_run: bool = False,
    ) -> Any:
        """Trigger a tier-migration cycle."""
        body = {
            "from_tier": from_tier,
            "to_tier": to_tier,
            "database": database,
            "measurement": measurement,
            "dry_run": dry_run,
        }
        body = {k: v for k, v in body.items() if v is not None}
        return await self._json("post", "/api/v1/tiering/migrate", json=body or None)

    async def stats(self) -> Any:
        """Get tier statistics and recent migrations."""
        return await self._json("get", "/api/v1/tiering/stats")

    async def scan(self) -> Any:
        """Scan the hot tier and register any unregistered files."""
        return await self._json("post", "/api/v1/tiering/scan")

    # --- Per-database policies ---
    async def list_policies(self) -> Any:
        """List per-database tiering policies."""
        return await self._json("get", "/api/v1/tiering/policies/")

    async def get_policy(self, database: str) -> Any:
        """Get the custom tiering policy for a database (404 if none)."""
        validate_name(database, "database")
        return await self._json("get", f"/api/v1/tiering/policies/{database}")

    async def set_policy(
        self,
        database: str,
        hot_only: Optional[bool] = None,
        hot_max_age_days: Optional[int] = None,
    ) -> Any:
        """Create or update a database's tiering policy."""
        validate_name(database, "database")
        body: dict[str, Any] = {}
        if hot_only is not None:
            body["hot_only"] = hot_only
        if hot_max_age_days is not None:
            body["hot_max_age_days"] = hot_max_age_days
        return await self._json("put", f"/api/v1/tiering/policies/{database}", json=body)

    async def delete_policy(self, database: str) -> Any:
        """Delete a database's custom policy (revert to global defaults)."""
        validate_name(database, "database")
        return await self._json("delete", f"/api/v1/tiering/policies/{database}")

    async def effective_policy(self, database: str) -> Any:
        """Get the effective (resolved) tiering policy for a database."""
        validate_name(database, "database")
        return await self._json("get", f"/api/v1/tiering/policies/{database}/effective")
