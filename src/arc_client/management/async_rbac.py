"""Asynchronous RBAC management client for Arc (enterprise).

Wraps ``/api/v1/rbac`` (organizations -> teams -> roles -> measurement
permissions) plus the token/team membership routes under ``/api/v1/auth``.

All routes require an admin token and the ``rbac`` enterprise license feature;
a missing feature raises :class:`arc_client.exceptions.ArcLicenseError`.

Methods return the server's parsed JSON (dicts/lists) so newer server fields are
never dropped.
"""

from __future__ import annotations

from typing import Any, Optional

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError
from arc_client.http.async_http import AsyncHTTPClient


class AsyncRBACClient:
    """Manage RBAC organizations, teams, roles, and permissions (enterprise)."""

    def __init__(self, http: AsyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    async def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            fn = getattr(self._http, method)
            response = await fn(path, **kwargs)
            return response.json()
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"RBAC request failed ({method.upper()} {path}): {e}") from e

    # --- Organizations ---
    async def list_organizations(self) -> Any:
        """List organizations."""
        return await self._json("get", "/api/v1/rbac/organizations")

    async def create_organization(self, name: str, description: Optional[str] = None) -> Any:
        """Create an organization."""
        body: dict[str, Any] = {"name": name}
        if description is not None:
            body["description"] = description
        return await self._json("post", "/api/v1/rbac/organizations", json=body)

    async def get_organization(self, org_id: int) -> Any:
        """Get an organization (includes its teams)."""
        return await self._json("get", f"/api/v1/rbac/organizations/{org_id}")

    async def update_organization(self, org_id: int, **fields: Any) -> Any:
        """Update an organization (name/description/enabled)."""
        return await self._json("patch", f"/api/v1/rbac/organizations/{org_id}", json=fields)

    async def delete_organization(self, org_id: int) -> Any:
        """Delete an organization."""
        return await self._json("delete", f"/api/v1/rbac/organizations/{org_id}")

    # --- Teams ---
    async def list_teams(self, org_id: int) -> Any:
        """List teams in an organization."""
        return await self._json("get", f"/api/v1/rbac/organizations/{org_id}/teams")

    async def create_team(self, org_id: int, name: str, description: Optional[str] = None) -> Any:
        """Create a team in an organization."""
        body: dict[str, Any] = {"name": name}
        if description is not None:
            body["description"] = description
        return await self._json("post", f"/api/v1/rbac/organizations/{org_id}/teams", json=body)

    async def get_team(self, team_id: int) -> Any:
        """Get a team (includes its roles)."""
        return await self._json("get", f"/api/v1/rbac/teams/{team_id}")

    async def update_team(self, team_id: int, **fields: Any) -> Any:
        """Update a team (name/description/enabled)."""
        return await self._json("patch", f"/api/v1/rbac/teams/{team_id}", json=fields)

    async def delete_team(self, team_id: int) -> Any:
        """Delete a team."""
        return await self._json("delete", f"/api/v1/rbac/teams/{team_id}")

    # --- Roles ---
    async def list_roles(self, team_id: int) -> Any:
        """List roles in a team."""
        return await self._json("get", f"/api/v1/rbac/teams/{team_id}/roles")

    async def create_role(self, team_id: int, database_pattern: str, permissions: list[str]) -> Any:
        """Create a role in a team."""
        body = {"database_pattern": database_pattern, "permissions": permissions}
        return await self._json("post", f"/api/v1/rbac/teams/{team_id}/roles", json=body)

    async def get_role(self, role_id: int) -> Any:
        """Get a role (includes measurement permissions)."""
        return await self._json("get", f"/api/v1/rbac/roles/{role_id}")

    async def update_role(self, role_id: int, **fields: Any) -> Any:
        """Update a role (database_pattern/permissions)."""
        return await self._json("patch", f"/api/v1/rbac/roles/{role_id}", json=fields)

    async def delete_role(self, role_id: int) -> Any:
        """Delete a role."""
        return await self._json("delete", f"/api/v1/rbac/roles/{role_id}")

    # --- Measurement permissions ---
    async def list_measurement_permissions(self, role_id: int) -> Any:
        """List measurement permissions for a role."""
        return await self._json("get", f"/api/v1/rbac/roles/{role_id}/measurements")

    async def create_measurement_permission(
        self, role_id: int, measurement_pattern: str, permissions: list[str]
    ) -> Any:
        """Create a measurement permission for a role."""
        body = {"measurement_pattern": measurement_pattern, "permissions": permissions}
        return await self._json("post", f"/api/v1/rbac/roles/{role_id}/measurements", json=body)

    async def delete_measurement_permission(self, permission_id: int) -> Any:
        """Delete a measurement permission."""
        return await self._json("delete", f"/api/v1/rbac/measurement-permissions/{permission_id}")

    # --- Token / team membership (under /api/v1/auth) ---
    async def list_token_teams(self, token_id: int) -> Any:
        """List the teams a token belongs to."""
        return await self._json("get", f"/api/v1/auth/tokens/{token_id}/teams")

    async def add_token_to_team(self, token_id: int, team_id: int) -> Any:
        """Add a token to a team."""
        return await self._json(
            "post", f"/api/v1/auth/tokens/{token_id}/teams", json={"team_id": team_id}
        )

    async def remove_token_from_team(self, token_id: int, team_id: int) -> Any:
        """Remove a token from a team."""
        return await self._json("delete", f"/api/v1/auth/tokens/{token_id}/teams/{team_id}")

    async def get_token_permissions(self, token_id: int) -> Any:
        """Get the effective permissions for a token."""
        return await self._json("get", f"/api/v1/auth/tokens/{token_id}/permissions")
