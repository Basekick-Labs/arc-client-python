"""Synchronous RBAC management client for Arc (enterprise).

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
from arc_client.http.sync_http import SyncHTTPClient


class RBACClient:
    """Manage RBAC organizations, teams, roles, and permissions (enterprise)."""

    def __init__(self, http: SyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            fn = getattr(self._http, method)
            response = fn(path, **kwargs)
            return response.json()
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"RBAC request failed ({method.upper()} {path}): {e}") from e

    # --- Organizations ---
    def list_organizations(self) -> Any:
        """List organizations."""
        return self._json("get", "/api/v1/rbac/organizations")

    def create_organization(self, name: str, description: Optional[str] = None) -> Any:
        """Create an organization."""
        body: dict[str, Any] = {"name": name}
        if description is not None:
            body["description"] = description
        return self._json("post", "/api/v1/rbac/organizations", json=body)

    def get_organization(self, org_id: int) -> Any:
        """Get an organization (includes its teams)."""
        return self._json("get", f"/api/v1/rbac/organizations/{org_id}")

    def update_organization(self, org_id: int, **fields: Any) -> Any:
        """Update an organization (name/description/enabled)."""
        return self._json("patch", f"/api/v1/rbac/organizations/{org_id}", json=fields)

    def delete_organization(self, org_id: int) -> Any:
        """Delete an organization."""
        return self._json("delete", f"/api/v1/rbac/organizations/{org_id}")

    # --- Teams ---
    def list_teams(self, org_id: int) -> Any:
        """List teams in an organization."""
        return self._json("get", f"/api/v1/rbac/organizations/{org_id}/teams")

    def create_team(self, org_id: int, name: str, description: Optional[str] = None) -> Any:
        """Create a team in an organization."""
        body: dict[str, Any] = {"name": name}
        if description is not None:
            body["description"] = description
        return self._json("post", f"/api/v1/rbac/organizations/{org_id}/teams", json=body)

    def get_team(self, team_id: int) -> Any:
        """Get a team (includes its roles)."""
        return self._json("get", f"/api/v1/rbac/teams/{team_id}")

    def update_team(self, team_id: int, **fields: Any) -> Any:
        """Update a team (name/description/enabled)."""
        return self._json("patch", f"/api/v1/rbac/teams/{team_id}", json=fields)

    def delete_team(self, team_id: int) -> Any:
        """Delete a team."""
        return self._json("delete", f"/api/v1/rbac/teams/{team_id}")

    # --- Roles ---
    def list_roles(self, team_id: int) -> Any:
        """List roles in a team."""
        return self._json("get", f"/api/v1/rbac/teams/{team_id}/roles")

    def create_role(self, team_id: int, database_pattern: str, permissions: list[str]) -> Any:
        """Create a role in a team."""
        body = {"database_pattern": database_pattern, "permissions": permissions}
        return self._json("post", f"/api/v1/rbac/teams/{team_id}/roles", json=body)

    def get_role(self, role_id: int) -> Any:
        """Get a role (includes measurement permissions)."""
        return self._json("get", f"/api/v1/rbac/roles/{role_id}")

    def update_role(self, role_id: int, **fields: Any) -> Any:
        """Update a role (database_pattern/permissions)."""
        return self._json("patch", f"/api/v1/rbac/roles/{role_id}", json=fields)

    def delete_role(self, role_id: int) -> Any:
        """Delete a role."""
        return self._json("delete", f"/api/v1/rbac/roles/{role_id}")

    # --- Measurement permissions ---
    def list_measurement_permissions(self, role_id: int) -> Any:
        """List measurement permissions for a role."""
        return self._json("get", f"/api/v1/rbac/roles/{role_id}/measurements")

    def create_measurement_permission(
        self, role_id: int, measurement_pattern: str, permissions: list[str]
    ) -> Any:
        """Create a measurement permission for a role."""
        body = {"measurement_pattern": measurement_pattern, "permissions": permissions}
        return self._json("post", f"/api/v1/rbac/roles/{role_id}/measurements", json=body)

    def delete_measurement_permission(self, permission_id: int) -> Any:
        """Delete a measurement permission."""
        return self._json("delete", f"/api/v1/rbac/measurement-permissions/{permission_id}")

    # --- Token / team membership (under /api/v1/auth) ---
    def list_token_teams(self, token_id: int) -> Any:
        """List the teams a token belongs to."""
        return self._json("get", f"/api/v1/auth/tokens/{token_id}/teams")

    def add_token_to_team(self, token_id: int, team_id: int) -> Any:
        """Add a token to a team."""
        return self._json(
            "post", f"/api/v1/auth/tokens/{token_id}/teams", json={"team_id": team_id}
        )

    def remove_token_from_team(self, token_id: int, team_id: int) -> Any:
        """Remove a token from a team."""
        return self._json("delete", f"/api/v1/auth/tokens/{token_id}/teams/{team_id}")

    def get_token_permissions(self, token_id: int) -> Any:
        """Get the effective permissions for a token."""
        return self._json("get", f"/api/v1/auth/tokens/{token_id}/permissions")
