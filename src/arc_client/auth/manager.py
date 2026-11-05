"""Synchronous auth client for Arc."""

from __future__ import annotations

from typing import Any, List, Optional

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcAuthenticationError, ArcError, ArcNotFoundError
from arc_client.http.sync_http import SyncHTTPClient
from arc_client.models.auth import (
    CreateTokenResponse,
    RotateTokenResponse,
    TokenInfo,
    TokenListResponse,
    VerifyResponse,
)


class AuthClient:
    """Synchronous client for authentication and token management.

    This client provides methods for verifying tokens and managing API tokens
    (create, list, update, delete, rotate, revoke).

    Note: Token management operations require admin permissions.

    Example:
        >>> with ArcClient(host="localhost", token="admin-token") as client:
        ...     result = client.auth.verify()
        ...     if result.valid:
        ...         print(f"Token valid with permissions: {result.permissions}")
    """

    def __init__(self, http: SyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    def verify(self) -> VerifyResponse:
        """Verify the current token."""
        if not self._config.token:
            return VerifyResponse(valid=False, error="No token configured")

        try:
            response = self._http.get("/api/v1/auth/verify")
            data = response.json()
            return VerifyResponse(**data)
        except ArcAuthenticationError:
            return VerifyResponse(valid=False, error="Invalid or expired token")
        except ArcError:
            raise
        except Exception as e:
            raise ArcAuthenticationError(f"Token verification failed: {e}") from e

    def create_token(
        self,
        name: str,
        description: Optional[str] = None,
        permissions: Optional[list[str]] = None,
        expires_in: Optional[str] = None,
    ) -> CreateTokenResponse:
        """Create a new API token. Requires admin permissions."""
        payload: dict[str, Any] = {"name": name}
        if description is not None:
            payload["description"] = description
        # An empty list is meaningful: Arc creates an RBAC-only token with no
        # OSS permissions. Omitting the key makes Arc default to read,write
        # (internal/api/auth_routes.go CreateTokenRequest).
        if permissions is not None:
            payload["permissions"] = permissions
        if expires_in is not None:
            payload["expires_in"] = expires_in

        try:
            response = self._http.post("/api/v1/auth/tokens", json=payload)
            data = response.json()
            return CreateTokenResponse(**data)
        except ArcError:
            raise
        except Exception as e:
            raise ArcAuthenticationError(f"Failed to create token: {e}") from e

    def list_tokens(self) -> List[TokenInfo]:
        """List all tokens. Requires admin permissions."""
        try:
            response = self._http.get("/api/v1/auth/tokens")
            data = response.json()
            result = TokenListResponse(**data)
            if not result.success:
                raise ArcAuthenticationError(result.error or "Failed to list tokens")
            return result.tokens
        except ArcError:
            raise
        except Exception as e:
            raise ArcAuthenticationError(f"Failed to list tokens: {e}") from e

    def get_token(self, token_id: int) -> TokenInfo:
        """Get token details by ID. Requires admin permissions."""
        try:
            response = self._http.get(f"/api/v1/auth/tokens/{token_id}")
            data = response.json()
            if not data.get("success", True):
                error = data.get("error", "Unknown error")
                if "not found" in error.lower():
                    raise ArcNotFoundError(f"Token {token_id} not found")
                raise ArcAuthenticationError(error)
            return TokenInfo(**data["token"])
        except ArcError:
            raise
        except Exception as e:
            raise ArcAuthenticationError(f"Failed to get token: {e}") from e

    def update_token(
        self,
        token_id: int,
        name: Optional[str] = None,
        description: Optional[str] = None,
        permissions: Optional[list[str]] = None,
        expires_in: Optional[str] = None,
    ) -> None:
        """Update token properties. Requires admin permissions."""
        payload: dict[str, Any] = {}
        if name is not None:
            payload["name"] = name
        if description is not None:
            payload["description"] = description
        if permissions is not None:
            payload["permissions"] = permissions
        if expires_in is not None:
            payload["expires_in"] = expires_in

        try:
            response = self._http.patch(f"/api/v1/auth/tokens/{token_id}", json=payload)
            data = response.json()
            if not data.get("success", True):
                error = data.get("error", "Unknown error")
                if "not found" in error.lower():
                    raise ArcNotFoundError(f"Token {token_id} not found")
                raise ArcAuthenticationError(error)
        except ArcError:
            raise
        except Exception as e:
            raise ArcAuthenticationError(f"Failed to update token: {e}") from e

    def delete_token(self, token_id: int) -> None:
        """Delete a token. Requires admin permissions."""
        try:
            response = self._http.delete(f"/api/v1/auth/tokens/{token_id}")
            data = response.json()
            if not data.get("success", True):
                error = data.get("error", "Unknown error")
                if "not found" in error.lower():
                    raise ArcNotFoundError(f"Token {token_id} not found")
                raise ArcAuthenticationError(error)
        except ArcError:
            raise
        except Exception as e:
            raise ArcAuthenticationError(f"Failed to delete token: {e}") from e

    def rotate_token(self, token_id: int) -> str:
        """Rotate a token and return the new value. Requires admin permissions."""
        try:
            response = self._http.post(f"/api/v1/auth/tokens/{token_id}/rotate")
            data = response.json()
            result = RotateTokenResponse(**data)
            if not result.success:
                error = result.error or "Unknown error"
                if "not found" in error.lower():
                    raise ArcNotFoundError(f"Token {token_id} not found")
                raise ArcAuthenticationError(error)
            if not result.new_token:
                raise ArcAuthenticationError("No new token returned")
            return result.new_token
        except ArcError:
            raise
        except Exception as e:
            raise ArcAuthenticationError(f"Failed to rotate token: {e}") from e

    def revoke_token(self, token_id: int) -> None:
        """Revoke a token. Requires admin permissions."""
        try:
            response = self._http.post(f"/api/v1/auth/tokens/{token_id}/revoke")
            data = response.json()
            if not data.get("success", True):
                error = data.get("error", "Unknown error")
                if "not found" in error.lower():
                    raise ArcNotFoundError(f"Token {token_id} not found")
                raise ArcAuthenticationError(error)
        except ArcError:
            raise
        except Exception as e:
            raise ArcAuthenticationError(f"Failed to revoke token: {e}") from e

    def cache_stats(self) -> dict[str, Any]:
        """Get the server's token-validation cache statistics (admin).

        Returns the inner ``stats`` object as a raw dict (operator
        diagnostics): ``cache_size``, ``max_cache_size``,
        ``cache_ttl_seconds``, ``utilization_percent``, ``total_requests``,
        ``cache_hits``, ``cache_misses``, ``cache_evictions`` and
        ``hit_rate_percent``. The cache is per node.
        """
        data = self._admin_json("get", "/api/v1/auth/cache/stats")
        stats = data.get("stats")
        if not isinstance(stats, dict):
            raise ArcError(f"Unexpected auth cache stats response: {data!r}")
        return stats

    def invalidate_cache(self) -> dict[str, Any]:
        """Clear the server's token-validation cache on this node (admin).

        Loses no data and needs no ``confirm``: the next request for each
        token is validated against the token store again, which costs one
        lookup per token. Use it to make a token change take effect before
        the cache TTL expires. Returns ``{"success": true, "message": ...}``.
        """
        return self._admin_json("post", "/api/v1/auth/cache/invalidate")

    def _admin_json(self, method: str, path: str) -> dict[str, Any]:
        try:
            response = getattr(self._http, method)(path)
            data = response.json()
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Auth cache request failed ({method.upper()} {path}): {e}") from e
        if not isinstance(data, dict):
            raise ArcError(f"Unexpected response from {path}: {data!r}")
        return data
