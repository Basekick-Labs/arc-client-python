"""Asynchronous auth client for Arc."""

from __future__ import annotations

from typing import Any, List, Optional

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcAuthenticationError, ArcNotFoundError
from arc_client.http.async_http import AsyncHTTPClient
from arc_client.models.auth import (
    CreateTokenResponse,
    RotateTokenResponse,
    TokenInfo,
    TokenListResponse,
    VerifyResponse,
)


class AsyncAuthClient:
    """Asynchronous client for authentication and token management."""

    def __init__(self, http: AsyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    async def verify(self) -> VerifyResponse:
        """Verify the current token."""
        if not self._config.token:
            return VerifyResponse(valid=False, error="No token configured")

        try:
            response = await self._http.get("/api/v1/auth/verify")
            data = response.json()
            return VerifyResponse(**data)
        except ArcAuthenticationError:
            return VerifyResponse(valid=False, error="Invalid or expired token")
        except Exception as e:
            raise ArcAuthenticationError(f"Token verification failed: {e}") from e

    async def create_token(
        self,
        name: str,
        description: Optional[str] = None,
        permissions: Optional[list[str]] = None,
        expires_in: Optional[str] = None,
    ) -> CreateTokenResponse:
        """Create a new API token. Requires admin permissions."""
        payload: dict[str, Any] = {"name": name}
        if description:
            payload["description"] = description
        if permissions:
            payload["permissions"] = permissions
        if expires_in:
            payload["expires_in"] = expires_in

        try:
            response = await self._http.post("/api/v1/auth/tokens", json=payload)
            data = response.json()
            return CreateTokenResponse(**data)
        except ArcAuthenticationError:
            raise
        except Exception as e:
            raise ArcAuthenticationError(f"Failed to create token: {e}") from e

    async def list_tokens(self) -> List[TokenInfo]:
        """List all tokens. Requires admin permissions."""
        try:
            response = await self._http.get("/api/v1/auth/tokens")
            data = response.json()
            result = TokenListResponse(**data)
            if not result.success:
                raise ArcAuthenticationError(result.error or "Failed to list tokens")
            return result.tokens
        except ArcAuthenticationError:
            raise
        except Exception as e:
            raise ArcAuthenticationError(f"Failed to list tokens: {e}") from e

    async def get_token(self, token_id: int) -> TokenInfo:
        """Get token details by ID. Requires admin permissions."""
        try:
            response = await self._http.get(f"/api/v1/auth/tokens/{token_id}")
            data = response.json()
            if not data.get("success", True):
                error = data.get("error", "Unknown error")
                if "not found" in error.lower():
                    raise ArcNotFoundError(f"Token {token_id} not found")
                raise ArcAuthenticationError(error)
            return TokenInfo(**data["token"])
        except (ArcNotFoundError, ArcAuthenticationError):
            raise
        except Exception as e:
            raise ArcAuthenticationError(f"Failed to get token: {e}") from e

    async def update_token(
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
            response = await self._http.patch(f"/api/v1/auth/tokens/{token_id}", json=payload)
            data = response.json()
            if not data.get("success", True):
                error = data.get("error", "Unknown error")
                if "not found" in error.lower():
                    raise ArcNotFoundError(f"Token {token_id} not found")
                raise ArcAuthenticationError(error)
        except (ArcNotFoundError, ArcAuthenticationError):
            raise
        except Exception as e:
            raise ArcAuthenticationError(f"Failed to update token: {e}") from e

    async def delete_token(self, token_id: int) -> None:
        """Delete a token. Requires admin permissions."""
        try:
            response = await self._http.delete(f"/api/v1/auth/tokens/{token_id}")
            data = response.json()
            if not data.get("success", True):
                error = data.get("error", "Unknown error")
                if "not found" in error.lower():
                    raise ArcNotFoundError(f"Token {token_id} not found")
                raise ArcAuthenticationError(error)
        except (ArcNotFoundError, ArcAuthenticationError):
            raise
        except Exception as e:
            raise ArcAuthenticationError(f"Failed to delete token: {e}") from e

    async def rotate_token(self, token_id: int) -> str:
        """Rotate a token and return the new value. Requires admin permissions."""
        try:
            response = await self._http.post(f"/api/v1/auth/tokens/{token_id}/rotate")
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
        except (ArcNotFoundError, ArcAuthenticationError):
            raise
        except Exception as e:
            raise ArcAuthenticationError(f"Failed to rotate token: {e}") from e

    async def revoke_token(self, token_id: int) -> None:
        """Revoke a token. Requires admin permissions."""
        try:
            response = await self._http.post(f"/api/v1/auth/tokens/{token_id}/revoke")
            data = response.json()
            if not data.get("success", True):
                error = data.get("error", "Unknown error")
                if "not found" in error.lower():
                    raise ArcNotFoundError(f"Token {token_id} not found")
                raise ArcAuthenticationError(error)
        except (ArcNotFoundError, ArcAuthenticationError):
            raise
        except Exception as e:
            raise ArcAuthenticationError(f"Failed to revoke token: {e}") from e
