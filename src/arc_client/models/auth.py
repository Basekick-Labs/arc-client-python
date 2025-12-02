"""Authentication-related models."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class TokenInfo(BaseModel):
    """Information about an API token.

    Attributes:
        id: Unique token identifier.
        name: Human-readable token name.
        description: Optional token description.
        permissions: List of permissions (read, write, delete, admin).
        created_at: When the token was created.
        last_used_at: When the token was last used.
        enabled: Whether the token is active.
        expires_at: When the token expires (if set).
    """

    id: int
    name: str
    description: str | None = None
    permissions: list[str] = Field(default_factory=list)
    created_at: datetime | None = None
    last_used_at: datetime | None = None
    enabled: bool = True
    expires_at: datetime | None = None


class VerifyResponse(BaseModel):
    """Response from token verification.

    Attributes:
        valid: Whether the token is valid.
        token_info: Token information if valid.
        permissions: List of permissions.
        error: Error message if invalid.
    """

    valid: bool
    token_info: TokenInfo | None = None
    permissions: list[str] | None = None
    error: str | None = None


class CreateTokenResponse(BaseModel):
    """Response from token creation.

    Attributes:
        success: Whether creation succeeded.
        token: The new token value (only shown once).
        message: Status message.
        error: Error message if failed.
    """

    success: bool
    token: str | None = None
    message: str | None = None
    error: str | None = None


class TokenListResponse(BaseModel):
    """Response from listing tokens.

    Attributes:
        success: Whether the request succeeded.
        tokens: List of token information.
        count: Number of tokens.
        error: Error message if failed.
    """

    success: bool
    tokens: list[TokenInfo] = Field(default_factory=list)
    count: int = 0
    error: str | None = None


class RotateTokenResponse(BaseModel):
    """Response from token rotation.

    Attributes:
        success: Whether rotation succeeded.
        new_token: The new token value (only shown once).
        message: Status message.
        error: Error message if failed.
    """

    success: bool
    new_token: str | None = None
    message: str | None = None
    error: str | None = None
