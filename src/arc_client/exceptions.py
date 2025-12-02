"""Arc client exceptions."""

from __future__ import annotations

from typing import Optional


class ArcError(Exception):
    """Base exception for arc-client."""

    pass


class ArcConnectionError(ArcError):
    """Connection to Arc server failed."""

    pass


class ArcAuthenticationError(ArcError):
    """Authentication failed."""

    pass


class ArcQueryError(ArcError):
    """Query execution failed."""

    pass


class ArcIngestionError(ArcError):
    """Data ingestion failed."""

    pass


class ArcValidationError(ArcError):
    """Request validation failed."""

    pass


class ArcNotFoundError(ArcError):
    """Resource not found."""

    pass


class ArcRateLimitError(ArcError):
    """Rate limit exceeded."""

    pass


class ArcServerError(ArcError):
    """Server returned an error."""

    def __init__(self, message: str, status_code: Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code
