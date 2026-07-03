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


class ArcNotSupportedError(ArcError):
    """The server does not support the requested operation.

    Raised, for example, when calling ``query_msgpack`` against an Arc server
    that was not built with the ``duckdb_arrow`` build tag (HTTP 501).
    """

    pass


class ArcLicenseError(ArcAuthenticationError):
    """The requested feature requires an enterprise license.

    Subclasses :class:`ArcAuthenticationError` so existing ``except`` blocks
    that catch permission failures keep working.
    """

    pass
