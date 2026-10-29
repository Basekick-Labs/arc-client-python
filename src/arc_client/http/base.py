"""Base HTTP client configuration and utilities."""

from __future__ import annotations

import os
import re
from typing import Any, Optional

import httpx

from arc_client import __version__
from arc_client.config import ClientConfig
from arc_client.exceptions import (
    ArcAuthenticationError,
    ArcConnectionError,
    ArcLicenseError,
    ArcNotFoundError,
    ArcNotSupportedError,
    ArcPermissionDataUnavailableError,
    ArcPermissionError,
    ArcRateLimitError,
    ArcScopedAccessError,
    ArcServerError,
)

USER_AGENT = f"arc-client-python/{__version__}"


def build_headers(
    config: ClientConfig, extra_headers: Optional[dict[str, str]] = None
) -> dict[str, str]:
    """Build request headers with authentication."""
    headers: dict[str, str] = {
        "User-Agent": USER_AGENT,
    }
    if config.token:
        headers["Authorization"] = f"Bearer {config.token}"
    if config.database:
        headers["x-arc-database"] = config.database
    if extra_headers:
        headers.update(extra_headers)
    return headers


# Arc sends HTTP 403 for three different things and distinguishes them only
# by message, so the message is what these read. The shapes its read
# middleware and handler gates produce:
#
#   no permission for read on database 'x'               -> scoped
#   access denied: no read permission for database 'x'   -> scoped
#   permission data unavailable                          -> grants unreadable
#   token does not have 'read' permission                -> no read permission
#   Permission denied: read required                     -> no read permission
#
# Only the first two are resolved by naming a different database, so they
# must not collapse onto one exception. The patterns match the stable parts
# of each message rather than the whole string, so a change to the permission
# word or the surrounding punctuation cannot silently reclassify one as
# another, and the database name is captured where Arc sends one.
_SCOPED_DENIAL_RES = (
    # The read middleware on the listing routes. The name is empty on the
    # list-everything route, which sends no database.
    re.compile(
        r"^no permission for \w+ on database '(?P<database>[^']*)'",
        re.IGNORECASE,
    ),
    # The handler gates' own wording.
    re.compile(
        r"^access denied: no \w+ permission for database '(?P<database>[^']*)'",
        re.IGNORECASE,
    ),
)

# Arc could not load the token's grants and refused rather than guess. A 403
# like the others, but it says nothing about the token or the database, so it
# must not be reported as a permission problem. Checked first.
_PERMISSION_DATA_UNAVAILABLE = "permission data unavailable"


def _scoped_denial_database(message: object) -> Optional[str]:
    """Return the database named by a per-database denial, else None.

    An empty capture is returned as ``""`` — Arc sends an empty name on the
    list-everything route — so callers must test against ``None``, not
    truthiness, to tell "not a scoped denial" from "scoped, no name".
    """
    if not isinstance(message, str):
        return None
    text = message.strip()
    for pattern in _SCOPED_DENIAL_RES:
        match = pattern.match(text)
        if match is not None:
            return match.group("database")
    return None


def _is_permission_data_unavailable(message: object) -> bool:
    """True if Arc failed closed because it could not load the grants."""
    return isinstance(message, str) and _PERMISSION_DATA_UNAVAILABLE in message.lower()


def handle_response_error(response: httpx.Response) -> None:
    """Handle HTTP error responses and raise appropriate exceptions.

    Raises:
        ArcAuthenticationError: HTTP 401 — the token is missing, invalid or
            expired. Re-authenticate.
        ArcLicenseError: HTTP 403 naming an enterprise license.
        ArcScopedAccessError: HTTP 403 from Arc's per-database permission
            check — the token may read, but not the database asked for. A
            subclass of ``ArcPermissionError``; carries ``database``.
        ArcPermissionDataUnavailableError: HTTP 403 because Arc could not
            load the token's grants and failed closed. A server-side fault,
            not a statement about the token.
        ArcPermissionError: any other HTTP 403 — the token does not carry the
            permission the endpoint requires.
        ArcNotFoundError: HTTP 404.
        ArcRateLimitError: HTTP 429.
        ArcNotSupportedError: HTTP 501.
        ArcServerError: HTTP 5xx and any other non-2xx status.
    """
    if response.is_success:
        return

    status_code = response.status_code
    try:
        error_body = response.json()
        message = error_body.get("error", error_body.get("message", response.text))
    except Exception:
        message = response.text or f"HTTP {status_code}"

    if status_code == 401:
        raise ArcAuthenticationError(f"Authentication failed: {message}")
    elif status_code == 403:
        # Arc returns 403 for missing enterprise-license features with a
        # distinctive message; surface those as ArcLicenseError (a subclass of
        # ArcAuthenticationError, so plain permission-denied handling still works).
        if isinstance(message, str) and "enterprise license" in message.lower():
            raise ArcLicenseError(f"Enterprise license required: {message}")
        # Checked before the scoped shape: Arc failing to read its own
        # permission data is a server fault, and reporting it as "your token
        # is scoped" would send the caller after a database that was never
        # the problem.
        if _is_permission_data_unavailable(message):
            raise ArcPermissionDataUnavailableError(
                f"Arc could not read its own permission data and refused the request "
                f"rather than guessing. This is a server-side fault, not a problem "
                f"with this token — check the Arc server log. ({message})"
            )
        # Arc's per-database check refuses a token that may read but holds no
        # grant for the database asked for. Naming a database it is granted is
        # the only thing that resolves it, so it gets its own exception rather
        # than being reported as "this token cannot read".
        database = _scoped_denial_database(message)
        if database is not None:
            if database in ("", "*"):
                detail = (
                    "this token is scoped to specific databases, so Arc will not list "
                    "them all. Name one with get() or list_measurements()."
                )
            else:
                detail = (
                    f"this token is scoped to specific databases and holds no read "
                    f"grant for {database!r}. Name a database it is granted."
                )
            raise ArcScopedAccessError(
                f"Access denied: {detail} ({message})",
                database=database,
            )
        raise ArcPermissionError(f"Permission denied: {message}")
    elif status_code == 404:
        raise ArcNotFoundError(f"Resource not found: {message}")
    elif status_code == 429:
        raise ArcRateLimitError(f"Rate limit exceeded: {message}")
    elif status_code == 501:
        raise ArcNotSupportedError(f"Operation not supported by server: {message}")
    elif status_code >= 500:
        raise ArcServerError(f"Server error: {message}", status_code=status_code)
    else:
        raise ArcServerError(f"Request failed ({status_code}): {message}", status_code=status_code)


def handle_connection_error(error: Exception, url: str) -> None:
    """Handle connection errors and raise ArcConnectionError."""
    raise ArcConnectionError(f"Failed to connect to {url}: {error}") from error


# Transport errors that are safe to retry (connection refused, DNS, timeouts).
RETRYABLE_TRANSPORT_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.RemoteProtocolError,
)

# HTTP status codes worth retrying: 429 rate-limit, 503 schema-churn /
# replication catch-up (both explicitly retryable per the Arc server).
RETRYABLE_STATUS_CODES = frozenset({429, 503})


def is_retryable_status(status_code: int) -> bool:
    """Return True if a response status should be retried."""
    return status_code in RETRYABLE_STATUS_CODES


def parse_retry_after(response: httpx.Response) -> Optional[float]:
    """Parse a ``Retry-After`` header (seconds only) into a float, if present."""
    value = response.headers.get("Retry-After")
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        # HTTP-date form is not honored here; fall back to backoff.
        return None
    return max(0.0, seconds)


def compute_backoff(attempt: int, base: float, retry_after: Optional[float] = None) -> float:
    """Compute the delay before the next retry.

    Honors an explicit ``Retry-After`` when provided; otherwise uses capped
    exponential backoff (``base * 2**attempt``) with full jitter. ``attempt`` is
    0-indexed (0 = delay before the first retry).
    """
    if retry_after is not None:
        return retry_after
    ceiling: float = min(base * (2**attempt), 30.0)
    # Full jitter in [0, ceiling]; os.urandom avoids seeding a global RNG.
    frac: float = int.from_bytes(os.urandom(4), "big") / 0xFFFFFFFF
    return ceiling * frac


class HTTPClientBase:
    """Base class for HTTP clients with common functionality."""

    def __init__(self, config: ClientConfig) -> None:
        self.config = config
        self._base_url = config.base_url

    def _build_url(self, path: str) -> str:
        """Build full URL from path."""
        if path.startswith("/"):
            return f"{self._base_url}{path}"
        return f"{self._base_url}/{path}"

    def _prepare_request_kwargs(
        self,
        headers: Optional[dict[str, str]] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Prepare common request kwargs."""
        request_headers = build_headers(self.config, headers)
        return {
            "headers": request_headers,
            "timeout": self.config.timeout,
            **kwargs,
        }
