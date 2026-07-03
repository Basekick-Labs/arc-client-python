"""Base HTTP client configuration and utilities."""

from __future__ import annotations

import os
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
    ArcRateLimitError,
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


def handle_response_error(response: httpx.Response) -> None:
    """Handle HTTP error responses and raise appropriate exceptions."""
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
        raise ArcAuthenticationError(f"Permission denied: {message}")
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
