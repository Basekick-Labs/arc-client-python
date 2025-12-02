"""Base HTTP client configuration and utilities."""

from __future__ import annotations

from typing import Any, Optional

import httpx

from arc_client.config import ClientConfig
from arc_client.exceptions import (
    ArcAuthenticationError,
    ArcConnectionError,
    ArcNotFoundError,
    ArcRateLimitError,
    ArcServerError,
)


def build_headers(
    config: ClientConfig, extra_headers: Optional[dict[str, str]] = None
) -> dict[str, str]:
    """Build request headers with authentication."""
    headers: dict[str, str] = {
        "User-Agent": "arc-client-python/0.1.0",
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
        raise ArcAuthenticationError(f"Permission denied: {message}")
    elif status_code == 404:
        raise ArcNotFoundError(f"Resource not found: {message}")
    elif status_code == 429:
        raise ArcRateLimitError(f"Rate limit exceeded: {message}")
    elif status_code >= 500:
        raise ArcServerError(f"Server error: {message}", status_code=status_code)
    else:
        raise ArcServerError(f"Request failed ({status_code}): {message}", status_code=status_code)


def handle_connection_error(error: Exception, url: str) -> None:
    """Handle connection errors and raise ArcConnectionError."""
    raise ArcConnectionError(f"Failed to connect to {url}: {error}") from error


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
