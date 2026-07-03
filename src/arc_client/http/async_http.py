"""Asynchronous HTTP client for Arc."""

from __future__ import annotations

import asyncio
from typing import Any, Optional

import httpx

from arc_client.config import ClientConfig
from arc_client.http.base import (
    RETRYABLE_TRANSPORT_ERRORS,
    HTTPClientBase,
    compute_backoff,
    handle_connection_error,
    handle_response_error,
    is_retryable_status,
    parse_retry_after,
)


class AsyncHTTPClient(HTTPClientBase):
    """Asynchronous HTTP client using httpx."""

    def __init__(self, config: ClientConfig) -> None:
        super().__init__(config)
        self._client: Optional[httpx.AsyncClient] = None

    def _get_client(self) -> httpx.AsyncClient:
        """Get or create the httpx async client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=self.config.timeout,
                verify=self.config.verify_ssl,
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _request(
        self,
        method: str,
        path: str,
        *,
        headers: Optional[dict[str, str]] = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send a request with retry/backoff for transient failures.

        Retries connection/timeout errors and 429/503 responses up to
        ``config.max_retries`` times, honoring ``Retry-After`` when present.
        """
        request_kwargs = self._prepare_request_kwargs(headers=headers, **kwargs)
        client = self._get_client()
        last_exc: Optional[Exception] = None

        for attempt in range(self.config.max_retries + 1):
            try:
                response = await client.request(method, path, **request_kwargs)
            except RETRYABLE_TRANSPORT_ERRORS as e:
                last_exc = e
                if attempt >= self.config.max_retries:
                    break
                await asyncio.sleep(compute_backoff(attempt, self.config.retry_backoff))
                continue

            if is_retryable_status(response.status_code) and attempt < self.config.max_retries:
                retry_after = parse_retry_after(response)
                await asyncio.sleep(
                    compute_backoff(attempt, self.config.retry_backoff, retry_after)
                )
                continue

            handle_response_error(response)
            return response

        # Exhausted retries on a transport error.
        assert last_exc is not None
        handle_connection_error(last_exc, self._build_url(path))
        raise last_exc  # Never reached, but satisfies the type checker.

    async def get(
        self,
        path: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> httpx.Response:
        """Make a GET request."""
        return await self._request("GET", path, headers=headers, params=params)

    async def post(
        self,
        path: str,
        json: Optional[Any] = None,
        content: Optional[bytes] = None,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        files: Optional[Any] = None,
        data: Optional[Any] = None,
    ) -> httpx.Response:
        """Make a POST request."""
        extra: dict[str, Any] = {}
        if json is not None:
            extra["json"] = json
        if content is not None:
            extra["content"] = content
        if files is not None:
            extra["files"] = files
        if data is not None:
            extra["data"] = data
        return await self._request("POST", path, headers=headers, params=params, **extra)

    async def put(
        self,
        path: str,
        json: Optional[Any] = None,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> httpx.Response:
        """Make a PUT request."""
        extra: dict[str, Any] = {}
        if json is not None:
            extra["json"] = json
        return await self._request("PUT", path, headers=headers, params=params, **extra)

    async def patch(
        self,
        path: str,
        json: Optional[Any] = None,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> httpx.Response:
        """Make a PATCH request."""
        extra: dict[str, Any] = {}
        if json is not None:
            extra["json"] = json
        return await self._request("PATCH", path, headers=headers, params=params, **extra)

    async def delete(
        self,
        path: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> httpx.Response:
        """Make a DELETE request."""
        return await self._request("DELETE", path, headers=headers, params=params)

    async def __aenter__(self) -> AsyncHTTPClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()
