"""Asynchronous HTTP client for Arc."""

from __future__ import annotations

import asyncio
from typing import Any, Optional

import httpx

from arc_client.config import ClientConfig
from arc_client.http.arrow_trailers import capture_async_arrow_trailers
from arc_client.http.base import (
    RETRYABLE_TRANSPORT_ERRORS,
    HTTPClientBase,
    RetryPredicate,
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
                event_hooks={"response": [capture_async_arrow_trailers]},
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
        idempotent: bool = False,
        retry_if: Optional[RetryPredicate] = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send a request with retry/backoff for transient failures.

        Reads (GET/HEAD/OPTIONS) retry transient transport failures and
        429/503 responses up to ``config.max_retries`` times. Requests the
        caller marks ``idempotent=True`` (Arc's query POSTs, which only read)
        retry 503 (not 429, see below); for transport failures they keep the
        mutation rule (only connection establishment failures), because a
        timeout or a connection lost mid-response means Arc already ran the
        query, and silently re-running it would multiply its cost and hide a
        truncated stream. Data DELETE never retries. Unknown write outcomes
        are returned to the caller.

        ``retry_if`` replaces the status-retry rule for this one request: a
        response for which it returns True is retried (honoring Retry-After),
        whatever the method. Writes use it to resend only the 503 refusals Arc
        documents as "nothing was written".
        """
        request_kwargs = self._prepare_request_kwargs(headers=headers, **kwargs)
        client = self._get_client()
        last_exc: Optional[Exception] = None
        read_only = method.upper() in {"GET", "HEAD", "OPTIONS"}
        # DELETE may partially commit. Other mutations may be retried only
        # when no application request could have reached the server.
        data_delete = method.upper() == "POST" and path.rstrip("/") == "/api/v1/delete"

        for attempt in range(self.config.max_retries + 1):
            try:
                response = await client.request(method, path, **request_kwargs)
            except RETRYABLE_TRANSPORT_ERRORS as e:
                last_exc = e
                if (
                    data_delete
                    or not (read_only or isinstance(e, (httpx.ConnectError, httpx.ConnectTimeout)))
                    or attempt >= self.config.max_retries
                ):
                    break
                await asyncio.sleep(compute_backoff(attempt, self.config.retry_backoff))
                continue

            if retry_if is not None:
                retry_status = retry_if(response)
            else:
                # Idempotent POSTs (queries) retry only 503: Arc's query 429
                # is governance (rate limit or quota) with no Retry-After, and
                # does not clear within a few seconds of backoff.
                retry_status = (read_only and is_retryable_status(response.status_code)) or (
                    idempotent and response.status_code == 503
                )
            if retry_status and attempt < self.config.max_retries:
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
        retry_if: Optional[RetryPredicate] = None,
    ) -> httpx.Response:
        """Make a GET request."""
        return await self._request("GET", path, headers=headers, params=params, retry_if=retry_if)

    async def post(
        self,
        path: str,
        json: Optional[Any] = None,
        content: Optional[bytes] = None,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        files: Optional[Any] = None,
        data: Optional[Any] = None,
        idempotent: bool = False,
        retry_if: Optional[RetryPredicate] = None,
    ) -> httpx.Response:
        """Make a POST request.

        ``idempotent=True`` marks a POST that only reads (Arc's query
        endpoints) so it retries like a GET. ``retry_if`` overrides the status
        retry rule for this request (see ``_request``).
        """
        extra: dict[str, Any] = {}
        if json is not None:
            extra["json"] = json
        if content is not None:
            extra["content"] = content
        if files is not None:
            extra["files"] = files
        if data is not None:
            extra["data"] = data
        return await self._request(
            "POST",
            path,
            headers=headers,
            params=params,
            idempotent=idempotent,
            retry_if=retry_if,
            **extra,
        )

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
