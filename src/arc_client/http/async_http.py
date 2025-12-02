"""Asynchronous HTTP client for Arc."""

from __future__ import annotations

from typing import Any, Optional

import httpx

from arc_client.config import ClientConfig
from arc_client.http.base import (
    HTTPClientBase,
    handle_connection_error,
    handle_response_error,
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

    async def get(
        self,
        path: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> httpx.Response:
        """Make a GET request."""
        kwargs = self._prepare_request_kwargs(headers=headers, params=params)
        try:
            response = await self._get_client().get(path, **kwargs)
            handle_response_error(response)
            return response
        except httpx.ConnectError as e:
            handle_connection_error(e, self._build_url(path))
            raise  # Never reached, but satisfies type checker

    async def post(
        self,
        path: str,
        json: Optional[Any] = None,
        content: Optional[bytes] = None,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> httpx.Response:
        """Make a POST request."""
        kwargs = self._prepare_request_kwargs(headers=headers, params=params)
        if json is not None:
            kwargs["json"] = json
        if content is not None:
            kwargs["content"] = content
        try:
            response = await self._get_client().post(path, **kwargs)
            handle_response_error(response)
            return response
        except httpx.ConnectError as e:
            handle_connection_error(e, self._build_url(path))
            raise

    async def put(
        self,
        path: str,
        json: Optional[Any] = None,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> httpx.Response:
        """Make a PUT request."""
        kwargs = self._prepare_request_kwargs(headers=headers, params=params)
        if json is not None:
            kwargs["json"] = json
        try:
            response = await self._get_client().put(path, **kwargs)
            handle_response_error(response)
            return response
        except httpx.ConnectError as e:
            handle_connection_error(e, self._build_url(path))
            raise

    async def patch(
        self,
        path: str,
        json: Optional[Any] = None,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> httpx.Response:
        """Make a PATCH request."""
        kwargs = self._prepare_request_kwargs(headers=headers, params=params)
        if json is not None:
            kwargs["json"] = json
        try:
            response = await self._get_client().patch(path, **kwargs)
            handle_response_error(response)
            return response
        except httpx.ConnectError as e:
            handle_connection_error(e, self._build_url(path))
            raise

    async def delete(
        self,
        path: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> httpx.Response:
        """Make a DELETE request."""
        kwargs = self._prepare_request_kwargs(headers=headers, params=params)
        try:
            response = await self._get_client().delete(path, **kwargs)
            handle_response_error(response)
            return response
        except httpx.ConnectError as e:
            handle_connection_error(e, self._build_url(path))
            raise

    async def __aenter__(self) -> AsyncHTTPClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()
