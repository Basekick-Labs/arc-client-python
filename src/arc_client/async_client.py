"""Asynchronous Arc client."""

from __future__ import annotations

from typing import Any

from arc_client.config import ClientConfig
from arc_client.http.async_http import AsyncHTTPClient
from arc_client.models.common import HealthResponse


class AsyncArcClient:
    """Asynchronous client for Arc time-series database.

    Example:
        >>> async with AsyncArcClient(host="localhost", token="my-token") as client:
        ...     health = await client.health()
        ...     print(health.status)
        ...
        ...     await client.write.write_columnar(
        ...         measurement="cpu",
        ...         columns={
        ...             "time": [1633024800000000, 1633024801000000],
        ...             "host": ["server01", "server01"],
        ...             "usage": [45.2, 47.8],
        ...         },
        ...     )
        ...
        ...     df = await client.query.query_pandas("SELECT * FROM cpu LIMIT 100")
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8000,
        token: str | None = None,
        database: str = "default",
        timeout: float = 30.0,
        compression: bool = True,
        ssl: bool = False,
        verify_ssl: bool = True,
    ) -> None:
        """Initialize the async Arc client.

        Args:
            host: Arc server hostname.
            port: Arc server port.
            token: API token for authentication.
            database: Default database name.
            timeout: Request timeout in seconds.
            compression: Enable gzip compression for writes.
            ssl: Use HTTPS.
            verify_ssl: Verify SSL certificates.
        """
        self._config = ClientConfig(
            host=host,
            port=port,
            token=token,
            database=database,
            timeout=timeout,
            compression=compression,
            ssl=ssl,
            verify_ssl=verify_ssl,
        )
        self._http: AsyncHTTPClient | None = None

        # Lazy-initialized sub-clients
        self._write: Any = None
        self._query: Any = None
        self._auth: Any = None
        self._retention: Any = None
        self._continuous_queries: Any = None
        self._delete: Any = None

    def _get_http(self) -> AsyncHTTPClient:
        """Get or create the HTTP client."""
        if self._http is None:
            self._http = AsyncHTTPClient(self._config)
        return self._http

    @property
    def config(self) -> ClientConfig:
        """Get the client configuration."""
        return self._config

    @property
    def write(self) -> Any:
        """Get the async write client for data ingestion."""
        if self._write is None:
            from arc_client.ingestion.async_writer import AsyncWriteClient

            self._write = AsyncWriteClient(self._get_http(), self._config)
        return self._write

    @property
    def query(self) -> Any:
        """Get the async query client for SQL queries."""
        if self._query is None:
            from arc_client.query.async_executor import AsyncQueryClient

            self._query = AsyncQueryClient(self._get_http(), self._config)
        return self._query

    @property
    def auth(self) -> Any:
        """Get the async auth client for token management."""
        if self._auth is None:
            from arc_client.auth.async_manager import AsyncAuthClient

            self._auth = AsyncAuthClient(self._get_http(), self._config)
        return self._auth

    @property
    def retention(self) -> Any:
        """Get the async retention client for policy management."""
        if self._retention is None:
            from arc_client.management.async_retention import AsyncRetentionClient

            self._retention = AsyncRetentionClient(self._get_http(), self._config)
        return self._retention

    @property
    def continuous_queries(self) -> Any:
        """Get the async continuous queries client."""
        if self._continuous_queries is None:
            from arc_client.management.async_continuous_query import AsyncContinuousQueryClient

            self._continuous_queries = AsyncContinuousQueryClient(self._get_http(), self._config)
        return self._continuous_queries

    @property
    def delete(self) -> Any:
        """Get the async delete client for data deletion."""
        if self._delete is None:
            from arc_client.management.async_delete import AsyncDeleteClient

            self._delete = AsyncDeleteClient(self._get_http(), self._config)
        return self._delete

    async def health(self) -> HealthResponse:
        """Check server health.

        Returns:
            HealthResponse with server status.

        Raises:
            ArcConnectionError: If connection to server fails.
        """
        response = await self._get_http().get("/health")
        return HealthResponse.model_validate(response.json())

    async def ready(self) -> bool:
        """Check if server is ready to accept requests.

        Returns:
            True if server is ready, False otherwise.
        """
        try:
            response = await self._get_http().get("/ready")
            return response.status_code == 200
        except Exception:
            return False

    async def close(self) -> None:
        """Close the client and release resources."""
        if self._http is not None:
            await self._http.close()
            self._http = None

    async def __aenter__(self) -> AsyncArcClient:
        """Enter async context manager."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Exit async context manager."""
        await self.close()

    def __repr__(self) -> str:
        return f"AsyncArcClient(host={self._config.host!r}, port={self._config.port})"
