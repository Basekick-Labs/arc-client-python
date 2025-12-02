"""Synchronous Arc client."""

from __future__ import annotations

from typing import Any, Optional

from arc_client.config import ClientConfig
from arc_client.http.sync_http import SyncHTTPClient
from arc_client.models.common import HealthResponse


class ArcClient:
    """Synchronous client for Arc time-series database.

    Example:
        >>> with ArcClient(host="localhost", token="my-token") as client:
        ...     health = client.health()
        ...     print(health.status)
        ...
        ...     client.write.write_columnar(
        ...         measurement="cpu",
        ...         columns={
        ...             "time": [1633024800000000, 1633024801000000],
        ...             "host": ["server01", "server01"],
        ...             "usage": [45.2, 47.8],
        ...         },
        ...     )
        ...
        ...     df = client.query.query_pandas("SELECT * FROM cpu LIMIT 100")
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8000,
        token: Optional[str] = None,
        database: str = "default",
        timeout: float = 30.0,
        compression: bool = True,
        ssl: bool = False,
        verify_ssl: bool = True,
    ) -> None:
        """Initialize the Arc client.

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
        self._http: Optional[SyncHTTPClient] = None

        # Lazy-initialized sub-clients
        self._write: Any = None
        self._query: Any = None
        self._auth: Any = None
        self._retention: Any = None
        self._continuous_queries: Any = None
        self._delete: Any = None

    def _get_http(self) -> SyncHTTPClient:
        """Get or create the HTTP client."""
        if self._http is None:
            self._http = SyncHTTPClient(self._config)
        return self._http

    @property
    def config(self) -> ClientConfig:
        """Get the client configuration."""
        return self._config

    @property
    def write(self) -> Any:
        """Get the write client for data ingestion."""
        if self._write is None:
            from arc_client.ingestion.writer import WriteClient

            self._write = WriteClient(self._get_http(), self._config)
        return self._write

    @property
    def query(self) -> Any:
        """Get the query client for SQL queries."""
        if self._query is None:
            from arc_client.query.executor import QueryClient

            self._query = QueryClient(self._get_http(), self._config)
        return self._query

    @property
    def auth(self) -> Any:
        """Get the auth client for token management."""
        if self._auth is None:
            from arc_client.auth.manager import AuthClient

            self._auth = AuthClient(self._get_http(), self._config)
        return self._auth

    @property
    def retention(self) -> Any:
        """Get the retention client for policy management."""
        if self._retention is None:
            from arc_client.management.retention import RetentionClient

            self._retention = RetentionClient(self._get_http(), self._config)
        return self._retention

    @property
    def continuous_queries(self) -> Any:
        """Get the continuous queries client."""
        if self._continuous_queries is None:
            from arc_client.management.continuous_query import ContinuousQueryClient

            self._continuous_queries = ContinuousQueryClient(self._get_http(), self._config)
        return self._continuous_queries

    @property
    def delete(self) -> Any:
        """Get the delete client for data deletion."""
        if self._delete is None:
            from arc_client.management.delete import DeleteClient

            self._delete = DeleteClient(self._get_http(), self._config)
        return self._delete

    def health(self) -> HealthResponse:
        """Check server health.

        Returns:
            HealthResponse with server status.

        Raises:
            ArcConnectionError: If connection to server fails.
        """
        response = self._get_http().get("/health")
        return HealthResponse.model_validate(response.json())

    def ready(self) -> bool:
        """Check if server is ready to accept requests.

        Returns:
            True if server is ready, False otherwise.
        """
        try:
            response = self._get_http().get("/ready")
            return response.status_code == 200
        except Exception:
            return False

    def close(self) -> None:
        """Close the client and release resources."""
        if self._http is not None:
            self._http.close()
            self._http = None

    def __enter__(self) -> ArcClient:
        """Enter context manager."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit context manager."""
        self.close()

    def __repr__(self) -> str:
        return f"ArcClient(host={self._config.host!r}, port={self._config.port})"
