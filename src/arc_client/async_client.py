"""Asynchronous Arc client."""

from __future__ import annotations

from typing import Any, Optional

from arc_client.compression import Compression
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
        token: Optional[str] = None,
        database: str = "default",
        timeout: float = 30.0,
        compression: Compression | str | bool = Compression.ZSTD,
        ssl: bool = False,
        verify_ssl: bool = True,
        max_retries: int = 3,
        retry_backoff: float = 0.5,
    ) -> None:
        """Initialize the async Arc client.

        Args:
            host: Arc server hostname.
            port: Arc server port.
            token: API token for authentication.
            database: Default database name.
            timeout: Request timeout in seconds.
            compression: Write compression codec (none/gzip/zstd, or bool for
                back-compat). Defaults to zstd.
            ssl: Use HTTPS.
            verify_ssl: Verify SSL certificates.
            max_retries: Max automatic retries for transient failures
                (connection errors, 429, 503). 0 disables retries.
            retry_backoff: Base backoff (seconds) for exponential retry.
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
            max_retries=max_retries,
            retry_backoff=retry_backoff,
        )
        self._http: Optional[AsyncHTTPClient] = None

        # Lazy-initialized sub-clients
        self._write: Any = None
        self._query: Any = None
        self._auth: Any = None
        self._retention: Any = None
        self._continuous_queries: Any = None
        self._delete: Any = None
        self._import: Any = None
        self._databases: Any = None
        self._rbac: Any = None
        self._governance: Any = None
        self._audit: Any = None
        self._queries: Any = None
        self._cluster: Any = None
        self._compaction: Any = None
        self._tiering: Any = None
        self._backup: Any = None
        self._mqtt: Any = None

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

    @property
    def import_(self) -> Any:
        """Get the async bulk import client (CSV, Parquet, line protocol, TLE)."""
        if self._import is None:
            from arc_client.ingestion.async_importer import AsyncImportClient

            self._import = AsyncImportClient(self._get_http(), self._config)
        return self._import

    @property
    def databases(self) -> Any:
        """Get the async databases management client."""
        if self._databases is None:
            from arc_client.management.async_databases import AsyncDatabaseClient

            self._databases = AsyncDatabaseClient(self._get_http(), self._config)
        return self._databases

    @property
    def rbac(self) -> Any:
        """Get the async RBAC management client (enterprise)."""
        if self._rbac is None:
            from arc_client.management.async_rbac import AsyncRBACClient

            self._rbac = AsyncRBACClient(self._get_http(), self._config)
        return self._rbac

    @property
    def governance(self) -> Any:
        """Get the async query governance client (enterprise)."""
        if self._governance is None:
            from arc_client.management.async_governance import AsyncGovernanceClient

            self._governance = AsyncGovernanceClient(self._get_http(), self._config)
        return self._governance

    @property
    def audit(self) -> Any:
        """Get the async audit log client (enterprise)."""
        if self._audit is None:
            from arc_client.management.async_audit import AsyncAuditClient

            self._audit = AsyncAuditClient(self._get_http(), self._config)
        return self._audit

    @property
    def queries(self) -> Any:
        """Get the async query management client (active/history/kill)."""
        if self._queries is None:
            from arc_client.management.async_query_management import (
                AsyncQueryManagementClient,
            )

            self._queries = AsyncQueryManagementClient(self._get_http(), self._config)
        return self._queries

    @property
    def cluster(self) -> Any:
        """Get the async cluster management client (enterprise)."""
        if self._cluster is None:
            from arc_client.management.async_cluster import AsyncClusterClient

            self._cluster = AsyncClusterClient(self._get_http(), self._config)
        return self._cluster

    @property
    def compaction(self) -> Any:
        """Get the async compaction management client (enterprise)."""
        if self._compaction is None:
            from arc_client.management.async_compaction import AsyncCompactionClient

            self._compaction = AsyncCompactionClient(self._get_http(), self._config)
        return self._compaction

    @property
    def tiering(self) -> Any:
        """Get the async storage tiering client (enterprise)."""
        if self._tiering is None:
            from arc_client.management.async_tiering import AsyncTieringClient

            self._tiering = AsyncTieringClient(self._get_http(), self._config)
        return self._tiering

    @property
    def backup(self) -> Any:
        """Get the async backup/restore client (enterprise)."""
        if self._backup is None:
            from arc_client.management.async_backup import AsyncBackupClient

            self._backup = AsyncBackupClient(self._get_http(), self._config)
        return self._backup

    @property
    def mqtt(self) -> Any:
        """Get the async MQTT ingestion management client (enterprise)."""
        if self._mqtt is None:
            from arc_client.management.async_mqtt import AsyncMQTTClient

            self._mqtt = AsyncMQTTClient(self._get_http(), self._config)
        return self._mqtt

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
