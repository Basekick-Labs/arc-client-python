"""Synchronous Arc client."""

from __future__ import annotations

from typing import Any, Optional

from arc_client.compression import Compression
from arc_client.config import ClientConfig
from arc_client.exceptions import ArcServerError
from arc_client.http.base import never_retry_status
from arc_client.http.sync_http import SyncHTTPClient
from arc_client.models.common import HealthResponse, ReadyResponse


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
        database: Optional[str] = None,
        timeout: float = 30.0,
        compression: Compression | str | bool = Compression.ZSTD,
        ssl: bool = False,
        verify_ssl: bool = True,
        max_retries: int = 3,
        retry_backoff: float = 0.5,
        send_installation_id: bool = True,
    ) -> None:
        """Initialize the Arc client.

        Args:
            host: Arc server hostname.
            port: Arc server port.
            token: API token for authentication.
            database: Default database. Writes use "default" when unset;
                queries send it only when set, so `db.table` names work
                without it (#35).
            timeout: Request timeout in seconds.
            compression: Write compression codec (none/gzip/zstd, or bool for
                back-compat). Defaults to zstd.
            ssl: Use HTTPS.
            verify_ssl: Verify SSL certificates.
            max_retries: Max automatic retries for transient failures
                (connection errors, 429, 503). 0 disables retries.
            retry_backoff: Base backoff (seconds) for exponential retry.
            send_installation_id: Send a random per-installation id so Arc's
                opt-out telemetry can count SDK installations. Nothing about
                requests or data is included. DO_NOT_TRACK=1 also disables it.
        """
        self._config = ClientConfig(
            host=host,
            port=port,
            token=token,
            timeout=timeout,
            compression=compression,
            ssl=ssl,
            verify_ssl=verify_ssl,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            send_installation_id=send_installation_id,
            # Only when given, so ClientConfig can tell a chosen database
            # from the default (ClientConfig.query_database).
            **({"database": database} if database is not None else {}),
        )
        self._http: Optional[SyncHTTPClient] = None

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
        self._edge_sync: Any = None
        self._diagnostics: Any = None
        self._reconciliation: Any = None
        self._schedulers: Any = None

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

    @property
    def import_(self) -> Any:
        """Get the bulk import client (CSV, Parquet, line protocol, TLE)."""
        if self._import is None:
            from arc_client.ingestion.importer import ImportClient

            self._import = ImportClient(self._get_http(), self._config)
        return self._import

    @property
    def databases(self) -> Any:
        """Get the databases management client."""
        if self._databases is None:
            from arc_client.management.databases import DatabaseClient

            self._databases = DatabaseClient(self._get_http(), self._config)
        return self._databases

    @property
    def rbac(self) -> Any:
        """Get the RBAC management client (enterprise)."""
        if self._rbac is None:
            from arc_client.management.rbac import RBACClient

            self._rbac = RBACClient(self._get_http(), self._config)
        return self._rbac

    @property
    def governance(self) -> Any:
        """Get the query governance client (enterprise)."""
        if self._governance is None:
            from arc_client.management.governance import GovernanceClient

            self._governance = GovernanceClient(self._get_http(), self._config)
        return self._governance

    @property
    def audit(self) -> Any:
        """Get the audit log client (enterprise)."""
        if self._audit is None:
            from arc_client.management.audit import AuditClient

            self._audit = AuditClient(self._get_http(), self._config)
        return self._audit

    @property
    def queries(self) -> Any:
        """Get the query management client (active/history/kill)."""
        if self._queries is None:
            from arc_client.management.query_management import QueryManagementClient

            self._queries = QueryManagementClient(self._get_http(), self._config)
        return self._queries

    @property
    def cluster(self) -> Any:
        """Get the cluster management client (enterprise)."""
        if self._cluster is None:
            from arc_client.management.cluster import ClusterClient

            self._cluster = ClusterClient(self._get_http(), self._config)
        return self._cluster

    @property
    def compaction(self) -> Any:
        """Get the compaction management client (enterprise)."""
        if self._compaction is None:
            from arc_client.management.compaction import CompactionClient

            self._compaction = CompactionClient(self._get_http(), self._config)
        return self._compaction

    @property
    def tiering(self) -> Any:
        """Get the storage tiering client (enterprise)."""
        if self._tiering is None:
            from arc_client.management.tiering import TieringClient

            self._tiering = TieringClient(self._get_http(), self._config)
        return self._tiering

    @property
    def backup(self) -> Any:
        """Get the backup/restore client (enterprise)."""
        if self._backup is None:
            from arc_client.management.backup import BackupClient

            self._backup = BackupClient(self._get_http(), self._config)
        return self._backup

    @property
    def schedulers(self) -> Any:
        """Get the scheduler inspection and control client."""
        if self._schedulers is None:
            from arc_client.management.schedulers import SchedulerClient

            self._schedulers = SchedulerClient(self._get_http(), self._config)
        return self._schedulers

    @property
    def mqtt(self) -> Any:
        """Get the MQTT ingestion management client (enterprise)."""
        if self._mqtt is None:
            from arc_client.management.mqtt import MQTTClient

            self._mqtt = MQTTClient(self._get_http(), self._config)
        return self._mqtt

    @property
    def reconciliation(self) -> Any:
        """Get the manifest/storage reconciliation client (enterprise)."""
        if self._reconciliation is None:
            from arc_client.management.reconciliation import ReconciliationClient

            self._reconciliation = ReconciliationClient(self._get_http(), self._config)
        return self._reconciliation

    @property
    def diagnostics(self) -> Any:
        """Get the operational diagnostics client (metrics, write protocols, logs, memory)."""
        if self._diagnostics is None:
            from arc_client.management.diagnostics import DiagnosticsClient

            self._diagnostics = DiagnosticsClient(self._get_http(), self._config)
        return self._diagnostics

    @property
    def edge_sync(self) -> Any:
        """Get the edge-sync administration client (hub spokes/import, spoke sync/export)."""
        if self._edge_sync is None:
            from arc_client.management.edge_sync import EdgeSyncClient

            self._edge_sync = EdgeSyncClient(self._get_http(), self._config)
        return self._edge_sync

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

        A single probe: a 503 "not ready" answer is not retried. Use
        :meth:`readiness` for the reason.

        Returns:
            True if server is ready, False otherwise (including when it cannot
            be reached).
        """
        try:
            return (self.readiness()).ready
        except Exception:
            return False

    def readiness(self) -> ReadyResponse:
        """Ask ``/ready`` whether this node should receive traffic, and why not.

        Arc answers 503 while starting up, replaying its WAL or shutting down,
        and (with ``server.storage_credentials_fail_ready``) while a storage
        tier's credentials are expired or unavailable; ``reason`` and ``tier``
        say which. The 503 is returned as ``ready=False``, not raised, and not
        retried.

        Raises:
            ArcConnectionError: If the server cannot be reached.
        """
        return self._readiness("/ready")

    def write_readiness(self) -> ReadyResponse:
        """Ask ``/ready/write`` whether writes should be routed to this node.

        Strictly narrower than :meth:`readiness`: a reader, or a writer that is
        not the primary in local-storage cluster mode, answers
        ``status="not_write_target"``.

        Raises:
            ArcConnectionError: If the server cannot be reached.
        """
        return self._readiness("/ready/write")

    def _readiness(self, path: str) -> ReadyResponse:
        try:
            response = self._get_http().get(path, retry_if=never_retry_status)
        except ArcServerError as error:
            if error.status_code != 503:
                raise
            return _ready_response(False, error.response_body)
        try:
            body = response.json()
        except ValueError:
            body = None
        return _ready_response(True, body)

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


def _ready_response(ready: bool, body: object) -> ReadyResponse:
    data = dict(body) if isinstance(body, dict) else {}
    data["ready"] = ready
    data.setdefault("message", data.get("reason"))
    return ReadyResponse.model_validate(data)
