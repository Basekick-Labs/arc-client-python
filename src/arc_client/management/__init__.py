"""Management module for Arc client.

Retention, continuous queries, delete, databases, and the enterprise surface
(RBAC, governance, audit, query management, cluster, compaction, tiering,
backup, MQTT).
"""

from arc_client.management.async_audit import AsyncAuditClient
from arc_client.management.async_backup import AsyncBackupClient
from arc_client.management.async_cluster import AsyncClusterClient
from arc_client.management.async_compaction import AsyncCompactionClient
from arc_client.management.async_continuous_query import AsyncContinuousQueryClient
from arc_client.management.async_databases import AsyncDatabaseClient
from arc_client.management.async_delete import AsyncDeleteClient
from arc_client.management.async_governance import AsyncGovernanceClient
from arc_client.management.async_mqtt import AsyncMQTTClient
from arc_client.management.async_query_management import AsyncQueryManagementClient
from arc_client.management.async_rbac import AsyncRBACClient
from arc_client.management.async_retention import AsyncRetentionClient
from arc_client.management.async_tiering import AsyncTieringClient
from arc_client.management.audit import AuditClient
from arc_client.management.backup import BackupClient
from arc_client.management.cluster import ClusterClient
from arc_client.management.compaction import CompactionClient
from arc_client.management.continuous_query import ContinuousQueryClient
from arc_client.management.databases import DatabaseClient
from arc_client.management.delete import DeleteClient
from arc_client.management.governance import GovernanceClient
from arc_client.management.mqtt import MQTTClient
from arc_client.management.query_management import QueryManagementClient
from arc_client.management.rbac import RBACClient
from arc_client.management.retention import RetentionClient
from arc_client.management.tiering import TieringClient

__all__ = [
    # Core management
    "ContinuousQueryClient",
    "AsyncContinuousQueryClient",
    "DeleteClient",
    "AsyncDeleteClient",
    "RetentionClient",
    "AsyncRetentionClient",
    "DatabaseClient",
    "AsyncDatabaseClient",
    # Enterprise
    "RBACClient",
    "AsyncRBACClient",
    "GovernanceClient",
    "AsyncGovernanceClient",
    "AuditClient",
    "AsyncAuditClient",
    "QueryManagementClient",
    "AsyncQueryManagementClient",
    "ClusterClient",
    "AsyncClusterClient",
    "CompactionClient",
    "AsyncCompactionClient",
    "TieringClient",
    "AsyncTieringClient",
    "BackupClient",
    "AsyncBackupClient",
    "MQTTClient",
    "AsyncMQTTClient",
]
