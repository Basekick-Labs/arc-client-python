"""Synchronous continuous query client for Arc."""

from __future__ import annotations

from datetime import datetime
from typing import Any, List, Optional, Union

from arc_client._sentinels import CLEAR, ClearType
from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcNotFoundError, ArcValidationError
from arc_client.http.sync_http import SyncHTTPClient
from arc_client.models.continuous_query import (
    ContinuousQuery,
    CQExecution,
    ExecuteCQResponse,
)


class ContinuousQueryClient:
    """Synchronous client for continuous query management."""

    def __init__(self, http: SyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    def create(
        self,
        name: str,
        database: str,
        source_measurement: str,
        destination_measurement: str,
        query: str,
        interval: str,
        description: Optional[str] = None,
        retention_days: Optional[int] = None,
        delete_source_after_days: Optional[int] = None,
        is_active: bool = True,
        *,
        tag_columns: Optional[List[str]] = None,
    ) -> ContinuousQuery:
        """Create a new continuous query."""
        payload = {
            "name": name,
            "database": database,
            "source_measurement": source_measurement,
            "destination_measurement": destination_measurement,
            "query": query,
            "interval": interval,
            "description": description,
            "retention_days": retention_days,
            "delete_source_after_days": delete_source_after_days,
            "is_active": is_active,
            "tag_columns": tag_columns,
        }
        try:
            response = self._http.post("/api/v1/continuous_queries", json=payload)
            data = response.json()
            if not data.get("success", True):
                raise ArcError(data.get("error", "Failed to create CQ"))
            return ContinuousQuery.from_response(data)
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to create continuous query: {e}") from e

    def list(
        self, database: Optional[str] = None, is_active: Optional[bool] = None
    ) -> List[ContinuousQuery]:
        """List all continuous queries."""
        try:
            params: dict[str, Any] = {}
            if database:
                params["database"] = database
            if is_active is not None:
                params["is_active"] = str(is_active).lower()

            response = self._http.get("/api/v1/continuous_queries", params=params)
            data = response.json()
            if data is None:
                queries = []
            elif isinstance(data, list):
                queries = data
            elif isinstance(data, dict) and "queries" in data:
                queries = data["queries"]
                if queries is None:
                    queries = []
            else:
                raise ArcError("Malformed continuous query list: expected an array")
            if not isinstance(queries, list):
                raise ArcError("Malformed continuous query list: expected an array")
            return [ContinuousQuery.from_response(q) for q in queries]
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to list continuous queries: {e}") from e

    def get(self, query_id: int) -> ContinuousQuery:
        """Get continuous query details."""
        try:
            response = self._http.get(f"/api/v1/continuous_queries/{query_id}")
            data = response.json()
            if not data.get("success", True):
                error = data.get("error", "Unknown error")
                if "not found" in error.lower():
                    raise ArcNotFoundError(f"CQ {query_id} not found")
                raise ArcError(error)
            return ContinuousQuery.from_response(data)
        except (ArcNotFoundError, ArcError):
            raise
        except Exception as e:
            raise ArcError(f"Failed to get continuous query: {e}") from e

    def update(
        self,
        query_id: int,
        name: Optional[str] = None,
        description: Union[str, ClearType, None] = None,
        query: Optional[str] = None,
        interval: Optional[str] = None,
        retention_days: Union[int, ClearType, None] = None,
        delete_source_after_days: Union[int, ClearType, None] = None,
        is_active: Optional[bool] = None,
        *,
        database: Optional[str] = None,
        source_measurement: Optional[str] = None,
        destination_measurement: Optional[str] = None,
        tag_columns: Union[List[str], ClearType, None] = None,
    ) -> ContinuousQuery:
        """Send a partial update without reading or replacing unrelated fields.

        None omits an argument, preserving existing caller behavior. CLEAR
        sends JSON null for nullable fields. Servers with full-replacement
        PUT semantics (including Arc 26.09.3) require replace() instead.
        """
        values = {
            "name": name,
            "description": description,
            "query": query,
            "interval": interval,
            "retention_days": retention_days,
            "delete_source_after_days": delete_source_after_days,
            "is_active": is_active,
            "database": database,
            "source_measurement": source_measurement,
            "destination_measurement": destination_measurement,
            "tag_columns": tag_columns,
        }
        nullable = {"description", "tag_columns", "retention_days", "delete_source_after_days"}
        payload: dict[str, Any] = {}
        for key, value in values.items():
            if value is CLEAR:
                if key not in nullable:
                    raise ArcValidationError(f"{key} may not be cleared")
                payload[key] = None
            elif value is not None:
                payload[key] = value
        return self._put_definition(query_id, payload)

    def replace(
        self,
        query_id: int,
        *,
        name: str,
        database: str,
        source_measurement: str,
        destination_measurement: str,
        query: str,
        interval: str,
        description: Optional[str],
        tag_columns: Optional[List[str]],
        retention_days: Optional[int],
        delete_source_after_days: Optional[int],
        is_active: bool,
    ) -> ContinuousQuery:
        """Replace all mutable fields with one PUT on old or new servers.

        Every field must be explicit. This operation intentionally overwrites
        the definition; it does not fetch or merge a potentially stale copy.
        """
        payload = {
            "name": name,
            "database": database,
            "source_measurement": source_measurement,
            "destination_measurement": destination_measurement,
            "query": query,
            "interval": interval,
            "description": description,
            "tag_columns": tag_columns,
            "retention_days": retention_days,
            "delete_source_after_days": delete_source_after_days,
            "is_active": is_active,
        }
        return self._put_definition(query_id, payload)

    def _put_definition(self, query_id: int, payload: dict[str, Any]) -> ContinuousQuery:
        try:
            response = self._http.put(f"/api/v1/continuous_queries/{query_id}", json=payload)
            data = response.json()
            if not data.get("success", True):
                error = data.get("error", "Unknown error")
                if "not found" in error.lower():
                    raise ArcNotFoundError(f"CQ {query_id} not found")
                raise ArcError(error)
            return ContinuousQuery.from_response(data)
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to update continuous query: {e}") from e

    def delete(self, query_id: int) -> None:
        """Delete a continuous query."""
        try:
            response = self._http.delete(f"/api/v1/continuous_queries/{query_id}")
            data = response.json()
            if not data.get("success", True):
                error = data.get("error", "Unknown error")
                if "not found" in error.lower():
                    raise ArcNotFoundError(f"CQ {query_id} not found")
                raise ArcError(error)
        except (ArcNotFoundError, ArcError):
            raise
        except Exception as e:
            raise ArcError(f"Failed to delete continuous query: {e}") from e

    def execute(
        self,
        query_id: int,
        start_time: Union[datetime, str, None] = None,
        end_time: Union[datetime, str, None] = None,
        dry_run: bool = False,
    ) -> ExecuteCQResponse:
        """Execute a continuous query manually."""
        payload: dict[str, Any] = {"dry_run": dry_run}
        if start_time:
            payload["start_time"] = (
                start_time.isoformat() if isinstance(start_time, datetime) else start_time
            )
        if end_time:
            payload["end_time"] = (
                end_time.isoformat() if isinstance(end_time, datetime) else end_time
            )

        try:
            response = self._http.post(
                f"/api/v1/continuous_queries/{query_id}/execute", json=payload
            )
            data = response.json()
            return ExecuteCQResponse(**data)
        except Exception as e:
            raise ArcError(f"Failed to execute continuous query: {e}") from e

    def get_executions(self, query_id: int, limit: int = 50) -> List[CQExecution]:
        """Get execution history for a query."""
        try:
            response = self._http.get(
                f"/api/v1/continuous_queries/{query_id}/executions",
                params={"limit": limit},
            )
            data = response.json()
            executions = data.get("executions", [])
            if executions is None:
                executions = []
            if not isinstance(executions, list):
                raise ArcError("Malformed CQ history: expected an array")
            return [CQExecution(**e) for e in executions]
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to get CQ executions: {e}") from e
