"""Synchronous continuous query client for Arc."""

from __future__ import annotations

from datetime import datetime

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcNotFoundError
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
        description: str | None = None,
        retention_days: int | None = None,
        delete_source_after_days: int | None = None,
        is_active: bool = True,
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
        }
        try:
            response = self._http.post("/api/v1/continuous_queries", json=payload)
            data = response.json()
            if not data.get("success", True):
                raise ArcError(data.get("error", "Failed to create CQ"))
            return ContinuousQuery(**data.get("query", data))
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to create continuous query: {e}") from e

    def list(
        self, database: str | None = None, is_active: bool | None = None
    ) -> list[ContinuousQuery]:
        """List all continuous queries."""
        try:
            params = {}
            if database:
                params["database"] = database
            if is_active is not None:
                params["is_active"] = str(is_active).lower()

            response = self._http.get("/api/v1/continuous_queries", params=params)
            data = response.json()
            queries = data.get("queries", [])
            return [ContinuousQuery(**q) for q in queries]
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
            return ContinuousQuery(**data.get("query", data))
        except (ArcNotFoundError, ArcError):
            raise
        except Exception as e:
            raise ArcError(f"Failed to get continuous query: {e}") from e

    def update(
        self,
        query_id: int,
        name: str | None = None,
        description: str | None = None,
        query: str | None = None,
        interval: str | None = None,
        retention_days: int | None = None,
        delete_source_after_days: int | None = None,
        is_active: bool | None = None,
    ) -> ContinuousQuery:
        """Update a continuous query."""
        payload: dict = {}
        if name is not None:
            payload["name"] = name
        if description is not None:
            payload["description"] = description
        if query is not None:
            payload["query"] = query
        if interval is not None:
            payload["interval"] = interval
        if retention_days is not None:
            payload["retention_days"] = retention_days
        if delete_source_after_days is not None:
            payload["delete_source_after_days"] = delete_source_after_days
        if is_active is not None:
            payload["is_active"] = is_active

        try:
            response = self._http.put(
                f"/api/v1/continuous_queries/{query_id}", json=payload
            )
            data = response.json()
            if not data.get("success", True):
                error = data.get("error", "Unknown error")
                if "not found" in error.lower():
                    raise ArcNotFoundError(f"CQ {query_id} not found")
                raise ArcError(error)
            return ContinuousQuery(**data.get("query", data))
        except (ArcNotFoundError, ArcError):
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
        start_time: datetime | str | None = None,
        end_time: datetime | str | None = None,
        dry_run: bool = False,
    ) -> ExecuteCQResponse:
        """Execute a continuous query manually."""
        payload: dict = {"dry_run": dry_run}
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

    def get_executions(self, query_id: int, limit: int = 50) -> list[CQExecution]:
        """Get execution history for a query."""
        try:
            response = self._http.get(
                f"/api/v1/continuous_queries/{query_id}/executions",
                params={"limit": limit},
            )
            data = response.json()
            executions = data.get("executions", [])
            return [CQExecution(**e) for e in executions]
        except Exception as e:
            raise ArcError(f"Failed to get CQ executions: {e}") from e
