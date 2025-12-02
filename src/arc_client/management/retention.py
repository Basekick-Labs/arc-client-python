"""Synchronous retention policy client for Arc."""

from __future__ import annotations

from typing import Any, List, Optional

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcNotFoundError
from arc_client.http.sync_http import SyncHTTPClient
from arc_client.models.retention import (
    ExecuteRetentionResponse,
    RetentionExecution,
    RetentionPolicy,
)


class RetentionClient:
    """Synchronous client for retention policy management."""

    def __init__(self, http: SyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    def create(
        self,
        name: str,
        database: str,
        retention_days: int,
        measurement: Optional[str] = None,
        buffer_days: int = 7,
        is_active: bool = True,
    ) -> RetentionPolicy:
        """Create a new retention policy."""
        payload = {
            "name": name,
            "database": database,
            "retention_days": retention_days,
            "measurement": measurement,
            "buffer_days": buffer_days,
            "is_active": is_active,
        }
        try:
            response = self._http.post("/api/v1/retention", json=payload)
            data = response.json()
            if not data.get("success", True):
                raise ArcError(data.get("error", "Failed to create policy"))
            return RetentionPolicy(**data.get("policy", data))
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to create retention policy: {e}") from e

    def list(self) -> List[RetentionPolicy]:
        """List all retention policies."""
        try:
            response = self._http.get("/api/v1/retention")
            data = response.json()
            policies = data.get("policies", [])
            return [RetentionPolicy(**p) for p in policies]
        except Exception as e:
            raise ArcError(f"Failed to list retention policies: {e}") from e

    def get(self, policy_id: int) -> RetentionPolicy:
        """Get retention policy details."""
        try:
            response = self._http.get(f"/api/v1/retention/{policy_id}")
            data = response.json()
            if not data.get("success", True):
                error = data.get("error", "Unknown error")
                if "not found" in error.lower():
                    raise ArcNotFoundError(f"Policy {policy_id} not found")
                raise ArcError(error)
            return RetentionPolicy(**data.get("policy", data))
        except (ArcNotFoundError, ArcError):
            raise
        except Exception as e:
            raise ArcError(f"Failed to get retention policy: {e}") from e

    def update(
        self,
        policy_id: int,
        name: Optional[str] = None,
        retention_days: Optional[int] = None,
        measurement: Optional[str] = None,
        buffer_days: Optional[int] = None,
        is_active: Optional[bool] = None,
    ) -> RetentionPolicy:
        """Update a retention policy."""
        payload: dict[str, Any] = {}
        if name is not None:
            payload["name"] = name
        if retention_days is not None:
            payload["retention_days"] = retention_days
        if measurement is not None:
            payload["measurement"] = measurement
        if buffer_days is not None:
            payload["buffer_days"] = buffer_days
        if is_active is not None:
            payload["is_active"] = is_active

        try:
            response = self._http.put(f"/api/v1/retention/{policy_id}", json=payload)
            data = response.json()
            if not data.get("success", True):
                error = data.get("error", "Unknown error")
                if "not found" in error.lower():
                    raise ArcNotFoundError(f"Policy {policy_id} not found")
                raise ArcError(error)
            return RetentionPolicy(**data.get("policy", data))
        except (ArcNotFoundError, ArcError):
            raise
        except Exception as e:
            raise ArcError(f"Failed to update retention policy: {e}") from e

    def delete(self, policy_id: int) -> None:
        """Delete a retention policy."""
        try:
            response = self._http.delete(f"/api/v1/retention/{policy_id}")
            data = response.json()
            if not data.get("success", True):
                error = data.get("error", "Unknown error")
                if "not found" in error.lower():
                    raise ArcNotFoundError(f"Policy {policy_id} not found")
                raise ArcError(error)
        except (ArcNotFoundError, ArcError):
            raise
        except Exception as e:
            raise ArcError(f"Failed to delete retention policy: {e}") from e

    def execute(
        self, policy_id: int, dry_run: bool = True, confirm: bool = False
    ) -> ExecuteRetentionResponse:
        """Execute a retention policy."""
        payload = {"dry_run": dry_run, "confirm": confirm}
        try:
            response = self._http.post(f"/api/v1/retention/{policy_id}/execute", json=payload)
            data = response.json()
            return ExecuteRetentionResponse(**data)
        except Exception as e:
            raise ArcError(f"Failed to execute retention policy: {e}") from e

    def get_executions(self, policy_id: int, limit: int = 50) -> List[RetentionExecution]:
        """Get execution history for a policy."""
        try:
            response = self._http.get(
                f"/api/v1/retention/{policy_id}/executions",
                params={"limit": limit},
            )
            data = response.json()
            executions = data.get("executions", [])
            return [RetentionExecution(**e) for e in executions]
        except Exception as e:
            raise ArcError(f"Failed to get retention executions: {e}") from e
