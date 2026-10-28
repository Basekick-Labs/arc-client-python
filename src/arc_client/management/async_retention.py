"""Asynchronous retention policy client for Arc."""

from __future__ import annotations

from typing import Any, List, Optional, Union

from arc_client._sentinels import CLEAR, ClearType
from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcNotFoundError, ArcValidationError
from arc_client.http.async_http import AsyncHTTPClient
from arc_client.models.retention import (
    ExecuteRetentionResponse,
    RetentionExecution,
    RetentionPolicy,
)


class AsyncRetentionClient:
    """Asynchronous client for retention policy management."""

    def __init__(self, http: AsyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    async def create(
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
            response = await self._http.post("/api/v1/retention", json=payload)
            data = response.json()
            if not data.get("success", True):
                raise ArcError(data.get("error", "Failed to create policy"))
            return RetentionPolicy(**data.get("policy", data))
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to create retention policy: {e}") from e

    async def list(self) -> List[RetentionPolicy]:
        """List all retention policies."""
        try:
            response = await self._http.get("/api/v1/retention")
            data = response.json()
            policies = data.get("policies", data) if isinstance(data, dict) else data
            if policies is None:
                policies = []
            if not isinstance(policies, list):
                raise ArcError("Malformed retention policy list: expected an array")
            return [RetentionPolicy(**p) for p in policies]
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to list retention policies: {e}") from e

    async def get(self, policy_id: int) -> RetentionPolicy:
        """Get retention policy details."""
        try:
            response = await self._http.get(f"/api/v1/retention/{policy_id}")
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

    async def update(
        self,
        policy_id: int,
        name: Optional[str] = None,
        retention_days: Optional[int] = None,
        measurement: Union[str, ClearType, None] = None,
        buffer_days: Optional[int] = None,
        is_active: Optional[bool] = None,
        *,
        database: Optional[str] = None,
    ) -> RetentionPolicy:
        """Send a partial update without reading unrelated fields.

        None omits an argument, preserving existing caller behavior. CLEAR
        sends JSON null for measurement. Servers with full-replacement PUT
        semantics (including Arc 26.09.3) require replace() instead.
        """
        values = {
            "name": name,
            "retention_days": retention_days,
            "measurement": measurement,
            "buffer_days": buffer_days,
            "is_active": is_active,
            "database": database,
        }
        payload: dict[str, Any] = {}
        for key, value in values.items():
            if value is CLEAR:
                if key != "measurement":
                    raise ArcValidationError(f"{key} may not be cleared")
                payload[key] = None
            elif value is not None:
                payload[key] = value
        return await self._put_definition(policy_id, payload)

    async def replace(
        self,
        policy_id: int,
        *,
        name: str,
        database: str,
        retention_days: int,
        measurement: Optional[str],
        buffer_days: int,
        is_active: bool,
    ) -> RetentionPolicy:
        """Replace all mutable fields with one PUT on old or new servers.

        Every field must be explicit. This operation intentionally overwrites
        the definition; it does not fetch or merge a potentially stale copy.
        """
        payload = {
            "name": name,
            "database": database,
            "retention_days": retention_days,
            "measurement": measurement,
            "buffer_days": buffer_days,
            "is_active": is_active,
        }
        return await self._put_definition(policy_id, payload)

    async def _put_definition(self, policy_id: int, payload: dict[str, Any]) -> RetentionPolicy:
        try:
            response = await self._http.put(f"/api/v1/retention/{policy_id}", json=payload)
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

    async def delete(self, policy_id: int) -> None:
        """Delete a retention policy."""
        try:
            response = await self._http.delete(f"/api/v1/retention/{policy_id}")
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

    async def execute(
        self, policy_id: int, dry_run: bool = True, confirm: bool = False
    ) -> ExecuteRetentionResponse:
        """Execute a retention policy."""
        payload = {"dry_run": dry_run, "confirm": confirm}
        try:
            response = await self._http.post(f"/api/v1/retention/{policy_id}/execute", json=payload)
            data = response.json()
            return ExecuteRetentionResponse(**data)
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to execute retention policy: {e}") from e

    async def get_executions(self, policy_id: int, limit: int = 50) -> List[RetentionExecution]:
        """Get execution history for a policy."""
        try:
            response = await self._http.get(
                f"/api/v1/retention/{policy_id}/executions",
                params={"limit": limit},
            )
            data = response.json()
            executions = data.get("executions", [])
            if executions is None:
                executions = []
            if not isinstance(executions, list):
                raise ArcError("Malformed retention execution history: expected an array")
            return [RetentionExecution(**e) for e in executions]
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Failed to get retention executions: {e}") from e
