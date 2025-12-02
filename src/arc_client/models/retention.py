"""Retention policy models."""

from __future__ import annotations

from pydantic import BaseModel, Field


class RetentionPolicy(BaseModel):
    """A retention policy definition."""

    id: int
    name: str
    database: str
    measurement: str | None = None
    retention_days: int
    buffer_days: int = 7
    is_active: bool = True
    last_execution_time: str | None = None
    last_execution_status: str | None = None
    last_deleted_count: int | None = None
    created_at: str | None = None
    updated_at: str | None = None


class ExecuteRetentionResponse(BaseModel):
    """Response from executing a retention policy."""

    policy_id: int
    policy_name: str
    deleted_count: int = 0
    files_deleted: int = 0
    execution_time_ms: float = 0.0
    dry_run: bool = False
    cutoff_date: str | None = None
    affected_measurements: list[str] = Field(default_factory=list)
    error: str | None = None


class RetentionExecution(BaseModel):
    """A retention execution history record."""

    id: int
    policy_id: int
    execution_time: str
    status: str
    deleted_count: int = 0
    cutoff_date: str | None = None
    execution_duration_ms: float = 0.0
    error_message: str | None = None
