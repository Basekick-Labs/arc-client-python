"""Retention policy models."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


class RetentionPolicy(BaseModel):
    """A retention policy definition."""

    id: int
    name: str
    database: str
    measurement: Optional[str] = None
    retention_days: int
    buffer_days: int = 7
    is_active: bool = True
    last_execution_time: Optional[str] = None
    last_execution_status: Optional[str] = None
    last_deleted_count: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ExecuteRetentionResponse(BaseModel):
    """Response from executing a retention policy."""

    policy_id: int
    policy_name: str
    deleted_count: int = 0
    files_deleted: int = 0
    skipped_files: int = 0
    skipped_reason: str = ""
    cold_files_deleted: int = 0
    cold_rows_deleted: int = 0
    cold_files_skipped: int = 0
    cold_skipped_reason: str = ""
    execution_time_ms: float = 0.0
    dry_run: bool = False
    cutoff_date: Optional[str] = None
    affected_measurements: list[str] = Field(default_factory=list)
    error: Optional[str] = None

    @field_validator("affected_measurements", mode="before")
    @classmethod
    def normalize_measurements(cls, value: Any) -> Any:
        """Treat a nil Go slice as empty, preserving validation for other values."""
        return [] if value is None else value


class RetentionExecution(BaseModel):
    """A retention execution history record."""

    id: int
    policy_id: int
    execution_time: str
    status: str
    deleted_count: int = 0
    cutoff_date: Optional[str] = None
    execution_duration_ms: float = 0.0
    error_message: Optional[str] = None
