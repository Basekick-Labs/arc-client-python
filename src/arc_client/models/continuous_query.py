"""Continuous query models."""

from __future__ import annotations

from pydantic import BaseModel


class ContinuousQuery(BaseModel):
    """A continuous query definition."""

    id: int
    name: str
    description: str | None = None
    database: str
    source_measurement: str
    destination_measurement: str
    query: str
    interval: str
    retention_days: int | None = None
    delete_source_after_days: int | None = None
    is_active: bool = True
    last_execution_time: str | None = None
    last_execution_status: str | None = None
    last_processed_time: str | None = None
    last_records_written: int | None = None
    created_at: str | None = None
    updated_at: str | None = None


class ExecuteCQResponse(BaseModel):
    """Response from executing a continuous query."""

    query_id: int
    query_name: str
    execution_id: str
    status: str
    start_time: str
    end_time: str
    records_read: int | None = None
    records_written: int = 0
    execution_time_seconds: float = 0.0
    destination_measurement: str
    dry_run: bool = False
    executed_at: str | None = None
    executed_query: str | None = None
    error: str | None = None


class CQExecution(BaseModel):
    """A continuous query execution history record."""

    id: int
    query_id: int
    execution_id: str
    execution_time: str
    status: str
    start_time: str
    end_time: str
    records_read: int | None = None
    records_written: int = 0
    execution_duration_seconds: float = 0.0
    error_message: str | None = None
