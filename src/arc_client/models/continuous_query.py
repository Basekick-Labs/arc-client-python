"""Continuous query models."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class ContinuousQuery(BaseModel):
    """A continuous query definition."""

    id: int
    name: str
    description: Optional[str] = None
    database: str
    source_measurement: str
    destination_measurement: str
    query: str
    interval: str
    retention_days: Optional[int] = None
    delete_source_after_days: Optional[int] = None
    is_active: bool = True
    last_execution_time: Optional[str] = None
    last_execution_status: Optional[str] = None
    last_processed_time: Optional[str] = None
    last_records_written: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ExecuteCQResponse(BaseModel):
    """Response from executing a continuous query."""

    query_id: int
    query_name: str
    execution_id: str
    status: str
    start_time: str
    end_time: str
    records_read: Optional[int] = None
    records_written: int = 0
    execution_time_seconds: float = 0.0
    destination_measurement: str
    dry_run: bool = False
    executed_at: Optional[str] = None
    executed_query: Optional[str] = None
    error: Optional[str] = None


class CQExecution(BaseModel):
    """A continuous query execution history record."""

    id: int
    query_id: int
    execution_id: str
    execution_time: str
    status: str
    start_time: str
    end_time: str
    records_read: Optional[int] = None
    records_written: int = 0
    execution_duration_seconds: float = 0.0
    error_message: Optional[str] = None
