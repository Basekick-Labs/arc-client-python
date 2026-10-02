"""Continuous query models."""

from __future__ import annotations

from typing import Any, List, Optional

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
    tag_columns: Optional[List[str]] = None
    retention_days: Optional[int] = None
    delete_source_after_days: Optional[int] = None
    is_active: bool = True
    last_execution_time: Optional[str] = None
    last_execution_status: Optional[str] = None
    last_processed_time: Optional[str] = None
    last_records_written: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @classmethod
    def from_response(cls, data: dict[str, Any]) -> ContinuousQuery:
        """Build a definition from a create, get or update response body.

        Arc returns the definition flat, and the flat body has a ``query`` key
        holding SQL -- so unwrapping ``data["query"]`` unconditionally passes a
        string where a mapping belongs and every one of those three calls fails
        with a TypeError. Only treat ``query`` as the envelope when it actually
        carries a mapping.
        """
        nested = data.get("query")
        if isinstance(nested, dict):
            return cls(**nested)
        return cls(**data)


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
