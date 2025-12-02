"""Query-related models."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class QueryResponse(BaseModel):
    """Response from a SQL query execution.

    Attributes:
        success: Whether the query executed successfully.
        columns: List of column names in the result.
        data: List of rows, where each row is a list of values.
        row_count: Number of rows returned.
        execution_time_ms: Query execution time in milliseconds.
        timestamp: Server timestamp when the query was executed.
        error: Error message if the query failed.
    """

    success: bool
    columns: list[str] = Field(default_factory=list)
    data: list[list[Any]] = Field(default_factory=list)
    row_count: int = 0
    execution_time_ms: float = 0.0
    timestamp: str | None = None
    error: str | None = None


class EstimateResponse(BaseModel):
    """Response from query estimation.

    Attributes:
        success: Whether the estimation succeeded.
        estimated_rows: Estimated number of rows the query will return.
        warning_level: Warning level (none, low, medium, high, error).
        warning_message: Human-readable warning message.
        execution_time_ms: Estimation execution time in milliseconds.
        error: Error message if estimation failed.
    """

    success: bool
    estimated_rows: int | None = None
    warning_level: str = "none"
    warning_message: str | None = None
    execution_time_ms: float = 0.0
    error: str | None = None


class MeasurementInfo(BaseModel):
    """Information about a measurement (table).

    Attributes:
        database: Database name containing the measurement.
        measurement: Measurement (table) name.
        file_count: Number of Parquet files storing the data.
        total_size_mb: Total size of the measurement in megabytes.
        storage_path: Storage path pattern for the measurement.
    """

    database: str
    measurement: str
    file_count: int = 0
    total_size_mb: float = 0.0
    storage_path: str | None = None
