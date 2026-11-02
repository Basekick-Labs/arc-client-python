"""Query-related models."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class QueryProfile(BaseModel):
    """Server-side timing for one query, returned when ``profile=True``.

    Mirrors Arc's ``database.QueryProfile``. Extra keys a newer server adds
    are kept rather than dropped.

    Attributes:
        total_ms: Total server-side time in milliseconds.
        planner_ms: Planning time in milliseconds.
        execution_ms: Execution time in milliseconds.
        rows_scanned: Rows DuckDB scanned.
        latency_ms: Latency DuckDB itself reported, in milliseconds.
    """

    model_config = ConfigDict(extra="allow")

    total_ms: float = 0.0
    planner_ms: float = 0.0
    execution_ms: float = 0.0
    rows_scanned: int = 0
    latency_ms: float = 0.0


class QueryResponse(BaseModel):
    """Response from a SQL query execution.

    Attributes:
        success: Whether the query executed successfully.
        columns: List of column names in the result.
        data: List of rows, where each row is a list of values.
        row_count: Number of rows returned.
        truncated: Whether the result was cut short by a query failure. Arc's
            JSON writer sets it when a stream fails after its headers were
            sent. Arc's MessagePack writer never sends it: a msgpack stream
            that fails part way arrives as an incomplete body, which
            ``query_msgpack`` raises as :class:`ArcQueryError` instead of
            returning partial rows.
        truncation_reason: Reason the result was truncated, if available.
        rows_capped: Whether the result reached a configured row cap.
        row_cap: Configured row cap, if available.
        execution_time_ms: Query execution time in milliseconds.
        timestamp: Server timestamp when the query was executed.
        error: Error message if the query failed.
        profile: Server-side timing, present when the query asked for it with
            ``profile=True``.
        query_id: Arc's ID for this query (``X-Arc-Query-ID``), usable with
            ``client.queries`` to look it up or cancel it. ``None`` when the
            server does not run the query registry.
    """

    success: bool
    columns: list[str] = Field(default_factory=list)
    data: list[list[Any]] = Field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    truncation_reason: Optional[str] = None
    rows_capped: bool = False
    row_cap: Optional[int] = None
    execution_time_ms: float = 0.0
    timestamp: Optional[str] = None
    error: Optional[str] = None
    # Parallel to ``columns``: Arrow type name per column, when supplied.
    types: list[str] = Field(default_factory=list)
    profile: Optional[QueryProfile] = None
    query_id: Optional[str] = None


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
    estimated_rows: Optional[int] = None
    warning_level: str = "none"
    warning_message: Optional[str] = None
    execution_time_ms: float = 0.0
    error: Optional[str] = None


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
    storage_path: Optional[str] = None
