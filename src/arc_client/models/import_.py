"""Models for bulk import endpoints.

These mirror the Arc server's import result structs. Extra fields are allowed so
newer server fields don't break older clients.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class ImportResult(BaseModel):
    """Result of a CSV or Parquet import.

    Attributes:
        database: Target database.
        measurement: Measurement (table) the rows were imported into.
        rows_imported: Number of rows ingested.
        partitions_created: Number of distinct hour partitions created.
        time_range_min: Earliest timestamp imported (RFC3339), if any.
        time_range_max: Latest timestamp imported (RFC3339), if any.
        columns: Column names in the imported data (time column normalized).
        duration_ms: Server-side import duration in milliseconds.
    """

    model_config = ConfigDict(extra="allow")

    database: str
    measurement: str
    rows_imported: int = 0
    partitions_created: int = 0
    time_range_min: Optional[str] = None
    time_range_max: Optional[str] = None
    columns: list[str] = Field(default_factory=list)
    duration_ms: int = 0


class LPImportResult(BaseModel):
    """Result of a line-protocol file import.

    Attributes:
        database: Target database.
        measurements: Measurements that received data.
        rows_imported: Number of rows ingested.
        precision: Timestamp precision used to parse the file.
        duration_ms: Server-side import duration in milliseconds.
    """

    model_config = ConfigDict(extra="allow")

    database: str
    measurements: list[str] = Field(default_factory=list)
    rows_imported: int = 0
    precision: str = "ns"
    duration_ms: int = 0


class TLEImportResult(BaseModel):
    """Result of a TLE (Two-Line Element) file import.

    Attributes:
        database: Target database.
        measurement: Measurement the satellites were imported into.
        satellite_count: Number of distinct satellites parsed.
        rows_imported: Number of rows ingested.
        parse_warnings: Non-fatal parse warnings, if any.
        duration_ms: Server-side import duration in milliseconds.
    """

    model_config = ConfigDict(extra="allow")

    database: str
    measurement: str
    satellite_count: int = 0
    rows_imported: int = 0
    parse_warnings: list[str] = Field(default_factory=list)
    duration_ms: int = 0


class ImportStats(BaseModel):
    """Cumulative import counters from ``GET /api/v1/import/stats``.

    Attributes:
        total_requests: Total import requests handled.
        total_records: Total records ingested across imports.
        total_errors: Total import errors.
    """

    model_config = ConfigDict(extra="allow")

    total_requests: int = 0
    total_records: int = 0
    total_errors: int = 0
