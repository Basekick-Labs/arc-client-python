"""Models for the databases management endpoints."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class DatabaseInfo(BaseModel):
    """Information about a database.

    Attributes:
        name: Database name.
        measurement_count: Number of measurements in the database.
        created_at: Creation timestamp (RFC3339), when reported by the server.
    """

    model_config = ConfigDict(extra="allow")

    name: str
    measurement_count: int = 0
    created_at: Optional[str] = None


class DatabaseListResponse(BaseModel):
    """Response from ``GET /api/v1/databases``."""

    model_config = ConfigDict(extra="allow")

    databases: list[DatabaseInfo] = Field(default_factory=list)
    count: int = 0


class DatabaseMeasurement(BaseModel):
    """A measurement within a database."""

    model_config = ConfigDict(extra="allow")

    name: str
    file_count: int = 0


class MeasurementListResponse(BaseModel):
    """Response from ``GET /api/v1/databases/:name/measurements``."""

    model_config = ConfigDict(extra="allow")

    database: str
    measurements: list[DatabaseMeasurement] = Field(default_factory=list)
    count: int = 0


class SchemaField(BaseModel):
    """One registered field of a measurement: its name and DuckDB type."""

    model_config = ConfigDict(extra="allow")

    name: str
    type: str


class MeasurementSchema(BaseModel):
    """Response from ``GET /api/v1/databases/:db/measurements/:m/schema``.

    The measurement's registered field schema (Arc's stable-schema registry,
    ``query.stable_schema``): the columns and types every query binds for it,
    regardless of the time range read.
    """

    model_config = ConfigDict(extra="allow")

    database: str
    measurement: str
    fields: list[SchemaField] = Field(default_factory=list)


class SchemaRebuildResponse(BaseModel):
    """Response from ``POST .../schema/rebuild`` (HTTP 202): the rebuild was queued."""

    model_config = ConfigDict(extra="allow")

    status: str
    database: str
    measurement: str
