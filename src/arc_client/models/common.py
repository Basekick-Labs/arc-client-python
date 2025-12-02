"""Common models used across the SDK."""

from __future__ import annotations

from pydantic import BaseModel


class HealthResponse(BaseModel):
    """Response from the health check endpoint."""

    status: str
    version: str | None = None
    uptime: str | None = None


class ReadyResponse(BaseModel):
    """Response from the readiness check endpoint."""

    ready: bool
    message: str | None = None
