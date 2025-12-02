"""Common models used across the SDK."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class HealthResponse(BaseModel):
    """Response from the health check endpoint."""

    status: str
    version: Optional[str] = None
    uptime: Optional[str] = None


class ReadyResponse(BaseModel):
    """Response from the readiness check endpoint."""

    ready: bool
    message: Optional[str] = None
