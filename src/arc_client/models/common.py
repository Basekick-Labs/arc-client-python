"""Common models used across the SDK."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict


class StorageTierHealth(BaseModel):
    """Credential health of one storage tier, from ``/health`` ``storage``.

    Mirrors Arc's ``database.StorageTierStatus``. ``state`` is e.g. ``"ok"``,
    ``"fallback"``, ``"expired"`` or ``"unavailable"``.
    """

    model_config = ConfigDict(extra="allow")

    backend: Optional[str] = None
    credentials: Optional[str] = None
    state: Optional[str] = None
    expires_at: Optional[str] = None
    source: Optional[str] = None
    last_refresh: Optional[str] = None
    consecutive_errors: int = 0


class LicenseHealth(BaseModel):
    """License status, from ``/health`` ``license`` (Arc's ``license.LicenseHealth``)."""

    model_config = ConfigDict(extra="allow")

    tier: Optional[str] = None
    status: Optional[str] = None
    source: Optional[str] = None
    expires_at: Optional[str] = None
    days_remaining: int = 0
    site_license: bool = False


class HealthResponse(BaseModel):
    """Response from the ``/health`` liveness endpoint.

    ``status`` is ``"ok"`` whenever the process is alive; Arc keeps it ``"ok"``
    even with expired storage credentials (that is ``/ready``'s job), so check
    ``storage`` for credential state.

    Attributes:
        status: Always ``"ok"`` from a live server.
        time: Server time (RFC 3339).
        uptime: Uptime as a Go duration string, e.g. ``"1h2m3.5s"``.
        uptime_sec: Uptime in seconds.
        storage: Per-tier credential health (``"hot"``, ``"cold"``), when
            the server reports any.
        license: License status, when the server has a license source wired.
        version: Not sent by Arc's ``/health``; kept for backward
            compatibility and always ``None`` against current servers.
    """

    model_config = ConfigDict(extra="allow")

    status: str
    time: Optional[str] = None
    uptime: Optional[str] = None
    uptime_sec: Optional[float] = None
    storage: Optional[dict[str, StorageTierHealth]] = None
    license: Optional[LicenseHealth] = None
    version: Optional[str] = None


class ReadyResponse(BaseModel):
    """Response from ``/ready`` or ``/ready/write``.

    Attributes:
        ready: True when the endpoint answered 200.
        status: ``"ready"``, ``"not_ready"`` or (``/ready/write`` only)
            ``"not_write_target"``.
        reason: Why the node is not ready, e.g.
            ``"storage_credentials_expired"`` or the startup/shutdown message.
        tier: The storage tier a credential problem is on (``"hot"``/``"cold"``).
        time: Server time (RFC 3339).
        uptime_sec: Uptime in seconds (``/ready`` when ready).
        message: Alias of ``reason``, kept for backward compatibility.
    """

    model_config = ConfigDict(extra="allow")

    ready: bool
    status: Optional[str] = None
    reason: Optional[str] = None
    tier: Optional[str] = None
    time: Optional[str] = None
    uptime_sec: Optional[float] = None
    message: Optional[str] = None
