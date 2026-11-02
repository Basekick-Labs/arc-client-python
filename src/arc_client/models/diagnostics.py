"""Models for Arc readiness probes."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict


class ReadinessStatus(BaseModel):
    """The answer of ``/ready`` or ``/ready/write``.

    ``status`` is ``"ready"``, ``"not_ready"`` (the node is not ready for any
    traffic) or, from ``/ready/write`` only, ``"not_write_target"`` (the node
    is healthy but its cluster role does not take writes; route writes to a
    writer). ``reason`` and, for a storage-credential problem, ``tier``
    explain a not-ready answer. ``uptime_sec`` is sent by ``/ready`` only.
    """

    model_config = ConfigDict(extra="allow")

    status: str
    time: Optional[str] = None
    reason: Optional[str] = None
    tier: Optional[str] = None
    uptime_sec: Optional[float] = None

    @property
    def ready(self) -> bool:
        """True when the node should receive the traffic the probe asked about."""
        return self.status == "ready"
