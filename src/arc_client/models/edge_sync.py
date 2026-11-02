"""Models for edge-sync spoke administration (hub side)."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class Spoke(BaseModel):
    """A spoke registered with this hub. Never carries the secret."""

    model_config = ConfigDict(extra="allow")

    spoke_id: str
    name: str = ""
    enabled: bool = True
    files_received: int = 0
    bytes_received: int = 0
    registered_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None


class SpokeSecret(BaseModel):
    """A spoke's HMAC secret, returned once by register and rotate.

    Arc cannot show the secret again. Store it before discarding this object;
    a lost secret can only be replaced by rotating.
    """

    model_config = ConfigDict(extra="allow")

    spoke_id: str
    secret: str
    name: Optional[str] = None
    warning: Optional[str] = None
