"""Explicit clearing for nullable fields in partial updates."""

from enum import Enum


class ClearType(Enum):
    """Marker for an explicit JSON null, distinct from an omitted argument."""

    CLEAR = "CLEAR"


CLEAR = ClearType.CLEAR
