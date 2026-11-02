"""Helpers shared by the sync and async write clients."""

from __future__ import annotations

import re
from typing import Any, Optional

from arc_client.exceptions import (
    ArcConnectionError,
    ArcError,
    ArcIngestionError,
    ArcPartialWriteError,
    ArcServerError,
    ArcValidationError,
)

# Timestamp precisions Arc's line-protocol handler accepts; anything else is a
# 400 (internal/api/lineprotocol.go handleWrite). Arc's default is "ns".
LP_PRECISIONS = ("ns", "us", "ms", "s")

# Arc's answer when it dropped some points and (maybe) wrote the rest
# (internal/api/partial_write.go partialWriteMessage):
#   "partial write: N points dropped: <reasons>[ the other points were written]"
_PARTIAL_WRITE_RE = re.compile(r"^\s*partial write:\s*(\d+)\s+points? dropped", re.IGNORECASE)
_OTHERS_WRITTEN = "the other points were written"


def validate_precision(precision: Optional[str]) -> Optional[str]:
    """Return ``precision`` if Arc accepts it, raising ArcValidationError otherwise."""
    if precision is None:
        return None
    if precision not in LP_PRECISIONS:
        raise ArcValidationError(
            f"Invalid precision {precision!r}: must be one of {', '.join(LP_PRECISIONS)}"
        )
    return precision


def partial_write_error(error: ArcServerError) -> Optional[ArcPartialWriteError]:
    """Recognize Arc's ``400 "partial write"`` answer, else return None."""
    if error.status_code != 400:
        return None
    body = error.response_body or {}
    message = body.get("error") if isinstance(body.get("error"), str) else None
    if message is None:
        return None
    match = _PARTIAL_WRITE_RE.match(message)
    if match is None:
        return None
    others_written = _OTHERS_WRITTEN in message.lower()
    return ArcPartialWriteError(
        f"Arc dropped {match.group(1)} point(s) of this write"
        + (" and wrote the others" if others_written else "; nothing was written")
        + f": {message}",
        dropped=int(match.group(1)),
        others_written=others_written,
    )


def as_write_error(error: Exception) -> Exception:
    """Map an exception from a write request to the one the caller should see.

    * ArcIngestionError (including ArcIngestRefusedError) passes unchanged.
    * A ``400 "partial write"`` becomes ArcPartialWriteError.
    * Other generic HTTP failures (ArcServerError) and transport failures
      (ArcConnectionError) become ArcIngestionError, as before, with the
      original as ``__cause__`` and the HTTP status on ``status_code``.
    * Every other typed ArcError (authentication, permission, scoped access,
      license, rate limit, not found, not supported) passes unchanged, so
      callers can tell "bad token" or "slow down" from "the data was rejected".
    * Anything else becomes ArcIngestionError.
    """
    if isinstance(error, ArcIngestionError):
        return error
    if isinstance(error, ArcServerError):
        partial = partial_write_error(error)
        if partial is not None:
            return partial
        return ArcIngestionError(f"Failed to write data: {error}", status_code=error.status_code)
    if isinstance(error, ArcConnectionError):
        return ArcIngestionError(f"Failed to write data: {error}")
    if isinstance(error, ArcError):
        return error
    return ArcIngestionError(f"Failed to write data: {error}")


def tle_stats_body(body: Any) -> dict[str, Any]:
    """Unwrap Arc's TLE stats envelope."""
    if not isinstance(body, dict):
        return {}
    stats = body.get("stats", body)
    return stats if isinstance(stats, dict) else {}
