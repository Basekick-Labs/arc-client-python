"""Shared client-side validation helpers.

Mirrors Arc's server-side identifier rules so invalid names fail fast with a
clear error (and never reach a SQL string via interpolation).
"""

from __future__ import annotations

import re
from urllib.parse import quote

from arc_client.exceptions import ArcValidationError

# Matches Arc's server-side rule: start with a letter, then letters/digits/
# underscore/hyphen, max 64 characters total.
_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")


def is_valid_name(name: str) -> bool:
    """Return True if ``name`` is a valid Arc database/measurement identifier."""
    return bool(_NAME_RE.match(name))


def validate_name(name: str, kind: str = "name") -> str:
    """Validate a database/measurement identifier, returning it unchanged.

    Args:
        name: The identifier to validate.
        kind: A short label used in the error message (e.g. "database").

    Returns:
        The validated ``name``.

    Raises:
        ArcValidationError: If ``name`` is not a valid identifier.
    """
    if not isinstance(name, str) or not is_valid_name(name):
        raise ArcValidationError(
            f"Invalid {kind} {name!r}: must start with a letter and contain only "
            "letters, digits, underscores, or hyphens (max 64 characters)."
        )
    return name


def path_segment(name: str, kind: str = "name") -> str:
    """Validate a name used as one URL path segment and percent-encode it.

    Looser than :func:`validate_name`, matching Arc's per-name storage-segment
    rule (internal/api/databases.go isSafeStoragePathSegment): routes such as
    the measurement schema must still resolve names an older release or a
    spoke pseudo-database uses. Rejects what can never be one segment: an empty
    name, a ``/`` or a leading ``.``.

    Raises:
        ArcValidationError: If ``name`` cannot be a single path segment.
    """
    if not isinstance(name, str) or not name or "/" in name or name.startswith("."):
        raise ArcValidationError(
            f"Invalid {kind} {name!r}: must be non-empty, contain no '/', and not start with '.'"
        )
    return quote(name, safe="")
