"""Client identity sent to Arc: the User-Agent and an installation id.

Every request carries ``User-Agent: arc-client-python/<version>
(python/<version>; <os>/<arch>)``. Unless the caller opts out, it also
carries ``Arc-Client-Installation-Id``: a random UUID minted once per
installation. Arc includes the distinct ids it served in its own opt-out
telemetry so Basekick can count SDK installations across Arc servers. The
SDK itself never contacts anything but the Arc server it is configured for,
and the id is not derived from the machine, the user, or the token.

Opt out with ``DO_NOT_TRACK=1``, ``ARC_CLIENT_SEND_INSTALLATION_ID=0``, or
``send_installation_id=False`` on the client.

The id lives in ``$XDG_CONFIG_HOME/arc-client-python/installation-id``
(``~/.config/...``; ``%APPDATA%`` on Windows). ``ARC_CLIENT_INSTALLATION_ID``
overrides it, which suits containers that would otherwise mint a new id per
start. Failing to read or write the file never fails a request: the process
falls back to an id held in memory.
"""

from __future__ import annotations

import contextlib
import os
import platform
import re
import sys
import tempfile
import threading
import uuid
from pathlib import Path
from typing import Optional

from arc_client import __version__

HEADER_INSTALLATION_ID = "Arc-Client-Installation-Id"

ENV_INSTALLATION_ID = "ARC_CLIENT_INSTALLATION_ID"
ENV_SEND_INSTALLATION_ID = "ARC_CLIENT_SEND_INSTALLATION_ID"

_FILE_NAME = "installation-id"
_DIR_NAME = "arc-client-python"

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_TRUTHY = {"1", "true", "yes", "on"}
_FALSY = {"0", "false", "no", "off"}

# Go's GOOS/GOARCH spelling, so SDK and arcli platforms read the same.
_ARCH = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}

_lock = threading.Lock()
_cached_id: Optional[str] = None


def _os_name() -> str:
    if sys.platform.startswith("win"):
        return "windows"
    if sys.platform.startswith("linux"):
        return "linux"
    return sys.platform


def _arch() -> str:
    machine = platform.machine().lower()
    return _ARCH.get(machine, machine or "unknown")


def user_agent() -> str:
    """``arc-client-python/0.2.0 (python/3.12.4; linux/amd64)``."""
    runtime = f"python/{platform.python_version()}"
    return f"arc-client-python/{__version__} ({runtime}; {_os_name()}/{_arch()})"


def _env_flag(name: str) -> Optional[bool]:
    value = os.environ.get(name, "").strip().lower()
    if value in _TRUTHY:
        return True
    if value in _FALSY:
        return False
    return None


def sends_installation_id(enabled: bool = True) -> bool:
    """Whether the id goes on requests: the client flag and both env opt-outs."""
    if not enabled or _env_flag("DO_NOT_TRACK") is True:
        return False
    return _env_flag(ENV_SEND_INSTALLATION_ID) is not False


def _config_dir() -> Path:
    if sys.platform.startswith("win"):
        base = os.environ.get("APPDATA")
        if base:
            return Path(base) / _DIR_NAME
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base_path = Path(xdg) if xdg else Path.home() / ".config"
    return base_path / _DIR_NAME


def _valid(value: str) -> Optional[str]:
    value = value.strip().lower()
    return value if _UUID_RE.match(value) else None


def _load_or_create() -> str:
    path = _config_dir() / _FILE_NAME
    try:
        existing = _valid(path.read_text(encoding="ascii"))
        if existing:
            return existing
    except (OSError, ValueError):
        pass
    new_id = str(uuid.uuid4())
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Write-then-rename so concurrent first runs never leave a torn file;
        # the last writer wins and later reads agree with it.
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".installation-id.")
        try:
            with os.fdopen(fd, "w", encoding="ascii") as f:
                f.write(new_id + "\n")
            os.replace(tmp, path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise
        # Re-read: if another process won the rename, use its id.
        return _valid(path.read_text(encoding="ascii")) or new_id
    except (OSError, ValueError):
        return new_id


def installation_id() -> str:
    """The installation id, minted and persisted on first use."""
    override = _valid(os.environ.get(ENV_INSTALLATION_ID, ""))
    if override:
        return override
    global _cached_id
    with _lock:
        if _cached_id is None:
            _cached_id = _load_or_create()
        return _cached_id


def outbound_installation_id(enabled: bool = True) -> Optional[str]:
    """The id to put on a request, or None when the caller opted out."""
    if not sends_installation_id(enabled):
        return None
    return installation_id()


def _reset_for_tests() -> None:
    global _cached_id
    with _lock:
        _cached_id = None
