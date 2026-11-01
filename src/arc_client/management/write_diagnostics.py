"""Synchronous diagnostics for Arc write protocols.

Wraps the read-only Line Protocol health/statistics and MessagePack
specification/statistics endpoints. All endpoints require authentication.
Responses are returned as raw JSON values because Arc does not promise a
stable schema for these operator diagnostics.
"""

from __future__ import annotations

from typing import Any

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError
from arc_client.http.sync_http import SyncHTTPClient


class WriteDiagnosticsClient:
    """Inspect writer health and statistics; return raw, server-defined JSON."""

    def __init__(self, http: SyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    def _json(self, path: str) -> Any:
        try:
            response = self._http.get(path)
            return response.json()
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Write diagnostics request failed (GET {path}): {e}") from e

    def line_protocol_health(self) -> Any:
        """Return raw JSON health status for the Line Protocol writer."""
        return self._json("/api/v1/write/line-protocol/health")

    def line_protocol_stats(self) -> Any:
        """Return raw JSON request, record, byte, and error counters for Line Protocol."""
        return self._json("/api/v1/write/line-protocol/stats")

    def msgpack_spec(self) -> Any:
        """Return the raw JSON MessagePack write-protocol specification."""
        return self._json("/api/v1/write/msgpack/spec")

    def msgpack_stats(self) -> Any:
        """Return raw JSON MessagePack decoder and Arrow buffer statistics."""
        return self._json("/api/v1/write/msgpack/stats")
