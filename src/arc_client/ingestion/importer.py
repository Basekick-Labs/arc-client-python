"""Synchronous bulk import client for Arc.

Wraps the ``/api/v1/import/*`` endpoints (CSV, Parquet, line-protocol file, TLE)
which accept a ``multipart/form-data`` upload under the field name ``file`` and
respond with ``{"status": "ok", "result": {...}}``. All require an admin token.
"""

from __future__ import annotations

import os
from typing import IO, Any, Optional, Union, cast

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcIngestionError
from arc_client.http.sync_http import SyncHTTPClient
from arc_client.models.import_ import (
    ImportResult,
    ImportStats,
    LPImportResult,
    TLEImportResult,
)

# A file source: a path, a binary file-like object, or raw bytes.
FileSource = Union[str, "os.PathLike[str]", bytes, bytearray, IO[bytes]]


def _resolve_file(source: FileSource) -> tuple[str, Any]:
    """Resolve a file source into an httpx multipart ``(filename, fileobj)`` pair.

    Returns a filename (best-effort, for the multipart part) and a bytes/file
    object that httpx can stream.
    """
    if isinstance(source, (bytes, bytearray)):
        return ("upload", bytes(source))
    if isinstance(source, (str, os.PathLike)):
        path = os.fspath(source)
        # Read the file into memory; imports are capped at 500MB server-side.
        with open(path, "rb") as fh:
            return (os.path.basename(path) or "upload", fh.read())
    # Assume a binary file-like object.
    name = getattr(source, "name", "upload")
    filename = os.path.basename(name) if isinstance(name, str) else "upload"
    return (filename, source.read())


class ImportClient:
    """Synchronous client for Arc bulk import endpoints.

    Example:
        >>> result = client.import_.import_csv("data.csv", measurement="cpu")
        >>> print(result.rows_imported)
    """

    def __init__(self, http: SyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    def _post_import(
        self,
        path: str,
        source: FileSource,
        params: dict[str, Any],
        database: Optional[str],
        extra_headers: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        """POST a multipart import request and return the ``result`` payload."""
        filename, fileobj = _resolve_file(source)
        headers = dict(extra_headers or {})
        if database:
            headers["x-arc-database"] = database

        # Drop params with None values so server defaults apply.
        clean_params = {k: v for k, v in params.items() if v is not None}

        try:
            response = self._http.post(
                path,
                files={"file": (filename, fileobj)},
                params=clean_params or None,
                headers=headers or None,
            )
            body = response.json()
        except ArcIngestionError:
            raise
        except Exception as e:
            raise ArcIngestionError(f"Import failed: {e}") from e

        result = body.get("result") if isinstance(body, dict) else None
        if result is None:
            raise ArcIngestionError(f"Unexpected import response: {body!r}")
        return cast("dict[str, Any]", result)

    def import_csv(
        self,
        source: FileSource,
        measurement: str,
        database: Optional[str] = None,
        time_column: str = "time",
        time_format: str = "",
        delimiter: str = ",",
        skip_rows: int = 0,
    ) -> ImportResult:
        """Import a CSV file.

        Args:
            source: Path, file-like object, or bytes of the CSV file.
            measurement: Target measurement (required).
            database: Target database (defaults to the client database).
            time_column: Name of the timestamp column. Default "time".
            time_format: "" (auto) or one of epoch_s/epoch_ms/epoch_us/epoch_ns.
            delimiter: Field delimiter (single character). Default ",".
            skip_rows: Rows to skip before the header. Default 0.

        Returns:
            ImportResult with rows imported and time range.
        """
        params = {
            "measurement": measurement,
            "time_column": time_column,
            "time_format": time_format or None,
            "delimiter": delimiter,
            "skip_rows": skip_rows,
        }
        result = self._post_import(
            "/api/v1/import/csv", source, params, database or self._config.database
        )
        return ImportResult.model_validate(result)

    def import_parquet(
        self,
        source: FileSource,
        measurement: str,
        database: Optional[str] = None,
        time_column: str = "time",
        time_format: str = "",
    ) -> ImportResult:
        """Import a Parquet file.

        Args:
            source: Path, file-like object, or bytes of the Parquet file.
            measurement: Target measurement (required).
            database: Target database (defaults to the client database).
            time_column: Name of the timestamp column. Default "time".
            time_format: "" (auto) or one of epoch_s/epoch_ms/epoch_us/epoch_ns.

        Returns:
            ImportResult with rows imported and time range.
        """
        params = {
            "measurement": measurement,
            "time_column": time_column,
            "time_format": time_format or None,
        }
        result = self._post_import(
            "/api/v1/import/parquet", source, params, database or self._config.database
        )
        return ImportResult.model_validate(result)

    def import_lp(
        self,
        source: FileSource,
        database: Optional[str] = None,
        precision: str = "ns",
        measurement: Optional[str] = None,
    ) -> LPImportResult:
        """Import an InfluxDB line-protocol file.

        Args:
            source: Path, file-like object, or bytes of the LP file (may be gzip).
            database: Target database (defaults to the client database).
            precision: Timestamp precision - ns/us/ms/s. Default "ns".
            measurement: Optional measurement filter; only matching lines import.

        Returns:
            LPImportResult with rows imported and measurements touched.
        """
        params = {"precision": precision, "measurement": measurement}
        result = self._post_import(
            "/api/v1/import/lp", source, params, database or self._config.database
        )
        return LPImportResult.model_validate(result)

    def import_tle(
        self,
        source: FileSource,
        database: Optional[str] = None,
        measurement: str = "satellite_tle",
    ) -> TLEImportResult:
        """Import a TLE (Two-Line Element) file.

        Args:
            source: Path, file-like object, or bytes of the TLE file (may be gzip).
            database: Target database (defaults to the client database).
            measurement: Target measurement (sent via x-arc-measurement header).
                Default "satellite_tle".

        Returns:
            TLEImportResult with satellite and row counts.
        """
        result = self._post_import(
            "/api/v1/import/tle",
            source,
            {},
            database or self._config.database,
            extra_headers={"x-arc-measurement": measurement},
        )
        return TLEImportResult.model_validate(result)

    def import_stats(self) -> ImportStats:
        """Get cumulative import statistics."""
        try:
            response = self._http.get("/api/v1/import/stats")
            body = response.json()
        except Exception as e:
            raise ArcIngestionError(f"Failed to get import stats: {e}") from e
        stats = body.get("stats", body) if isinstance(body, dict) else {}
        return ImportStats.model_validate(stats)
