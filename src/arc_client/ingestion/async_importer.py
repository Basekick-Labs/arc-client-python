"""Asynchronous bulk import client for Arc.

Async twin of :mod:`arc_client.ingestion.importer`. Wraps the
``/api/v1/import/*`` endpoints (CSV, Parquet, line-protocol file, TLE).
"""

from __future__ import annotations

from typing import Any, Optional, cast

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcIngestionError
from arc_client.http.async_http import AsyncHTTPClient
from arc_client.ingestion.importer import FileSource, _resolve_file
from arc_client.models.import_ import (
    ImportResult,
    ImportStats,
    LPImportResult,
    TLEImportResult,
)


class AsyncImportClient:
    """Asynchronous client for Arc bulk import endpoints.

    Example:
        >>> result = await client.import_.import_csv("data.csv", measurement="cpu")
        >>> print(result.rows_imported)
    """

    def __init__(self, http: AsyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    async def _post_import(
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

        clean_params = {k: v for k, v in params.items() if v is not None}

        try:
            response = await self._http.post(
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

    async def import_csv(
        self,
        source: FileSource,
        measurement: str,
        database: Optional[str] = None,
        time_column: str = "time",
        time_format: str = "",
        delimiter: str = ",",
        skip_rows: int = 0,
    ) -> ImportResult:
        """Import a CSV file. See ImportClient.import_csv for details."""
        params = {
            "measurement": measurement,
            "time_column": time_column,
            "time_format": time_format or None,
            "delimiter": delimiter,
            "skip_rows": skip_rows,
        }
        result = await self._post_import(
            "/api/v1/import/csv", source, params, database or self._config.database
        )
        return ImportResult.model_validate(result)

    async def import_parquet(
        self,
        source: FileSource,
        measurement: str,
        database: Optional[str] = None,
        time_column: str = "time",
        time_format: str = "",
    ) -> ImportResult:
        """Import a Parquet file. See ImportClient.import_parquet for details."""
        params = {
            "measurement": measurement,
            "time_column": time_column,
            "time_format": time_format or None,
        }
        result = await self._post_import(
            "/api/v1/import/parquet", source, params, database or self._config.database
        )
        return ImportResult.model_validate(result)

    async def import_lp(
        self,
        source: FileSource,
        database: Optional[str] = None,
        precision: str = "ns",
        measurement: Optional[str] = None,
    ) -> LPImportResult:
        """Import a line-protocol file. See ImportClient.import_lp for details."""
        params = {"precision": precision, "measurement": measurement}
        result = await self._post_import(
            "/api/v1/import/lp", source, params, database or self._config.database
        )
        return LPImportResult.model_validate(result)

    async def import_tle(
        self,
        source: FileSource,
        database: Optional[str] = None,
        measurement: str = "satellite_tle",
    ) -> TLEImportResult:
        """Import a TLE file. See ImportClient.import_tle for details."""
        result = await self._post_import(
            "/api/v1/import/tle",
            source,
            {},
            database or self._config.database,
            extra_headers={"x-arc-measurement": measurement},
        )
        return TLEImportResult.model_validate(result)

    async def import_stats(self) -> ImportStats:
        """Get cumulative import statistics."""
        try:
            response = await self._http.get("/api/v1/import/stats")
            body = response.json()
        except Exception as e:
            raise ArcIngestionError(f"Failed to get import stats: {e}") from e
        stats = body.get("stats", body) if isinstance(body, dict) else {}
        return ImportStats.model_validate(stats)
