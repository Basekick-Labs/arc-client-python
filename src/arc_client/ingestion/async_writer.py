"""Asynchronous write client for Arc."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Union

from arc_client.compression import Compression
from arc_client.config import ClientConfig
from arc_client.exceptions import ArcIngestionError, ArcValidationError
from arc_client.http.async_http import AsyncHTTPClient
from arc_client.ingestion.compression import compress as compress_data
from arc_client.ingestion.line_protocol import format_line_protocol
from arc_client.ingestion.msgpack import (
    dataframe_to_columnar,
    encode_columnar,
    encode_records,
)

# Compression argument accepted by write methods: a codec, its name, a bool
# (back-compat), or None to use the client default.
CompressArg = Optional[Union[Compression, str, bool]]

if TYPE_CHECKING:
    from arc_client.ingestion.async_buffered import AsyncBufferedWriter


class AsyncWriteClient:
    """Asynchronous client for writing data to Arc.

    This client provides async methods for ingesting time-series data using
    MessagePack columnar format (highest performance) or Line Protocol.

    Example:
        >>> async with AsyncArcClient(host="localhost", token="xxx") as client:
        ...     # Columnar format (fastest)
        ...     await client.write.write_columnar(
        ...         measurement="cpu",
        ...         columns={
        ...             "time": [1633024800000000, 1633024801000000],
        ...             "host": ["server01", "server01"],
        ...             "usage": [45.2, 47.8],
        ...         },
        ...     )
    """

    def __init__(self, http: AsyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    async def write_columnar(
        self,
        measurement: str,
        columns: dict[str, list[Any]],
        database: Optional[str] = None,
        compress: CompressArg = None,
        time_unit: str = "us",
    ) -> None:
        """Write columnar data to Arc (highest performance method).

        Args:
            measurement: The measurement (table) name.
            columns: Dictionary mapping column names to lists of values.
            database: Target database. Uses client default if not specified.
            compress: Whether to gzip compress. Uses client default if not specified.
            time_unit: Unit of timestamps - "s", "ms", or "us" (default).
        """
        try:
            data = encode_columnar(measurement, columns, time_unit)
        except ArcValidationError:
            raise
        except Exception as e:
            raise ArcValidationError(f"Failed to encode columnar data: {e}") from e

        await self._write_msgpack(data, database, compress)

    async def write_records(
        self,
        records: list[dict[str, Any]],
        database: Optional[str] = None,
        compress: CompressArg = None,
    ) -> None:
        """Write records to Arc using MessagePack row format."""
        try:
            data = encode_records(records)
        except ArcValidationError:
            raise
        except Exception as e:
            raise ArcValidationError(f"Failed to encode records: {e}") from e

        await self._write_msgpack(data, database, compress)

    async def write_dataframe(
        self,
        df: Any,
        measurement: str,
        database: Optional[str] = None,
        time_column: str = "time",
        tag_columns: Optional[list[str]] = None,
        compress: CompressArg = None,
    ) -> None:
        """Write a DataFrame to Arc."""
        try:
            columns = dataframe_to_columnar(df, measurement, time_column, tag_columns)
        except ArcValidationError:
            raise
        except Exception as e:
            raise ArcValidationError(f"Failed to convert DataFrame: {e}") from e

        await self.write_columnar(measurement, columns, database, compress)

    async def write_line_protocol(
        self,
        lines: str | list[str],
        database: Optional[str] = None,
        compress: CompressArg = None,
    ) -> None:
        """Write data using InfluxDB Line Protocol format."""
        data = "\n".join(lines) if isinstance(lines, list) else lines
        await self._write_line_protocol(data.encode("utf-8"), database, compress)

    async def write_point(
        self,
        measurement: str,
        fields: dict[str, Any],
        tags: Optional[dict[str, str]] = None,
        timestamp: Optional[int] = None,
        database: Optional[str] = None,
        compress: CompressArg = None,
    ) -> None:
        """Write a single data point using Line Protocol."""
        line = format_line_protocol(measurement, fields, tags, timestamp)
        await self._write_line_protocol(line.encode("utf-8"), database, compress)

    async def write_tle(
        self,
        tle_text: str | bytes,
        measurement: str = "satellite_tle",
        database: Optional[str] = None,
        compress: CompressArg = None,
    ) -> None:
        """Stream raw TLE (Two-Line Element) text to Arc.

        See WriteClient.write_tle for details. For large files prefer
        ``client.import_.import_tle``.
        """
        data = tle_text.encode("utf-8") if isinstance(tle_text, str) else tle_text
        await self._post_write(
            "/api/v1/write/tle",
            data,
            "text/plain",
            database,
            compress,
            extra_headers={"x-arc-measurement": measurement},
        )

    async def tle_stats(self) -> dict[str, Any]:
        """Get TLE streaming write statistics from ``/api/v1/write/tle/stats``."""
        try:
            response = await self._http.get("/api/v1/write/tle/stats")
            body = response.json()
        except Exception as e:
            raise ArcIngestionError(f"Failed to get TLE stats: {e}") from e
        return body.get("stats", body) if isinstance(body, dict) else {}

    def buffered(
        self,
        batch_size: int = 10000,
        flush_interval: float = 5.0,
    ) -> AsyncBufferedWriter:
        """Create a buffered writer for automatic batching."""
        from arc_client.ingestion.async_buffered import AsyncBufferedWriter

        return AsyncBufferedWriter(self, batch_size, flush_interval)

    def _resolve_codec(self, compress: CompressArg) -> Compression:
        """Resolve the effective compression codec for a write."""
        if compress is None:
            return Compression.coerce(self._config.compression)
        return Compression.coerce(compress)

    async def _post_write(
        self,
        path: str,
        data: bytes,
        content_type: str,
        database: Optional[str],
        compress: CompressArg,
        extra_headers: Optional[dict[str, str]] = None,
    ) -> None:
        """Compress (if requested) and POST a write payload.

        Arc auto-detects gzip/zstd from the payload's magic bytes, so no
        ``Content-Encoding`` header is sent.
        """
        codec = self._resolve_codec(compress)
        data = compress_data(data, codec)

        headers = {"Content-Type": content_type}
        if extra_headers:
            headers.update(extra_headers)
        if database:
            headers["x-arc-database"] = database

        try:
            response = await self._http.post(path, content=data, headers=headers)
            if response.status_code not in (200, 204):
                raise ArcIngestionError(
                    f"Write failed with status {response.status_code}: {response.text}"
                )
        except ArcIngestionError:
            raise
        except Exception as e:
            raise ArcIngestionError(f"Failed to write data: {e}") from e

    async def _write_msgpack(
        self,
        data: bytes,
        database: Optional[str],
        compress: CompressArg,
    ) -> None:
        """Send MessagePack data to Arc."""
        await self._post_write(
            "/api/v1/write/msgpack", data, "application/msgpack", database, compress
        )

    async def _write_line_protocol(
        self,
        data: bytes,
        database: Optional[str],
        compress: CompressArg,
    ) -> None:
        """Send Line Protocol data to Arc."""
        await self._post_write(
            "/api/v1/write/line-protocol", data, "text/plain", database, compress
        )
