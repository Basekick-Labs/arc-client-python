"""Asynchronous write client for Arc."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcIngestionError, ArcValidationError
from arc_client.http.async_http import AsyncHTTPClient
from arc_client.ingestion.compression import compress_gzip
from arc_client.ingestion.line_protocol import format_line_protocol
from arc_client.ingestion.msgpack import (
    dataframe_to_columnar,
    encode_columnar,
    encode_records,
)

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
        database: str | None = None,
        compress: bool | None = None,
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
        database: str | None = None,
        compress: bool | None = None,
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
        database: str | None = None,
        time_column: str = "time",
        tag_columns: list[str] | None = None,
        compress: bool | None = None,
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
        database: str | None = None,
        compress: bool | None = None,
    ) -> None:
        """Write data using InfluxDB Line Protocol format."""
        data = "\n".join(lines) if isinstance(lines, list) else lines
        await self._write_line_protocol(data.encode("utf-8"), database, compress)

    async def write_point(
        self,
        measurement: str,
        fields: dict[str, Any],
        tags: dict[str, str] | None = None,
        timestamp: int | None = None,
        database: str | None = None,
        compress: bool | None = None,
    ) -> None:
        """Write a single data point using Line Protocol."""
        line = format_line_protocol(measurement, fields, tags, timestamp)
        await self._write_line_protocol(line.encode("utf-8"), database, compress)

    def buffered(
        self,
        batch_size: int = 10000,
        flush_interval: float = 5.0,
    ) -> AsyncBufferedWriter:
        """Create a buffered writer for automatic batching."""
        from arc_client.ingestion.async_buffered import AsyncBufferedWriter

        return AsyncBufferedWriter(self, batch_size, flush_interval)

    async def _write_msgpack(
        self,
        data: bytes,
        database: str | None,
        compress: bool | None,
    ) -> None:
        """Send MessagePack data to Arc."""
        should_compress = compress if compress is not None else self._config.compression

        if should_compress:
            data = compress_gzip(data)

        headers = {
            "Content-Type": "application/msgpack",
        }
        if should_compress:
            headers["Content-Encoding"] = "gzip"

        if database:
            headers["x-arc-database"] = database

        try:
            response = await self._http.post(
                "/api/v1/write/msgpack",
                content=data,
                headers=headers,
            )
            if response.status_code not in (200, 204):
                raise ArcIngestionError(
                    f"Write failed with status {response.status_code}: {response.text}"
                )
        except ArcIngestionError:
            raise
        except Exception as e:
            raise ArcIngestionError(f"Failed to write data: {e}") from e

    async def _write_line_protocol(
        self,
        data: bytes,
        database: str | None,
        compress: bool | None,
    ) -> None:
        """Send Line Protocol data to Arc."""
        should_compress = compress if compress is not None else self._config.compression

        if should_compress:
            data = compress_gzip(data)

        headers = {
            "Content-Type": "text/plain",
        }
        if should_compress:
            headers["Content-Encoding"] = "gzip"

        if database:
            headers["x-arc-database"] = database

        try:
            response = await self._http.post(
                "/api/v1/write/line-protocol",
                content=data,
                headers=headers,
            )
            if response.status_code not in (200, 204):
                raise ArcIngestionError(
                    f"Write failed with status {response.status_code}: {response.text}"
                )
        except ArcIngestionError:
            raise
        except Exception as e:
            raise ArcIngestionError(f"Failed to write data: {e}") from e
