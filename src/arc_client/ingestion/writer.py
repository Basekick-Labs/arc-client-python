"""Synchronous write client for Arc."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcIngestionError, ArcValidationError
from arc_client.http.sync_http import SyncHTTPClient
from arc_client.ingestion.compression import compress_gzip
from arc_client.ingestion.line_protocol import format_line_protocol
from arc_client.ingestion.msgpack import (
    dataframe_to_columnar,
    encode_columnar,
    encode_records,
)

if TYPE_CHECKING:
    from arc_client.ingestion.buffered import BufferedWriter


class WriteClient:
    """Synchronous client for writing data to Arc.

    This client provides methods for ingesting time-series data using
    MessagePack columnar format (highest performance) or Line Protocol.

    Example:
        >>> with ArcClient(host="localhost", token="xxx") as client:
        ...     # Columnar format (fastest)
        ...     client.write.write_columnar(
        ...         measurement="cpu",
        ...         columns={
        ...             "time": [1633024800000000, 1633024801000000],
        ...             "host": ["server01", "server01"],
        ...             "usage": [45.2, 47.8],
        ...         },
        ...     )
        ...
        ...     # From DataFrame
        ...     client.write.write_dataframe(df, measurement="cpu")
        ...
        ...     # Line Protocol (InfluxDB compatible)
        ...     client.write.write_line_protocol("cpu,host=s1 usage=45.2")
    """

    def __init__(self, http: SyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    def write_columnar(
        self,
        measurement: str,
        columns: dict[str, list[Any]],
        database: str | None = None,
        compress: bool | None = None,
        time_unit: str = "us",
    ) -> None:
        """Write columnar data to Arc (highest performance method).

        This is the recommended ingestion method, providing 25-35% faster
        ingestion than row format due to zero-copy passthrough.

        Args:
            measurement: The measurement (table) name.
            columns: Dictionary mapping column names to lists of values.
                Must include a 'time' column with timestamps.
                All arrays must have the same length.
            database: Target database. Uses client default if not specified.
            compress: Whether to gzip compress. Uses client default if not specified.
            time_unit: Unit of timestamps - "s", "ms", or "us" (default).

        Raises:
            ArcIngestionError: If the write fails.
            ArcValidationError: If columns are invalid.

        Example:
            >>> client.write.write_columnar(
            ...     measurement="cpu",
            ...     columns={
            ...         "time": [1633024800000000, 1633024801000000],
            ...         "host": ["server01", "server01"],
            ...         "usage": [45.2, 47.8],
            ...     },
            ... )
        """
        # Encode to MessagePack
        try:
            data = encode_columnar(measurement, columns, time_unit)
        except ArcValidationError:
            raise
        except Exception as e:
            raise ArcValidationError(f"Failed to encode columnar data: {e}") from e

        self._write_msgpack(data, database, compress)

    def write_records(
        self,
        records: list[dict[str, Any]],
        database: str | None = None,
        compress: bool | None = None,
    ) -> None:
        """Write records to Arc using MessagePack row format.

        This uses the legacy row format. For better performance, use
        write_columnar() instead.

        Args:
            records: List of record dictionaries. Each should have:
                - measurement: str - The measurement name
                - timestamp: int - Timestamp in microseconds (optional)
                - fields: dict - Field values (required)
                - tags: dict - Tag values (optional)
            database: Target database. Uses client default if not specified.
            compress: Whether to gzip compress. Uses client default if not specified.

        Raises:
            ArcIngestionError: If the write fails.
            ArcValidationError: If records are invalid.
        """
        try:
            data = encode_records(records)
        except ArcValidationError:
            raise
        except Exception as e:
            raise ArcValidationError(f"Failed to encode records: {e}") from e

        self._write_msgpack(data, database, compress)

    def write_dataframe(
        self,
        df: Any,
        measurement: str,
        database: str | None = None,
        time_column: str = "time",
        tag_columns: list[str] | None = None,
        compress: bool | None = None,
    ) -> None:
        """Write a DataFrame to Arc.

        Supports pandas DataFrames, Polars DataFrames, and PyArrow Tables.
        The DataFrame is converted to columnar format for optimal performance.

        Args:
            df: pandas DataFrame, Polars DataFrame, or PyArrow Table.
            measurement: The measurement (table) name.
            database: Target database. Uses client default if not specified.
            time_column: Name of the timestamp column. Default "time".
            tag_columns: Columns to treat as tags (dimensions).
            compress: Whether to gzip compress. Uses client default if not specified.

        Raises:
            ArcIngestionError: If the write fails.
            ArcValidationError: If DataFrame is invalid.

        Example:
            >>> import pandas as pd
            >>> df = pd.DataFrame({
            ...     "time": [1633024800000000, 1633024801000000],
            ...     "host": ["server01", "server01"],
            ...     "usage": [45.2, 47.8],
            ... })
            >>> client.write.write_dataframe(df, measurement="cpu")
        """
        try:
            columns = dataframe_to_columnar(df, measurement, time_column, tag_columns)
        except ArcValidationError:
            raise
        except Exception as e:
            raise ArcValidationError(f"Failed to convert DataFrame: {e}") from e

        self.write_columnar(measurement, columns, database, compress)

    def write_line_protocol(
        self,
        lines: str | list[str],
        database: str | None = None,
        compress: bool | None = None,
    ) -> None:
        """Write data using InfluxDB Line Protocol format.

        This method is provided for compatibility with InfluxDB tooling.
        For best performance, use write_columnar() instead.

        Args:
            lines: Line Protocol string or list of lines.
            database: Target database. Uses client default if not specified.
            compress: Whether to gzip compress. Uses client default if not specified.

        Raises:
            ArcIngestionError: If the write fails.

        Example:
            >>> client.write.write_line_protocol(
            ...     "cpu,host=server01 usage=45.2 1633024800000000000"
            ... )
        """
        data = "\n".join(lines) if isinstance(lines, list) else lines
        self._write_line_protocol(data.encode("utf-8"), database, compress)

    def write_point(
        self,
        measurement: str,
        fields: dict[str, Any],
        tags: dict[str, str] | None = None,
        timestamp: int | None = None,
        database: str | None = None,
        compress: bool | None = None,
    ) -> None:
        """Write a single data point using Line Protocol.

        Convenience method for writing individual points.

        Args:
            measurement: The measurement name.
            fields: Dictionary of field values.
            tags: Optional dictionary of tag values.
            timestamp: Optional timestamp in microseconds.
            database: Target database. Uses client default if not specified.
            compress: Whether to gzip compress. Uses client default if not specified.
        """
        line = format_line_protocol(measurement, fields, tags, timestamp)
        self._write_line_protocol(line.encode("utf-8"), database, compress)

    def buffered(
        self,
        batch_size: int = 10000,
        flush_interval: float = 5.0,
    ) -> BufferedWriter:
        """Create a buffered writer for automatic batching.

        The buffered writer accumulates records and flushes them in batches
        for optimal throughput. It flushes automatically when:
        - batch_size records have accumulated
        - flush_interval seconds have passed since last flush

        Args:
            batch_size: Maximum number of records per batch. Default 10000.
            flush_interval: Maximum seconds between flushes. Default 5.0.

        Returns:
            BufferedWriter context manager.

        Example:
            >>> with client.write.buffered(batch_size=10000) as buffer:
            ...     for record in records:
            ...         buffer.write(record)
            ...     # Auto-flushes on exit
        """
        from arc_client.ingestion.buffered import BufferedWriter

        return BufferedWriter(self, batch_size, flush_interval)

    def _write_msgpack(
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
            response = self._http.post(
                "/api/v1/write/msgpack",
                content=data,
                headers=headers,
            )
            # Arc returns 204 No Content on success
            if response.status_code not in (200, 204):
                raise ArcIngestionError(
                    f"Write failed with status {response.status_code}: {response.text}"
                )
        except ArcIngestionError:
            raise
        except Exception as e:
            raise ArcIngestionError(f"Failed to write data: {e}") from e

    def _write_line_protocol(
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
            response = self._http.post(
                "/api/v1/write/line-protocol",
                content=data,
                headers=headers,
            )
            # Arc returns 204 No Content on success
            if response.status_code not in (200, 204):
                raise ArcIngestionError(
                    f"Write failed with status {response.status_code}: {response.text}"
                )
        except ArcIngestionError:
            raise
        except Exception as e:
            raise ArcIngestionError(f"Failed to write data: {e}") from e
