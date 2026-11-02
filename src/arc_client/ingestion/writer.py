"""Synchronous write client for Arc."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Optional, Union

from arc_client.compression import Compression
from arc_client.config import ClientConfig
from arc_client.exceptions import (
    ArcError,
    ArcIngestionError,
    ArcIngestRefusedError,
    ArcValidationError,
)
from arc_client.http.base import is_retryable_write_refusal
from arc_client.http.sync_http import SyncHTTPClient
from arc_client.ingestion.compression import compress as compress_data
from arc_client.ingestion.line_protocol import format_line_protocol
from arc_client.ingestion.msgpack import (
    dataframe_to_columnar,
    encode_columnar,
    encode_records,
)
from arc_client.ingestion.write_support import (
    as_write_error,
    refusal_without_resend,
    single_lp_measurement,
    tle_stats_body,
    validate_precision,
)

# Compression argument accepted by write methods: a codec, its name, a bool
# (back-compat), or None to use the client default.
CompressArg = Optional[Union[Compression, str, bool]]

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
        database: Optional[str] = None,
        compress: CompressArg = None,
        time_unit: str = "us",
        tag_columns: Optional[list[str]] = None,
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
            tag_columns: Columns Arc should store as tags (indexed dimensions)
                rather than fields. Sent as the payload's ``tag_keys``, which
                Arc records in the Parquet ``arc:tags`` metadata (used for
                compaction dedup and tag-aware queries). Each must be a column
                other than ``time``. Arc releases before 2027.01 ignore it.

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
            data = encode_columnar(measurement, columns, time_unit, tag_keys=tag_columns)
        except ArcValidationError:
            raise
        except Exception as e:
            raise ArcValidationError(f"Failed to encode columnar data: {e}") from e

        self._write_msgpack(data, database, compress)

    def write_records(
        self,
        records: list[dict[str, Any]],
        database: Optional[str] = None,
        compress: CompressArg = None,
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
        database: Optional[str] = None,
        time_column: str = "time",
        tag_columns: Optional[list[str]] = None,
        compress: CompressArg = None,
    ) -> None:
        """Write a DataFrame to Arc.

        Supports pandas DataFrames, Polars DataFrames, and PyArrow Tables.
        The DataFrame is converted to columnar format for optimal performance.

        Args:
            df: pandas DataFrame, Polars DataFrame, or PyArrow Table.
            measurement: The measurement (table) name.
            database: Target database. Uses client default if not specified.
            time_column: Name of the timestamp column. Default "time".
            tag_columns: Columns to store as tags (dimensions) rather than
                fields, sent as ``tag_keys``. Must name existing columns other
                than the time column.
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

        self.write_columnar(measurement, columns, database, compress, tag_columns=tag_columns)

    def write_line_protocol(
        self,
        lines: str | list[str],
        database: Optional[str] = None,
        compress: CompressArg = None,
        precision: Optional[str] = None,
    ) -> None:
        """Write data using InfluxDB Line Protocol format.

        This method is provided for compatibility with InfluxDB tooling.
        For best performance, use write_columnar() instead.

        Args:
            lines: Line Protocol string or list of lines.
            database: Target database. Uses client default if not specified.
            compress: Whether to gzip compress. Uses client default if not specified.
            precision: Unit of the timestamps in ``lines``: ``"ns"``, ``"us"``,
                ``"ms"`` or ``"s"``, sent as Arc's ``?precision=``. ``None``
                (default) sends nothing and Arc assumes nanoseconds.

        Raises:
            ArcIngestionError: If the write fails.
            ArcPartialWriteError: If Arc dropped some points (never retried).
            ArcValidationError: If ``precision`` is not one Arc accepts.

        Example:
            >>> client.write.write_line_protocol(
            ...     "cpu,host=server01 usage=45.2 1633024800000000000"
            ... )
        """
        data = "\n".join(lines) if isinstance(lines, list) else lines
        self._write_line_protocol(
            data.encode("utf-8"), database, compress, validate_precision(precision)
        )

    def write_point(
        self,
        measurement: str,
        fields: dict[str, Any],
        tags: Optional[dict[str, str]] = None,
        timestamp: Optional[int] = None,
        database: Optional[str] = None,
        compress: CompressArg = None,
        precision: Optional[str] = None,
    ) -> None:
        """Write a single data point using Line Protocol.

        Convenience method for writing individual points.

        Args:
            measurement: The measurement name.
            fields: Dictionary of field values.
            tags: Optional dictionary of tag values.
            timestamp: Optional timestamp, in microseconds unless ``precision``
                is given.
            database: Target database. Uses client default if not specified.
            compress: Whether to gzip compress. Uses client default if not specified.
            precision: Unit of ``timestamp`` (``"ns"``, ``"us"``, ``"ms"`` or
                ``"s"``). When given, the timestamp is sent unconverted with
                Arc's ``?precision=``. ``None`` (default) keeps the historical
                behavior: microseconds, converted to nanoseconds on the wire.
        """
        precision = validate_precision(precision)
        # With an explicit precision the timestamp goes out as-is ("ns" means
        # "no conversion") and Arc interprets it in that unit.
        time_unit = "us" if precision is None else "ns"
        line = format_line_protocol(measurement, fields, tags, timestamp, time_unit)
        self._write_line_protocol(line.encode("utf-8"), database, compress, precision)

    def write_tle(
        self,
        tle_text: str | bytes,
        measurement: str = "satellite_tle",
        database: Optional[str] = None,
        compress: CompressArg = None,
    ) -> None:
        """Stream raw TLE (Two-Line Element) text to Arc.

        Sends the TLE text as-is to ``/api/v1/write/tle``; the server parses it
        into orbital-element fields. For large historical files, prefer
        ``client.import_.import_tle`` (bulk import).

        Args:
            tle_text: TLE text (str or bytes). May contain many satellites.
            measurement: Target measurement, sent via x-arc-measurement header.
                Default "satellite_tle".
            database: Target database. Uses client default if not specified.
            compress: Compression codec. Uses client default if not specified.

        Raises:
            ArcIngestionError: If the write fails.
        """
        data = tle_text.encode("utf-8") if isinstance(tle_text, str) else tle_text
        self._post_write(
            "/api/v1/write/tle",
            data,
            "text/plain",
            database,
            compress,
            extra_headers={"x-arc-measurement": measurement},
        )

    def tle_stats(self) -> dict[str, Any]:
        """Get TLE streaming write statistics from ``/api/v1/write/tle/stats``."""
        try:
            response = self._http.get("/api/v1/write/tle/stats")
            body = response.json()
        except ArcError:
            raise
        except Exception as e:
            raise ArcIngestionError(f"Failed to get TLE stats: {e}") from e
        return tle_stats_body(body)

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

    def _resolve_codec(self, compress: CompressArg) -> Compression:
        """Resolve the effective compression codec for a write.

        ``compress`` overrides the client default when provided. Accepts a
        :class:`Compression`, a string codec name, or a bool (True->gzip,
        False->none) for backward compatibility.
        """
        if compress is None:
            return Compression.coerce(self._config.compression)
        return Compression.coerce(compress)

    def _post_write(
        self,
        path: str,
        data: bytes,
        content_type: str,
        database: Optional[str],
        compress: CompressArg,
        extra_headers: Optional[dict[str, str]] = None,
        params: Optional[dict[str, Any]] = None,
        resend_refusals: Callable[[], bool] = lambda: True,
    ) -> None:
        """Compress (if requested) and POST a write payload.

        Arc auto-detects gzip/zstd from the payload's magic bytes, so no
        ``Content-Encoding`` header is sent.

        A 503 Arc marks as an ingest refusal that applied nothing
        (``X-Arc-Ingest-Fenced``, or ``X-Arc-Ingest-Refused`` with any reason
        but ``wal_not_durable``) is resent up to ``max_retries`` times,
        honoring ``Retry-After``. Nothing else is resent: a partial write, a
        ``wal_not_durable`` refusal or any other failure may have stored
        data. Errors are mapped by ``as_write_error``.

        ``resend_refusals`` is asked only when Arc refuses the write. It answers
        False for multi-measurement line protocol: Arc re-checks its fence per
        measurement there, so a refusal may follow stored measurements. Such a
        refusal is raised with ``retry_safe=False``.
        """
        codec = self._resolve_codec(compress)
        data = compress_data(data, codec)

        headers = {"Content-Type": content_type}
        if extra_headers:
            headers.update(extra_headers)
        if database:
            headers["x-arc-database"] = database

        try:
            response = self._http.post(
                path,
                content=data,
                headers=headers,
                params=params,
                retry_if=lambda r: is_retryable_write_refusal(r) and resend_refusals(),
            )
        except Exception as e:
            mapped = as_write_error(e)
            if isinstance(mapped, ArcIngestRefusedError) and not resend_refusals():
                mapped = refusal_without_resend(mapped)
            if mapped is e:
                raise
            raise mapped from e
        # Arc returns 204 No Content on success
        if response.status_code not in (200, 204):
            raise ArcIngestionError(
                f"Write failed with status {response.status_code}: {response.text}",
                status_code=response.status_code,
            )

    def _write_msgpack(
        self,
        data: bytes,
        database: Optional[str],
        compress: CompressArg,
    ) -> None:
        """Send MessagePack data to Arc.

        Arc checks its ingest fence once for the whole msgpack batch
        (internal/ingest/arrow_writer.go ArrowBuffer.Write), so a refusal
        applied nothing and is safe to resend, whatever the measurements.
        """
        self._post_write(
            "/api/v1/write/msgpack",
            data,
            "application/msgpack",
            database,
            compress,
        )

    def _write_line_protocol(
        self,
        data: bytes,
        database: Optional[str],
        compress: CompressArg,
        precision: Optional[str] = None,
    ) -> None:
        """Send Line Protocol data to Arc."""
        self._post_write(
            "/api/v1/write/line-protocol",
            data,
            "text/plain",
            database,
            compress,
            params={"precision": precision} if precision else None,
            resend_refusals=lambda: single_lp_measurement(data),
        )
