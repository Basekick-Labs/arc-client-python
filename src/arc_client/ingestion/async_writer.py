"""Asynchronous write client for Arc."""

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
from arc_client.http.async_http import AsyncHTTPClient
from arc_client.http.base import is_retryable_write_refusal
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
        tag_columns: Optional[list[str]] = None,
    ) -> None:
        """Write columnar data to Arc (highest performance method).

        Args:
            measurement: The measurement (table) name.
            columns: Dictionary mapping column names to lists of values.
            database: Target database. Uses client default if not specified.
            compress: Whether to gzip compress. Uses client default if not specified.
            time_unit: Unit of timestamps - "s", "ms", or "us" (default).
            tag_columns: Columns Arc should store as tags (indexed dimensions)
                rather than fields. Sent as the payload's ``tag_keys``; see
                the sync client's ``write_columnar`` for details.
        """
        try:
            data = encode_columnar(measurement, columns, time_unit, tag_keys=tag_columns)
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

        await self.write_columnar(measurement, columns, database, compress, tag_columns=tag_columns)

    async def write_line_protocol(
        self,
        lines: str | list[str],
        database: Optional[str] = None,
        compress: CompressArg = None,
        precision: Optional[str] = None,
    ) -> None:
        """Write data using InfluxDB Line Protocol format."""
        data = "\n".join(lines) if isinstance(lines, list) else lines
        await self._write_line_protocol(
            data.encode("utf-8"), database, compress, validate_precision(precision)
        )

    async def write_point(
        self,
        measurement: str,
        fields: dict[str, Any],
        tags: Optional[dict[str, str]] = None,
        timestamp: Optional[int] = None,
        database: Optional[str] = None,
        compress: CompressArg = None,
        precision: Optional[str] = None,
    ) -> None:
        """Write a single data point using Line Protocol."""
        precision = validate_precision(precision)
        # With an explicit precision the timestamp goes out as-is ("ns" means
        # "no conversion") and Arc interprets it in that unit.
        time_unit = "us" if precision is None else "ns"
        line = format_line_protocol(measurement, fields, tags, timestamp, time_unit)
        await self._write_line_protocol(line.encode("utf-8"), database, compress, precision)

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
        except ArcError:
            raise
        except Exception as e:
            raise ArcIngestionError(f"Failed to get TLE stats: {e}") from e
        return tle_stats_body(body)

    async def flush_server_buffers(self) -> dict[str, Any]:
        """Flush this node's ingest buffers to Parquet now (admin token).

        ``POST /api/v1/write/line-protocol/flush``. Despite the route name,
        Arc flushes every buffered row on the node it is sent to: line
        protocol, MessagePack and MQTT share one buffer, across all
        databases. Rows become visible to queries without waiting for
        ``ingest.max_buffer_age_ms``. In a cluster it flushes only the node
        that receives it.

        Not the client-side :meth:`buffered` writer's ``flush()``, which
        sends rows the client holds; this acts on rows Arc already accepted.

        Returns:
            Arc's JSON answer, e.g. ``{"status": "success", "message": ...}``.

        Raises:
            ArcPermissionError: The token is not an admin token.
        """
        try:
            response = await self._http.post("/api/v1/write/line-protocol/flush")
            body: dict[str, Any] = response.json()
        except ArcError:
            raise
        except Exception as e:
            raise ArcIngestionError(f"Failed to flush server buffers: {e}") from e
        return body

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
            response = await self._http.post(
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

    async def _write_msgpack(
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
        await self._post_write(
            "/api/v1/write/msgpack",
            data,
            "application/msgpack",
            database,
            compress,
        )

    async def _write_line_protocol(
        self,
        data: bytes,
        database: Optional[str],
        compress: CompressArg,
        precision: Optional[str] = None,
    ) -> None:
        """Send Line Protocol data to Arc."""
        await self._post_write(
            "/api/v1/write/line-protocol",
            data,
            "text/plain",
            database,
            compress,
            params={"precision": precision} if precision else None,
            resend_refusals=lambda: single_lp_measurement(data),
        )
