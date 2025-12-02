"""Buffered writer for automatic batching of writes.

The BufferedWriter accumulates records and flushes them in batches for
optimal throughput. It supports both sync and async modes.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from arc_client.ingestion.writer import WriteClient


class BufferedWriter:
    """Buffered writer that batches records for optimal throughput.

    Automatically flushes when:
    - batch_size records have accumulated for a measurement
    - flush_interval seconds have passed since last flush
    - The context manager exits

    Example:
        >>> with client.write.buffered(batch_size=10000) as buffer:
        ...     for record in records:
        ...         buffer.write(record)
        ...     # Auto-flushes on exit

        >>> # Or with columnar data
        >>> with client.write.buffered() as buffer:
        ...     buffer.write_columnar("cpu", {"time": [...], "usage": [...]})
    """

    def __init__(
        self,
        write_client: WriteClient,
        batch_size: int = 10000,
        flush_interval: float = 5.0,
    ) -> None:
        """Initialize the buffered writer.

        Args:
            write_client: The underlying WriteClient instance.
            batch_size: Maximum records per measurement before auto-flush.
            flush_interval: Maximum seconds between flushes.
        """
        self._client = write_client
        self._batch_size = batch_size
        self._flush_interval = flush_interval

        # Buffers: measurement -> list of column dicts
        self._buffers: dict[str, list[dict[str, list[Any]]]] = defaultdict(list)
        self._record_counts: dict[str, int] = defaultdict(int)

        self._last_flush_time = time.monotonic()
        self._lock = threading.Lock()
        self._closed = False

    def write(self, record: dict[str, Any]) -> None:
        """Write a single record to the buffer.

        The record will be buffered and flushed when batch_size is reached
        or flush_interval has passed.

        Args:
            record: Record dictionary with:
                - measurement: str (required)
                - timestamp: int (optional, microseconds)
                - fields: dict (required)
                - tags: dict (optional)
        """
        measurement = record.get("measurement")
        if not measurement:
            raise ValueError("Record must have 'measurement' field")

        fields = record.get("fields", {})
        if not fields:
            raise ValueError("Record must have 'fields' field")

        timestamp = record.get("timestamp")
        if timestamp is None:
            timestamp = int(time.time() * 1_000_000)

        tags = record.get("tags", {})

        # Convert row to columnar format for buffering
        columns: dict[str, list[Any]] = {"time": [timestamp]}
        for key, value in fields.items():
            columns[key] = [value]
        for key, value in tags.items():
            columns[key] = [value]

        with self._lock:
            self._buffers[measurement].append(columns)
            self._record_counts[measurement] += 1

            # Check if we should flush this measurement
            if self._record_counts[measurement] >= self._batch_size:
                self._flush_measurement(measurement)

            # Check time-based flush
            if time.monotonic() - self._last_flush_time >= self._flush_interval:
                self._flush_all()

    def write_columnar(
        self,
        measurement: str,
        columns: dict[str, list[Any]],
    ) -> None:
        """Write columnar data to the buffer.

        Args:
            measurement: The measurement name.
            columns: Dictionary of column name to value list.
        """
        if not columns:
            return

        num_records = len(next(iter(columns.values())))

        with self._lock:
            self._buffers[measurement].append(columns)
            self._record_counts[measurement] += num_records

            # Check if we should flush this measurement
            if self._record_counts[measurement] >= self._batch_size:
                self._flush_measurement(measurement)

            # Check time-based flush
            if time.monotonic() - self._last_flush_time >= self._flush_interval:
                self._flush_all()

    def flush(self) -> None:
        """Manually flush all buffered data."""
        with self._lock:
            self._flush_all()

    def _flush_measurement(self, measurement: str) -> None:
        """Flush a single measurement's buffer. Must hold lock."""
        if measurement not in self._buffers or not self._buffers[measurement]:
            return

        # Merge all columnar batches into one
        merged = self._merge_columnar(self._buffers[measurement])

        # Clear buffer
        self._buffers[measurement] = []
        self._record_counts[measurement] = 0

        # Write to Arc (release lock during I/O)
        self._lock.release()
        try:
            self._client.write_columnar(measurement, merged)
        finally:
            self._lock.acquire()

        self._last_flush_time = time.monotonic()

    def _flush_all(self) -> None:
        """Flush all measurements. Must hold lock."""
        measurements = list(self._buffers.keys())
        for measurement in measurements:
            if self._buffers[measurement]:
                self._flush_measurement(measurement)

    def _merge_columnar(
        self, batches: list[dict[str, list[Any]]]
    ) -> dict[str, list[Any]]:
        """Merge multiple columnar batches into one."""
        if not batches:
            return {}

        if len(batches) == 1:
            return batches[0]

        # Get all column names
        all_columns: set[str] = set()
        for batch in batches:
            all_columns.update(batch.keys())

        # Merge each column
        merged: dict[str, list[Any]] = {}
        for col_name in all_columns:
            merged[col_name] = []
            for batch in batches:
                if col_name in batch:
                    merged[col_name].extend(batch[col_name])

        return merged

    def close(self) -> None:
        """Close the buffer and flush remaining data."""
        if self._closed:
            return

        with self._lock:
            self._flush_all()
            self._closed = True

    def __enter__(self) -> BufferedWriter:
        """Enter context manager."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit context manager, flushing remaining data."""
        self.close()

    @property
    def pending_count(self) -> int:
        """Get the total number of pending records across all measurements."""
        with self._lock:
            return sum(self._record_counts.values())

    @property
    def pending_measurements(self) -> dict[str, int]:
        """Get pending record counts by measurement."""
        with self._lock:
            return dict(self._record_counts)
