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
        self._closing = False
        self._flush_error: BaseException | None = None

        # Background timer that flushes on flush_interval even when no writes
        # arrive. Started on __enter__ and torn down on close().
        self._stop_event = threading.Event()
        self._flush_thread: threading.Thread | None = None

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
            self._raise_if_failed()
            if self._closed or self._closing:
                raise RuntimeError("Buffered writer is closed")
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
            self._raise_if_failed()
            if self._closed or self._closing:
                raise RuntimeError("Buffered writer is closed")
            self._buffers[measurement].append(columns)
            self._record_counts[measurement] += num_records

            # Check if we should flush this measurement
            if self._record_counts[measurement] >= self._batch_size:
                self._flush_measurement(measurement)

            # Check time-based flush
            if time.monotonic() - self._last_flush_time >= self._flush_interval:
                self._flush_all()

    def flush(self) -> None:
        """Flush all buffered data, waiting for any in-flight write.

        A failed write leaves its data pending and makes the buffer terminal:
        subsequent flush/close calls raise that failure without retrying data
        whose server outcome may be unknown. Producers wait while I/O runs.
        """
        with self._lock:
            self._flush_all()

    def _start_flush_thread(self) -> None:
        """Start the background flush timer (idempotent)."""
        if self._flush_thread is not None or self._flush_interval <= 0:
            return
        self._stop_event.clear()
        self._flush_thread = threading.Thread(
            target=self._flush_loop, name="arc-buffered-flush", daemon=True
        )
        self._flush_thread.start()

    def _flush_loop(self) -> None:
        """Periodically flush measurements whose data has aged past the interval."""
        try:
            while not self._stop_event.wait(self._flush_interval):
                with self._lock:
                    if self._closed or self._stop_event.is_set():
                        return
                    if (
                        self._buffers
                        and time.monotonic() - self._last_flush_time >= self._flush_interval
                    ):
                        self._flush_all()
        except Exception as error:
            # Keep the error observable on foreground operations; never retry
            # an uncertain write automatically from the background thread.
            with self._lock:
                self._flush_error = error

    def _raise_if_failed(self) -> None:
        if self._flush_error is not None:
            raise self._flush_error

    def _flush_measurement(self, measurement: str) -> None:
        """Flush a single measurement's buffer. Must hold lock."""
        if measurement not in self._buffers or not self._buffers[measurement]:
            return

        # Merge all columnar batches into one
        merged = self._merge_columnar(self._buffers[measurement])

        # Keep ownership and data until the write is acknowledged. Holding
        # the lock across I/O serializes timer, manual, and close flushes.
        try:
            self._client.write_columnar(measurement, merged)
        except BaseException as error:
            self._flush_error = error
            raise
        self._buffers[measurement] = []
        self._record_counts[measurement] = 0

        self._last_flush_time = time.monotonic()

    def _flush_all(self) -> None:
        """Flush all measurements. Must hold lock."""
        self._raise_if_failed()
        measurements = list(self._buffers.keys())
        for measurement in measurements:
            if self._buffers[measurement]:
                self._flush_measurement(measurement)

    def _merge_columnar(self, batches: list[dict[str, list[Any]]]) -> dict[str, list[Any]]:
        """Merge multiple columnar batches into one.

        When batches have different column sets (sparse columns), missing
        positions are filled with None so all columns have equal length.
        """
        if not batches:
            return {}

        if len(batches) == 1:
            return batches[0]

        # Get all column names
        all_columns: set[str] = set()
        for batch in batches:
            all_columns.update(batch.keys())

        # Merge each column, padding missing columns with None
        merged: dict[str, list[Any]] = {}
        for col_name in all_columns:
            merged[col_name] = []
            for batch in batches:
                if col_name in batch:
                    merged[col_name].extend(batch[col_name])
                else:
                    # Column missing from this batch — pad with None
                    batch_len = len(batch.get("time", next(iter(batch.values()))))
                    merged[col_name].extend([None] * batch_len)

        return merged

    def close(self) -> None:
        """Close the buffer and flush remaining data."""
        if self._closed:
            return

        self._closing = True
        # Stop new timer work and wait for any active write to finish.
        self._stop_event.set()
        thread = self._flush_thread
        if thread is not None:
            thread.join()
            self._flush_thread = None

        with self._lock:
            self._flush_all()
            self._closed = True

    def __enter__(self) -> BufferedWriter:
        """Enter context manager."""
        self._start_flush_thread()
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
