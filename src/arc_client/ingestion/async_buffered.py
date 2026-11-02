"""Async buffered writer for automatic batching of writes."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Optional

from arc_client.exceptions import ArcPartialWriteError
from arc_client.ingestion.msgpack import validate_tag_keys

if TYPE_CHECKING:
    from arc_client.ingestion.async_writer import AsyncWriteClient


class AsyncBufferedWriter:
    """Async buffered writer that batches records for optimal throughput.

    Failure semantics:

    * A write Arc answers with a partial write (``ArcPartialWriteError``,
      HTTP 400 "partial write: N points dropped") has been applied as far as
      Arc could apply it. The batch is **discarded**, never resent (a resend
      would duplicate every stored point), and the error is raised once: by
      the flush that sent it, or, for a background flush, by the next
      ``write``/``write_columnar`` call (before it buffers anything) or the
      next ``flush``/``close`` (after it has flushed the remaining data).
      The writer stays usable.
    * Any other failure keeps the batch and makes the writer terminal: later
      calls raise that failure and nothing is resent, because the server-side
      outcome may be unknown.

    Tags passed in ``write(record)["tags"]`` and ``write_columnar(...,
    tag_columns=...)`` are sent as the batch's ``tag_keys``.

    Example:
        >>> async with client.write.buffered(batch_size=10000) as buffer:
        ...     for record in records:
        ...         await buffer.write(record)
        ...     # Auto-flushes on exit
    """

    def __init__(
        self,
        write_client: AsyncWriteClient,
        batch_size: int = 10000,
        flush_interval: float = 5.0,
    ) -> None:
        self._client = write_client
        self._batch_size = batch_size
        self._flush_interval = flush_interval

        self._buffers: dict[str, list[dict[str, list[Any]]]] = defaultdict(list)
        self._record_counts: dict[str, int] = defaultdict(int)

        self._last_flush_time = time.monotonic()
        self._lock = asyncio.Lock()
        self._closed = False
        self._closing = False
        self._flush_error: BaseException | None = None
        # A partial write from a background flush, reported once by the next
        # foreground call (see the class docstring).
        self._partial_error: Optional[ArcPartialWriteError] = None
        # Columns sent as tag_keys, per measurement, in first-seen order.
        self._tag_keys: dict[str, dict[str, None]] = defaultdict(dict)

        # Background task that flushes on flush_interval even when idle.
        self._flush_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    async def write(self, record: dict[str, Any]) -> None:
        """Write a single record to the buffer."""
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

        columns: dict[str, list[Any]] = {"time": [timestamp]}
        for key, value in fields.items():
            columns[key] = [value]
        for key, value in tags.items():
            columns[key] = [value]

        async with self._lock:
            self._raise_if_failed()
            if self._closed or self._closing:
                raise RuntimeError("Buffered writer is closed")
            self._buffers[measurement].append(columns)
            self._record_counts[measurement] += 1
            for key in tags:
                self._tag_keys[measurement][key] = None

            if self._record_counts[measurement] >= self._batch_size:
                await self._flush_measurement_unlocked(measurement)

            if time.monotonic() - self._last_flush_time >= self._flush_interval:
                await self._flush_all_unlocked()

    async def write_columnar(
        self,
        measurement: str,
        columns: dict[str, list[Any]],
        tag_columns: Optional[list[str]] = None,
    ) -> None:
        """Write columnar data to the buffer."""
        if not columns:
            return
        validate_tag_keys(tag_columns, columns)

        num_records = len(next(iter(columns.values())))

        async with self._lock:
            self._raise_if_failed()
            if self._closed or self._closing:
                raise RuntimeError("Buffered writer is closed")
            self._buffers[measurement].append(columns)
            self._record_counts[measurement] += num_records
            for key in tag_columns or ():
                self._tag_keys[measurement][key] = None

            if self._record_counts[measurement] >= self._batch_size:
                await self._flush_measurement_unlocked(measurement)

            if time.monotonic() - self._last_flush_time >= self._flush_interval:
                await self._flush_all_unlocked()

    async def flush(self) -> None:
        """Flush all buffered data, waiting for any in-flight write.

        A failed write leaves its data pending and makes the buffer terminal:
        subsequent flush/close calls raise that failure without retrying data
        whose server outcome may be unknown. Producers wait while I/O runs.

        A partial write (``ArcPartialWriteError``) is the exception: its batch
        is discarded and the error raised once, after the other measurements
        were flushed.
        """
        async with self._lock:
            await self._flush_all_unlocked()
            self._raise_pending_partial()

    async def _flush_loop(self) -> None:
        """Periodically flush measurements whose data has aged past the interval."""
        try:
            while not self._stop_event.is_set():
                try:
                    await asyncio.wait_for(self._stop_event.wait(), self._flush_interval)
                    return
                except asyncio.TimeoutError:
                    pass
                async with self._lock:
                    if self._closed or self._stop_event.is_set():
                        return
                    if (
                        self._buffers
                        and time.monotonic() - self._last_flush_time >= self._flush_interval
                    ):
                        try:
                            await self._flush_all_unlocked()
                        except ArcPartialWriteError as error:
                            # Batch already discarded; report it once from the
                            # foreground and keep the timer running.
                            self._partial_error = error
        except Exception as error:
            # Foreground flush/close will surface this error. Do not retry an
            # uncertain outcome, or leave a task exception unobserved.
            self._flush_error = error

    def _raise_if_failed(self) -> None:
        """Raise a terminal failure, then a pending partial write (once)."""
        if self._flush_error is not None:
            raise self._flush_error
        self._raise_pending_partial()

    def _raise_pending_partial(self) -> None:
        error, self._partial_error = self._partial_error, None
        if error is not None:
            raise error

    def _discard(self, measurement: str) -> None:
        """Drop a measurement's buffered batch. Must hold lock."""
        self._buffers[measurement] = []
        self._record_counts[measurement] = 0
        self._tag_keys.pop(measurement, None)

    async def _flush_measurement_unlocked(self, measurement: str) -> None:
        """Flush a single measurement. Must hold lock."""
        if measurement not in self._buffers or not self._buffers[measurement]:
            return

        merged = self._merge_columnar(self._buffers[measurement])
        # Retain data and the lock until acknowledgement. Concurrent timer,
        # manual, and close flushes must never send the same batch twice.
        try:
            tags = list(self._tag_keys.get(measurement, ()))
            if tags:
                await self._client.write_columnar(measurement, merged, tag_columns=tags)
            else:
                await self._client.write_columnar(measurement, merged)
        except ArcPartialWriteError:
            # Arc stored what it could; resending would duplicate it.
            self._discard(measurement)
            self._last_flush_time = time.monotonic()
            raise
        except BaseException as error:
            self._flush_error = error
            raise
        self._discard(measurement)

        self._last_flush_time = time.monotonic()

    async def _flush_all_unlocked(self) -> None:
        """Flush all measurements. Must hold lock.

        A partial write does not stop the other measurements from flushing; the
        first one is raised after they have been sent.
        """
        if self._flush_error is not None:
            raise self._flush_error
        partial: Optional[ArcPartialWriteError] = None
        measurements = list(self._buffers.keys())
        for measurement in measurements:
            if self._buffers[measurement]:
                try:
                    await self._flush_measurement_unlocked(measurement)
                except ArcPartialWriteError as error:
                    partial = partial or error
        if partial is not None:
            raise partial

    def _merge_columnar(self, batches: list[dict[str, list[Any]]]) -> dict[str, list[Any]]:
        """Merge multiple columnar batches into one.

        When batches have different column sets (sparse columns), missing
        positions are filled with None so all columns have equal length.
        """
        if not batches:
            return {}

        if len(batches) == 1:
            return batches[0]

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

    async def close(self) -> None:
        """Close the buffer and flush remaining data."""
        if self._closed:
            return

        self._closing = True
        # Wake an idle timer, but let any in-flight write finish. Shielding
        # also prevents cancellation of close's caller from cancelling I/O.
        self._stop_event.set()
        task = self._flush_task
        if task is not None:
            await asyncio.shield(task)
            self._flush_task = None

        async with self._lock:
            try:
                await self._flush_all_unlocked()
            except ArcPartialWriteError:
                # Everything was flushed or discarded; the writer is done.
                self._closed = True
                raise
            self._closed = True
            self._raise_pending_partial()

    async def __aenter__(self) -> AsyncBufferedWriter:
        if self._flush_task is None and self._flush_interval > 0:
            self._flush_task = asyncio.ensure_future(self._flush_loop())
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    @property
    def pending_count(self) -> int:
        """Get the total number of pending records."""
        return sum(self._record_counts.values())
