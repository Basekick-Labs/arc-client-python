"""Async buffered writer for automatic batching of writes."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from arc_client.ingestion.async_writer import AsyncWriteClient


class AsyncBufferedWriter:
    """Async buffered writer that batches records for optimal throughput.

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
            self._buffers[measurement].append(columns)
            self._record_counts[measurement] += 1

            if self._record_counts[measurement] >= self._batch_size:
                await self._flush_measurement_unlocked(measurement)

            if time.monotonic() - self._last_flush_time >= self._flush_interval:
                await self._flush_all_unlocked()

    async def write_columnar(
        self,
        measurement: str,
        columns: dict[str, list[Any]],
    ) -> None:
        """Write columnar data to the buffer."""
        if not columns:
            return

        num_records = len(next(iter(columns.values())))

        async with self._lock:
            self._buffers[measurement].append(columns)
            self._record_counts[measurement] += num_records

            if self._record_counts[measurement] >= self._batch_size:
                await self._flush_measurement_unlocked(measurement)

            if time.monotonic() - self._last_flush_time >= self._flush_interval:
                await self._flush_all_unlocked()

    async def flush(self) -> None:
        """Manually flush all buffered data."""
        async with self._lock:
            await self._flush_all_unlocked()

    async def _flush_measurement_unlocked(self, measurement: str) -> None:
        """Flush a single measurement. Must hold lock."""
        if measurement not in self._buffers or not self._buffers[measurement]:
            return

        merged = self._merge_columnar(self._buffers[measurement])
        self._buffers[measurement] = []
        self._record_counts[measurement] = 0

        # Release lock during I/O
        self._lock.release()
        try:
            await self._client.write_columnar(measurement, merged)
        finally:
            await self._lock.acquire()

        self._last_flush_time = time.monotonic()

    async def _flush_all_unlocked(self) -> None:
        """Flush all measurements. Must hold lock."""
        measurements = list(self._buffers.keys())
        for measurement in measurements:
            if self._buffers[measurement]:
                await self._flush_measurement_unlocked(measurement)

    def _merge_columnar(self, batches: list[dict[str, list[Any]]]) -> dict[str, list[Any]]:
        """Merge multiple columnar batches into one."""
        if not batches:
            return {}

        if len(batches) == 1:
            return batches[0]

        all_columns: set[str] = set()
        for batch in batches:
            all_columns.update(batch.keys())

        merged: dict[str, list[Any]] = {}
        for col_name in all_columns:
            merged[col_name] = []
            for batch in batches:
                if col_name in batch:
                    merged[col_name].extend(batch[col_name])

        return merged

    async def close(self) -> None:
        """Close the buffer and flush remaining data."""
        if self._closed:
            return

        async with self._lock:
            await self._flush_all_unlocked()
            self._closed = True

    async def __aenter__(self) -> AsyncBufferedWriter:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    @property
    def pending_count(self) -> int:
        """Get the total number of pending records."""
        return sum(self._record_counts.values())
