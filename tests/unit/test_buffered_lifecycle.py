"""Deterministic ownership, shutdown, and failure checks for buffered writes."""

import asyncio
import threading

import pytest

from arc_client.ingestion.async_buffered import AsyncBufferedWriter
from arc_client.ingestion.buffered import BufferedWriter


class SyncWriter:
    def __init__(self, error=None):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls = []
        self.committed = []
        self.error = error

    def write_columnar(self, measurement, columns):
        self.calls.append((measurement, columns))
        self.entered.set()
        assert self.release.wait(3), "test did not release write"
        if self.error is not None:
            raise self.error
        self.committed.append((measurement, columns))


class AsyncWriter:
    def __init__(self, error=None):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = []
        self.committed = []
        self.error = error
        self.cancelled = False

    async def write_columnar(self, measurement, columns):
        self.calls.append((measurement, columns))
        self.entered.set()
        try:
            await asyncio.wait_for(self.release.wait(), 3)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        if self.error is not None:
            raise self.error
        self.committed.append((measurement, columns))


async def next_turn():
    """Let already scheduled tasks reach their next suspension without sleeping."""
    reached = asyncio.Event()
    asyncio.get_running_loop().call_soon(reached.set)
    await reached.wait()


async def start_async_timer(buffer):
    buffer._last_flush_time = 0
    buffer._flush_interval = 0.001
    await buffer.__aenter__()


def start_sync_timer(buffer):
    buffer._last_flush_time = 0
    buffer._flush_interval = 0.001
    buffer.__enter__()


async def test_async_close_waits_for_active_timer_write_without_cancelling():
    writer = AsyncWriter()
    buffer = AsyncBufferedWriter(writer, flush_interval=60)
    await buffer.write_columnar("cpu", {"time": [1], "value": [2]})
    await start_async_timer(buffer)
    await asyncio.wait_for(writer.entered.wait(), 3)
    closing = asyncio.create_task(buffer.close())
    await next_turn()
    writer.release.set()
    await asyncio.wait_for(closing, 3)
    assert writer.cancelled is False
    assert len(writer.committed) == len(writer.calls) == 1
    assert buffer.pending_count == 0


async def test_async_manual_flush_and_close_serialize_with_timer():
    writer = AsyncWriter()
    buffer = AsyncBufferedWriter(writer, flush_interval=60)
    await buffer.write_columnar("cpu", {"time": [1]})
    await start_async_timer(buffer)
    await asyncio.wait_for(writer.entered.wait(), 3)
    flushing = asyncio.create_task(buffer.flush())
    closing = asyncio.create_task(buffer.close())
    await next_turn()
    writer.release.set()
    await asyncio.wait_for(asyncio.gather(flushing, closing), 3)
    assert writer.cancelled is False
    assert len(writer.committed) == len(writer.calls) == 1


async def test_async_concurrent_closes_do_not_duplicate_inflight_write():
    writer = AsyncWriter()
    buffer = AsyncBufferedWriter(writer, flush_interval=60)
    await buffer.write_columnar("cpu", {"time": [1]})
    await start_async_timer(buffer)
    await asyncio.wait_for(writer.entered.wait(), 3)
    first = asyncio.create_task(buffer.close())
    second = asyncio.create_task(buffer.close())
    await next_turn()
    writer.release.set()
    await asyncio.wait_for(asyncio.gather(first, second), 3)
    assert len(writer.committed) == len(writer.calls) == 1


async def test_async_cancelling_close_propagates_without_cancelling_timer_io():
    writer = AsyncWriter()
    buffer = AsyncBufferedWriter(writer, flush_interval=60)
    await buffer.write_columnar("cpu", {"time": [1]})
    await start_async_timer(buffer)
    await asyncio.wait_for(writer.entered.wait(), 3)
    closing = asyncio.create_task(buffer.close())
    await next_turn()
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert writer.cancelled is False
    assert buffer.pending_count == 1
    writer.release.set()
    await asyncio.wait_for(buffer.close(), 3)
    assert len(writer.committed) == len(writer.calls) == 1


async def test_async_cancelling_manual_flush_retains_data_and_propagates():
    writer = AsyncWriter()
    buffer = AsyncBufferedWriter(writer, flush_interval=60)
    await buffer.write_columnar("cpu", {"time": [1]})
    flushing = asyncio.create_task(buffer.flush())
    await asyncio.wait_for(writer.entered.wait(), 3)
    flushing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await flushing
    assert buffer.pending_count == 1
    with pytest.raises(asyncio.CancelledError):
        await buffer.close()
    assert len(writer.calls) == 1


def test_sync_close_and_manual_flush_wait_for_active_timer(monkeypatch):
    writer = SyncWriter()
    buffer = BufferedWriter(writer, flush_interval=60)
    buffer.write_columnar("cpu", {"time": [1]})
    start_sync_timer(buffer)
    assert writer.entered.wait(3)
    joining = threading.Event()
    timer_join = buffer._flush_thread.join

    def observed_join(*args, **kwargs):
        joining.set()
        timer_join(*args, **kwargs)

    monkeypatch.setattr(buffer._flush_thread, "join", observed_join)
    closing = threading.Thread(target=buffer.close)
    closing.start()
    assert joining.wait(3)
    flushing = threading.Thread(target=buffer.flush)
    flushing.start()
    writer.release.set()
    closing.join(3)
    flushing.join(3)
    assert not closing.is_alive() and not flushing.is_alive()
    assert len(writer.committed) == len(writer.calls) == 1
    assert buffer.pending_count == 0


async def test_async_idle_close_wakes_timer_immediately():
    buffer = AsyncBufferedWriter(AsyncWriter(), flush_interval=3600)
    await buffer.__aenter__()
    task = buffer._flush_task
    await next_turn()
    await asyncio.wait_for(buffer.close(), 1)
    assert task.done()


@pytest.mark.parametrize("background", [False, True])
async def test_async_failure_retains_all_unsent_data_and_close_surfaces_error(background):
    error = RuntimeError("write outcome unknown")
    writer = AsyncWriter(error)
    buffer = AsyncBufferedWriter(writer, flush_interval=60)
    await buffer.write_columnar("cpu", {"time": [1]})
    await buffer.write_columnar("mem", {"time": [2]})
    writer.release.set()
    if background:
        await start_async_timer(buffer)
        await asyncio.wait_for(writer.entered.wait(), 3)
        # Observe task completion, allowing the implementation to record the error.
        await asyncio.gather(buffer._flush_task, return_exceptions=True)
    else:
        with pytest.raises(RuntimeError, match="outcome unknown"):
            await buffer.flush()
    assert buffer.pending_count == 2
    for operation in (buffer.close, buffer.flush, buffer.close):
        with pytest.raises(RuntimeError) as caught:
            await operation()
        assert caught.value is error
    assert len(writer.calls) == 1


@pytest.mark.parametrize("background", [False, True])
def test_sync_failure_retains_all_unsent_data_and_close_surfaces_error(background):
    error = RuntimeError("write outcome unknown")
    writer = SyncWriter(error)
    buffer = BufferedWriter(writer, flush_interval=60)
    buffer.write_columnar("cpu", {"time": [1]})
    buffer.write_columnar("mem", {"time": [2]})
    writer.release.set()
    if background:
        start_sync_timer(buffer)
        assert writer.entered.wait(3)
        buffer._flush_thread.join(3)
        assert not buffer._flush_thread.is_alive()
    else:
        with pytest.raises(RuntimeError, match="outcome unknown"):
            buffer.flush()
    assert buffer.pending_count == 2
    for operation in (buffer.close, buffer.flush, buffer.close):
        with pytest.raises(RuntimeError) as caught:
            operation()
        assert caught.value is error
    assert len(writer.calls) == 1


async def test_async_successful_batches_clear_but_failed_batch_remains():
    class FailSecond:
        calls = []

        async def write_columnar(self, measurement, columns):
            self.calls.append(measurement)
            if measurement == "mem":
                raise RuntimeError("second batch failed")

    writer = FailSecond()
    buffer = AsyncBufferedWriter(writer, flush_interval=60)
    await buffer.write_columnar("cpu", {"time": [1]})
    await buffer.write_columnar("mem", {"time": [2]})
    with pytest.raises(RuntimeError):
        await buffer.flush()
    assert writer.calls == ["cpu", "mem"]
    assert buffer.pending_count == 1


def test_sync_successful_batches_clear_but_failed_batch_remains():
    class FailSecond:
        calls = []

        def write_columnar(self, measurement, columns):
            self.calls.append(measurement)
            if measurement == "mem":
                raise RuntimeError("second batch failed")

    writer = FailSecond()
    buffer = BufferedWriter(writer, flush_interval=60)
    buffer.write_columnar("cpu", {"time": [1]})
    buffer.write_columnar("mem", {"time": [2]})
    with pytest.raises(RuntimeError):
        buffer.flush()
    assert writer.calls == ["cpu", "mem"]
    assert buffer.pending_count == 1


async def test_async_sparse_padding_survives_lifecycle_changes():
    writer = AsyncWriter()
    writer.release.set()
    buffer = AsyncBufferedWriter(writer, flush_interval=60)
    await buffer.write_columnar("cpu", {"time": [1], "a": [10]})
    await buffer.write_columnar("cpu", {"time": [2], "b": [20]})
    await buffer.close()
    assert writer.committed == [("cpu", {"time": [1, 2], "a": [10, None], "b": [None, 20]})]


def test_sync_sparse_padding_survives_lifecycle_changes():
    writer = SyncWriter()
    writer.release.set()
    buffer = BufferedWriter(writer, flush_interval=60)
    buffer.write_columnar("cpu", {"time": [1], "a": [10]})
    buffer.write_columnar("cpu", {"time": [2], "b": [20]})
    buffer.close()
    assert writer.committed == [("cpu", {"time": [1, 2], "a": [10, None], "b": [None, 20]})]
