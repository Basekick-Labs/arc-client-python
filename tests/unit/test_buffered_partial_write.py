"""A partial write is applied server-side: buffered writers discard it and report it once."""

from __future__ import annotations

import asyncio
import threading
from typing import Any, Optional

import pytest

from arc_client import ArcPartialWriteError
from arc_client.exceptions import ArcIngestRefusedError, ArcServerError, ArcValidationError
from arc_client.ingestion.async_buffered import AsyncBufferedWriter
from arc_client.ingestion.buffered import BufferedWriter


def partial() -> ArcPartialWriteError:
    return ArcPartialWriteError("partial write: 1 points dropped", dropped=1, others_written=True)


class FakeWriter:
    """Records every write; fails the writes listed in ``fail`` (by call index)."""

    def __init__(self, fail: Optional[dict[int, BaseException]] = None) -> None:
        self.calls: list[tuple[str, dict[str, list[Any]], Optional[list[str]]]] = []
        self.fail = fail or {}
        self.sent = threading.Event()

    def _record(self, measurement: str, columns: Any, tag_columns: Any) -> None:
        index = len(self.calls)
        self.calls.append((measurement, columns, tag_columns))
        self.sent.set()
        if index in self.fail:
            raise self.fail[index]

    def write_columnar(
        self, measurement: str, columns: Any, tag_columns: Optional[list[str]] = None
    ) -> None:
        self._record(measurement, columns, tag_columns)


class AsyncFakeWriter(FakeWriter):
    async def write_columnar(  # type: ignore[override]
        self, measurement: str, columns: Any, tag_columns: Optional[list[str]] = None
    ) -> None:
        self._record(measurement, columns, tag_columns)


ROW = {"measurement": "cpu", "fields": {"v": 1.0}, "timestamp": 1}


def test_sync_partial_write_discards_batch_and_raises_once() -> None:
    fake = FakeWriter(fail={0: partial()})
    buffer = BufferedWriter(fake, batch_size=100, flush_interval=3600)  # type: ignore[arg-type]
    buffer.write(ROW)

    with pytest.raises(ArcPartialWriteError):
        buffer.flush()
    assert buffer.pending_count == 0

    # Not terminal and nothing resent: the next flush sends only new data.
    buffer.flush()
    buffer.write({**ROW, "timestamp": 2})
    buffer.close()
    assert [c[1]["time"] for c in fake.calls] == [[1], [2]]


def test_sync_partial_write_does_not_block_other_measurements_on_close() -> None:
    fake = FakeWriter(fail={0: partial()})
    buffer = BufferedWriter(fake, batch_size=100, flush_interval=3600)  # type: ignore[arg-type]
    buffer.write(ROW)
    buffer.write({**ROW, "measurement": "mem"})

    with pytest.raises(ArcPartialWriteError):
        buffer.close()
    assert sorted(c[0] for c in fake.calls) == ["cpu", "mem"]
    assert buffer.pending_count == 0
    with pytest.raises(RuntimeError, match="closed"):
        buffer.write(ROW)


def test_sync_other_failure_stays_terminal() -> None:
    error = ArcServerError("boom", status_code=500)
    fake = FakeWriter(fail={0: error})
    buffer = BufferedWriter(fake, batch_size=100, flush_interval=3600)  # type: ignore[arg-type]
    buffer.write(ROW)
    for _ in range(2):
        with pytest.raises(ArcServerError):
            buffer.flush()
    assert buffer.pending_count == 1
    assert len(fake.calls) == 1


def test_sync_background_partial_is_reported_once_by_next_call() -> None:
    fake = FakeWriter(fail={0: partial()})
    buffer = BufferedWriter(fake, batch_size=100, flush_interval=0.05)  # type: ignore[arg-type]
    with buffer:
        buffer._last_flush_time -= 1  # make the first row old enough for the timer
        with buffer._lock:
            buffer._buffers["cpu"].append({"time": [1], "v": [1.0]})
            buffer._record_counts["cpu"] += 1
        assert fake.sent.wait(5)
        # Wait until the timer has recorded the partial write.
        for _ in range(100):
            with buffer._lock:
                if buffer._partial_errors:
                    break
            threading.Event().wait(0.01)
        with pytest.raises(ArcPartialWriteError):
            buffer.write({**ROW, "timestamp": 2})
        buffer.write({**ROW, "timestamp": 3})  # reported once only
    assert [c[1]["time"] for c in fake.calls] == [[1], [3]]


def test_sync_tags_are_sent_and_reset_per_batch() -> None:
    fake = FakeWriter()
    buffer = BufferedWriter(fake, batch_size=100, flush_interval=3600)  # type: ignore[arg-type]
    buffer.write({**ROW, "tags": {"host": "a"}})
    buffer.write_columnar("cpu", {"time": [2], "region": ["eu"], "v": [2.0]}, ["region"])
    buffer.flush()
    buffer.write(ROW)
    buffer.close()
    assert fake.calls[0][2] == ["host", "region"]
    assert fake.calls[0][1]["host"] == ["a", None]
    assert fake.calls[1][2] is None


async def test_async_partial_write_discards_batch_and_raises_once() -> None:
    fake = AsyncFakeWriter(fail={0: partial()})
    buffer = AsyncBufferedWriter(fake, batch_size=100, flush_interval=3600)  # type: ignore[arg-type]
    await buffer.write(ROW)

    with pytest.raises(ArcPartialWriteError):
        await buffer.flush()
    assert buffer.pending_count == 0

    await buffer.flush()
    await buffer.write({**ROW, "timestamp": 2})
    await buffer.close()
    assert [c[1]["time"] for c in fake.calls] == [[1], [2]]


async def test_async_partial_write_does_not_block_other_measurements_on_close() -> None:
    fake = AsyncFakeWriter(fail={0: partial()})
    buffer = AsyncBufferedWriter(fake, batch_size=100, flush_interval=3600)  # type: ignore[arg-type]
    await buffer.write(ROW)
    await buffer.write({**ROW, "measurement": "mem"})

    with pytest.raises(ArcPartialWriteError):
        await buffer.close()
    assert sorted(c[0] for c in fake.calls) == ["cpu", "mem"]
    assert buffer.pending_count == 0


async def test_async_background_partial_is_reported_once_by_next_call() -> None:
    fake = AsyncFakeWriter(fail={0: partial()})
    buffer = AsyncBufferedWriter(fake, batch_size=100, flush_interval=0.05)  # type: ignore[arg-type]
    async with buffer:
        buffer._last_flush_time -= 1
        buffer._buffers["cpu"].append({"time": [1], "v": [1.0]})
        buffer._record_counts["cpu"] += 1
        for _ in range(200):
            if buffer._partial_errors:
                break
            await asyncio.sleep(0.01)
        with pytest.raises(ArcPartialWriteError):
            await buffer.write({**ROW, "timestamp": 2})
        await buffer.write({**ROW, "timestamp": 3})
    assert [c[1]["time"] for c in fake.calls] == [[1], [3]]


# --- Review fixes: no lost partials, retry-safe refusals, tag validation -----


def refused(retry_safe: bool = True) -> ArcIngestRefusedError:
    return ArcIngestRefusedError("refused", reason="fenced", retry_safe=retry_safe)


def make(async_mode: bool, fail: dict[int, BaseException]) -> tuple[Any, Any]:
    if async_mode:
        fake: Any = AsyncFakeWriter(fail)
        return fake, AsyncBufferedWriter(fake, batch_size=100, flush_interval=3600)
    fake = FakeWriter(fail)
    return fake, BufferedWriter(fake, batch_size=100, flush_interval=3600)


async def run(result: Any) -> Any:
    return await result if asyncio.iscoroutine(result) else result


@pytest.mark.parametrize("async_mode", [False, True])
async def test_partial_survives_a_later_failure_in_the_same_flush(async_mode: bool) -> None:
    # cpu: partial write (discarded); mem: server error (terminal).
    fake, buffer = make(async_mode, {0: partial(), 1: ArcServerError("boom", status_code=500)})
    await run(buffer.write(ROW))
    await run(buffer.write({**ROW, "measurement": "mem"}))

    with pytest.raises(ArcPartialWriteError):
        await run(buffer.flush())
    with pytest.raises(ArcServerError):
        await run(buffer.flush())


@pytest.mark.parametrize("async_mode", [False, True])
async def test_several_partials_are_folded_into_one(async_mode: bool) -> None:
    fake, buffer = make(async_mode, {0: partial(), 1: partial()})
    await run(buffer.write(ROW))
    await run(buffer.write({**ROW, "measurement": "mem"}))

    with pytest.raises(ArcPartialWriteError) as info:
        await run(buffer.flush())
    assert info.value.dropped == 2
    await run(buffer.flush())  # reported once


@pytest.mark.parametrize("async_mode", [False, True])
async def test_retry_safe_refusal_keeps_batch_and_writer(async_mode: bool) -> None:
    fake, buffer = make(async_mode, {0: refused()})
    await run(buffer.write(ROW))

    with pytest.raises(ArcIngestRefusedError):
        await run(buffer.flush())
    assert buffer.pending_count == 1

    await run(buffer.flush())  # resent, accepted
    assert buffer.pending_count == 0
    await run(buffer.close())
    assert [c[1]["time"] for c in fake.calls] == [[1], [1]]


@pytest.mark.parametrize("async_mode", [False, True])
async def test_unsafe_refusal_is_terminal(async_mode: bool) -> None:
    fake, buffer = make(async_mode, {0: refused(retry_safe=False)})
    await run(buffer.write(ROW))
    for _ in range(2):
        with pytest.raises(ArcIngestRefusedError):
            await run(buffer.flush())
    assert len(fake.calls) == 1


@pytest.mark.parametrize("async_mode", [False, True])
async def test_bad_tag_is_rejected_before_buffering(async_mode: bool) -> None:
    fake, buffer = make(async_mode, {})
    with pytest.raises(ArcValidationError):
        await run(buffer.write({**ROW, "tags": {"time": "x"}}))
    assert buffer.pending_count == 0

    await run(buffer.write(ROW))  # writer is still usable
    await run(buffer.close())
    assert len(fake.calls) == 1
