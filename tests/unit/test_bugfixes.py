"""Regression tests for bugs fixed in 0.2.0."""

from __future__ import annotations

import time

import pytest

from arc_client.ingestion.buffered import BufferedWriter


class _FakeWriteClient:
    def __init__(self) -> None:
        self.writes: list[tuple[str, dict]] = []

    def write_columnar(self, measurement: str, columns: dict) -> None:
        self.writes.append((measurement, columns))


def test_buffered_background_flush_fires_when_idle() -> None:
    """The buffered writer flushes on flush_interval even without new writes."""
    fake = _FakeWriteClient()
    with BufferedWriter(fake, batch_size=10_000, flush_interval=0.2) as buf:
        buf.write_columnar("cpu", {"time": [1], "v": [1.0]})
        # Stay idle past the interval; the background timer must flush.
        deadline = time.monotonic() + 2.0
        while not fake.writes and time.monotonic() < deadline:
            time.sleep(0.05)
        assert fake.writes, "background timer should have flushed the idle buffer"


def test_buffered_flushes_remaining_on_close() -> None:
    fake = _FakeWriteClient()
    with BufferedWriter(fake, batch_size=10_000, flush_interval=60.0) as buf:
        buf.write_columnar("cpu", {"time": [1], "v": [1.0]})
    # After exit, the buffer must be flushed exactly once.
    assert len(fake.writes) == 1


EXPECTED_MICROS = 1633046400000000  # 2021-10-01T00:00:00Z in microseconds


@pytest.mark.parametrize("unit", ["ns", "us", "ms", "s"])
@pytest.mark.parametrize("tz", [None, "UTC"])
def test_pandas_datetime_all_resolutions(unit: str, tz: str | None) -> None:
    """Every pandas datetime resolution (tz-naive and tz-aware) scales to micros.

    Regression for a bug where tz-naive datetime64[us]/[ms]/[s] columns were
    mis-scaled because numpy datetime64 has no ``.unit`` attribute (only pandas'
    tz-aware DatetimeTZDtype does), so the code fell back to a nanosecond
    assumption and produced timestamps 1000x+ too small (landing in ~1970).
    """
    pd = pytest.importorskip("pandas")
    from arc_client.ingestion.msgpack import dataframe_to_columnar

    ts = pd.to_datetime(["2021-10-01T00:00:00"])
    if tz is not None:
        ts = ts.tz_localize(tz)
    ts = ts.as_unit(unit)
    df = pd.DataFrame({"time": ts, "v": [1.0]})

    assert dataframe_to_columnar(df, "m")["time"] == [EXPECTED_MICROS]


def test_polars_datetime_conversion() -> None:
    pl = pytest.importorskip("polars")
    from datetime import datetime, timezone

    from arc_client.ingestion.msgpack import dataframe_to_columnar

    df = pl.DataFrame({"time": [datetime(2021, 10, 1, tzinfo=timezone.utc)], "v": [1.0]})

    micros = dataframe_to_columnar(df, "m")["time"]

    assert micros == [1633046400000000]
