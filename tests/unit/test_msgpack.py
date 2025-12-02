"""Unit tests for MessagePack encoding."""

from __future__ import annotations

import msgpack
import pytest

from arc_client.exceptions import ArcValidationError
from arc_client.ingestion.msgpack import (
    encode_batch,
    encode_columnar,
    encode_records,
    encode_single_record,
)


class TestEncodeColumnar:
    """Tests for encode_columnar function."""

    def test_encode_basic_columnar(self) -> None:
        """Test basic columnar encoding."""
        columns = {
            "time": [1633024800000000, 1633024801000000],
            "host": ["server01", "server01"],
            "usage": [45.2, 47.8],
        }
        data = encode_columnar("cpu", columns)

        # Decode and verify structure
        decoded = msgpack.unpackb(data, raw=False)
        assert decoded["m"] == "cpu"
        assert "columns" in decoded
        assert decoded["columns"]["time"] == [1633024800000000, 1633024801000000]
        assert decoded["columns"]["host"] == ["server01", "server01"]
        assert decoded["columns"]["usage"] == [45.2, 47.8]

    def test_encode_columnar_generates_time(self) -> None:
        """Test that time column is generated if missing."""
        columns = {
            "host": ["server01", "server02"],
            "usage": [45.2, 47.8],
        }
        data = encode_columnar("cpu", columns)

        decoded = msgpack.unpackb(data, raw=False)
        assert "time" in decoded["columns"]
        assert len(decoded["columns"]["time"]) == 2

    def test_encode_columnar_time_unit_seconds(self) -> None:
        """Test timestamp normalization from seconds."""
        columns = {
            "time": [1633024800, 1633024801],  # seconds
            "value": [1.0, 2.0],
        }
        data = encode_columnar("test", columns, time_unit="s")

        decoded = msgpack.unpackb(data, raw=False)
        # Should be converted to microseconds
        assert decoded["columns"]["time"] == [1633024800000000, 1633024801000000]

    def test_encode_columnar_time_unit_milliseconds(self) -> None:
        """Test timestamp normalization from milliseconds."""
        columns = {
            "time": [1633024800000, 1633024801000],  # milliseconds
            "value": [1.0, 2.0],
        }
        data = encode_columnar("test", columns, time_unit="ms")

        decoded = msgpack.unpackb(data, raw=False)
        # Should be converted to microseconds
        assert decoded["columns"]["time"] == [1633024800000000, 1633024801000000]

    def test_encode_columnar_empty_measurement(self) -> None:
        """Test that empty measurement raises error."""
        with pytest.raises(ArcValidationError, match="Measurement name cannot be empty"):
            encode_columnar("", {"time": [1], "value": [1.0]})

    def test_encode_columnar_empty_columns(self) -> None:
        """Test that empty columns raises error."""
        with pytest.raises(ArcValidationError, match="Columns cannot be empty"):
            encode_columnar("cpu", {})

    def test_encode_columnar_mismatched_lengths(self) -> None:
        """Test that mismatched column lengths raise error."""
        columns = {
            "time": [1, 2, 3],
            "value": [1.0, 2.0],  # Different length
        }
        with pytest.raises(ArcValidationError, match="same length"):
            encode_columnar("cpu", columns)

    def test_encode_columnar_invalid_time_unit(self) -> None:
        """Test that invalid time unit raises error."""
        columns = {"time": [1], "value": [1.0]}
        with pytest.raises(ArcValidationError, match="Invalid time_unit"):
            encode_columnar("cpu", columns, time_unit="invalid")


class TestEncodeRecords:
    """Tests for encode_records function."""

    def test_encode_single_record(self) -> None:
        """Test encoding a single record."""
        records = [
            {
                "measurement": "cpu",
                "timestamp": 1633024800000000,
                "fields": {"usage": 45.2},
                "tags": {"host": "server01"},
            },
        ]
        data = encode_records(records)

        decoded = msgpack.unpackb(data, raw=False)
        assert len(decoded) == 1
        assert decoded[0]["m"] == "cpu"
        assert decoded[0]["t"] == 1633024800000000
        assert decoded[0]["fields"] == {"usage": 45.2}
        assert decoded[0]["tags"] == {"host": "server01"}

    def test_encode_multiple_records(self) -> None:
        """Test encoding multiple records."""
        records = [
            {"measurement": "cpu", "fields": {"usage": 45.2}},
            {"measurement": "cpu", "fields": {"usage": 47.8}},
        ]
        data = encode_records(records)

        decoded = msgpack.unpackb(data, raw=False)
        assert len(decoded) == 2

    def test_encode_records_generates_timestamp(self) -> None:
        """Test that timestamp is generated if missing."""
        records = [{"measurement": "cpu", "fields": {"usage": 45.2}}]
        data = encode_records(records)

        decoded = msgpack.unpackb(data, raw=False)
        assert "t" in decoded[0]
        assert decoded[0]["t"] > 0

    def test_encode_records_empty_list(self) -> None:
        """Test that empty records list raises error."""
        with pytest.raises(ArcValidationError, match="cannot be empty"):
            encode_records([])

    def test_encode_records_missing_measurement(self) -> None:
        """Test that missing measurement raises error."""
        with pytest.raises(ArcValidationError, match="measurement"):
            encode_records([{"fields": {"value": 1.0}}])

    def test_encode_records_missing_fields(self) -> None:
        """Test that missing fields raises error."""
        with pytest.raises(ArcValidationError, match="fields"):
            encode_records([{"measurement": "cpu"}])


class TestEncodeSingleRecord:
    """Tests for encode_single_record function."""

    def test_encode_single(self) -> None:
        """Test encoding a single record."""
        record = {
            "measurement": "cpu",
            "timestamp": 1633024800000000,
            "fields": {"usage": 45.2},
        }
        data = encode_single_record(record)

        decoded = msgpack.unpackb(data, raw=False)
        assert decoded["m"] == "cpu"
        assert decoded["fields"] == {"usage": 45.2}


class TestEncodeBatch:
    """Tests for encode_batch function."""

    def test_encode_batch(self) -> None:
        """Test encoding a batch of items."""
        items = [
            {"m": "cpu", "columns": {"time": [1, 2], "value": [1.0, 2.0]}},
            {"m": "mem", "columns": {"time": [1, 2], "value": [100, 200]}},
        ]
        data = encode_batch(items)

        decoded = msgpack.unpackb(data, raw=False)
        assert "batch" in decoded
        assert len(decoded["batch"]) == 2

    def test_encode_batch_empty(self) -> None:
        """Test that empty batch raises error."""
        with pytest.raises(ArcValidationError, match="cannot be empty"):
            encode_batch([])
