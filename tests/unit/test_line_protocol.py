"""Unit tests for Line Protocol formatting."""

from __future__ import annotations

import pytest

from arc_client.exceptions import ArcValidationError
from arc_client.ingestion.line_protocol import (
    format_columnar_as_lines,
    format_line_protocol,
    format_lines,
)


class TestFormatLineProtocol:
    """Tests for format_line_protocol function."""

    def test_basic_format(self) -> None:
        """Test basic Line Protocol formatting."""
        line = format_line_protocol(
            measurement="cpu",
            fields={"usage": 45.2},
        )
        assert line == "cpu usage=45.2"

    def test_format_with_tags(self) -> None:
        """Test formatting with tags."""
        line = format_line_protocol(
            measurement="cpu",
            fields={"usage": 45.2},
            tags={"host": "server01", "region": "us-east"},
        )
        # Tags should be sorted alphabetically
        assert line == "cpu,host=server01,region=us-east usage=45.2"

    def test_format_with_timestamp(self) -> None:
        """Test formatting with timestamp."""
        line = format_line_protocol(
            measurement="cpu",
            fields={"usage": 45.2},
            timestamp=1633024800000000,
            time_unit="us",
        )
        # Timestamp should be in nanoseconds
        assert line == "cpu usage=45.2 1633024800000000000"

    def test_format_multiple_fields(self) -> None:
        """Test formatting with multiple fields."""
        line = format_line_protocol(
            measurement="cpu",
            fields={"usage_idle": 95.0, "usage_user": 3.2},
        )
        assert "usage_idle=95.0" in line
        assert "usage_user=3.2" in line

    def test_format_integer_field(self) -> None:
        """Test integer field formatting (i suffix)."""
        line = format_line_protocol(
            measurement="mem",
            fields={"used": 1024},
        )
        assert "used=1024i" in line

    def test_format_boolean_field(self) -> None:
        """Test boolean field formatting."""
        line = format_line_protocol(
            measurement="status",
            fields={"healthy": True, "degraded": False},
        )
        assert "healthy=true" in line
        assert "degraded=false" in line

    def test_format_string_field(self) -> None:
        """Test string field formatting (quoted)."""
        line = format_line_protocol(
            measurement="logs",
            fields={"message": "hello world"},
        )
        assert 'message="hello world"' in line

    def test_escape_special_chars_in_measurement(self) -> None:
        """Test escaping special characters in measurement name."""
        line = format_line_protocol(
            measurement="cpu,usage",
            fields={"value": 1.0},
        )
        assert r"cpu\,usage" in line

    def test_escape_special_chars_in_tag(self) -> None:
        """Test escaping special characters in tags."""
        line = format_line_protocol(
            measurement="cpu",
            fields={"value": 1.0},
            tags={"host=name": "server,01"},
        )
        assert r"host\=name=server\,01" in line

    def test_empty_measurement_error(self) -> None:
        """Test that empty measurement raises error."""
        with pytest.raises(ArcValidationError, match="Measurement name cannot be empty"):
            format_line_protocol("", {"value": 1.0})

    def test_empty_fields_error(self) -> None:
        """Test that empty fields raises error."""
        with pytest.raises(ArcValidationError, match="Fields cannot be empty"):
            format_line_protocol("cpu", {})


class TestFormatLines:
    """Tests for format_lines function."""

    def test_format_multiple_lines(self) -> None:
        """Test formatting multiple records."""
        records = [
            {"measurement": "cpu", "fields": {"usage": 45.2}},
            {"measurement": "cpu", "fields": {"usage": 47.8}},
        ]
        lines = format_lines(records)
        assert lines.count("\n") == 1
        assert "usage=45.2" in lines
        assert "usage=47.8" in lines


class TestFormatColumnarAsLines:
    """Tests for format_columnar_as_lines function."""

    def test_convert_columnar_to_lines(self) -> None:
        """Test converting columnar data to lines."""
        columns = {
            "time": [1633024800000000, 1633024801000000],
            "host": ["server01", "server02"],
            "usage": [45.2, 47.8],
        }
        lines = format_columnar_as_lines(
            measurement="cpu",
            columns=columns,
            tag_columns=["host"],
        )

        lines_list = lines.split("\n")
        assert len(lines_list) == 2
        assert "host=server01" in lines_list[0]
        assert "host=server02" in lines_list[1]
        assert "usage=45.2" in lines_list[0]
        assert "usage=47.8" in lines_list[1]

    def test_empty_columns_error(self) -> None:
        """Test that empty columns raises error."""
        with pytest.raises(ArcValidationError, match="cannot be empty"):
            format_columnar_as_lines("cpu", {})
