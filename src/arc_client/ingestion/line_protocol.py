"""InfluxDB Line Protocol formatting for Arc ingestion.

This module provides Line Protocol serialization for Arc, which is
compatible with InfluxDB's text-based ingestion format.

Line Protocol Format:
    measurement[,tag=value...] field=value[,field=value...] [timestamp]

Example:
    cpu,host=server01,region=us-east usage_idle=95.0,usage_user=3.2 1633024800000000

Note: For best performance, use MessagePack columnar format instead.
Line Protocol is provided for compatibility with existing tooling.
"""

from __future__ import annotations

from typing import Any

from arc_client.exceptions import ArcValidationError


def format_line_protocol(
    measurement: str,
    fields: dict[str, Any],
    tags: dict[str, str] | None = None,
    timestamp: int | None = None,
    time_unit: str = "us",
) -> str:
    """Format a single data point as InfluxDB Line Protocol.

    Args:
        measurement: The measurement (table) name.
        fields: Dictionary of field names to values.
            Values can be float, int, bool, or str.
        tags: Optional dictionary of tag names to string values.
            Tags are indexed dimensions used for filtering.
        timestamp: Optional timestamp. If None, Arc will use server time.
        time_unit: Unit of the timestamp - "s", "ms", "us", or "ns".
            Arc expects nanoseconds in Line Protocol.

    Returns:
        Line Protocol formatted string.

    Raises:
        ArcValidationError: If measurement or fields are invalid.

    Example:
        >>> line = format_line_protocol(
        ...     measurement="cpu",
        ...     fields={"usage_idle": 95.0, "usage_user": 3.2},
        ...     tags={"host": "server01", "region": "us-east"},
        ...     timestamp=1633024800000000,
        ... )
        >>> print(line)
        cpu,host=server01,region=us-east usage_idle=95.0,usage_user=3.2 1633024800000000000
    """
    if not measurement:
        raise ArcValidationError("Measurement name cannot be empty")

    if not fields:
        raise ArcValidationError("Fields cannot be empty")

    # Escape measurement name
    escaped_measurement = _escape_measurement(measurement)

    # Build tag set
    tag_str = ""
    if tags:
        sorted_tags = sorted(tags.items())  # Sort for deterministic output
        tag_parts = [f"{_escape_tag_key(k)}={_escape_tag_value(v)}" for k, v in sorted_tags if v]
        if tag_parts:
            tag_str = "," + ",".join(tag_parts)

    # Build field set
    field_parts = []
    for key, value in fields.items():
        escaped_key = _escape_field_key(key)
        formatted_value = _format_field_value(value)
        field_parts.append(f"{escaped_key}={formatted_value}")
    field_str = ",".join(field_parts)

    # Build timestamp (convert to nanoseconds for Line Protocol)
    ts_str = ""
    if timestamp is not None:
        ts_ns = _normalize_to_nanoseconds(timestamp, time_unit)
        ts_str = f" {ts_ns}"

    return f"{escaped_measurement}{tag_str} {field_str}{ts_str}"


def format_lines(
    records: list[dict[str, Any]],
    time_unit: str = "us",
) -> str:
    """Format multiple records as Line Protocol.

    Args:
        records: List of record dictionaries. Each should have:
            - measurement: str
            - fields: dict
            - tags: dict (optional)
            - timestamp: int (optional)
        time_unit: Unit of timestamps in records.

    Returns:
        Multi-line string of Line Protocol formatted data.

    Example:
        >>> lines = format_lines([
        ...     {
        ...         "measurement": "cpu",
        ...         "fields": {"usage": 45.2},
        ...         "tags": {"host": "server01"},
        ...         "timestamp": 1633024800000000,
        ...     },
        ...     {
        ...         "measurement": "cpu",
        ...         "fields": {"usage": 47.8},
        ...         "tags": {"host": "server01"},
        ...         "timestamp": 1633024801000000,
        ...     },
        ... ])
    """
    lines = []
    for record in records:
        line = format_line_protocol(
            measurement=record.get("measurement", ""),
            fields=record.get("fields", {}),
            tags=record.get("tags"),
            timestamp=record.get("timestamp"),
            time_unit=time_unit,
        )
        lines.append(line)
    return "\n".join(lines)


def format_columnar_as_lines(
    measurement: str,
    columns: dict[str, list[Any]],
    tag_columns: list[str] | None = None,
    time_column: str = "time",
    time_unit: str = "us",
) -> str:
    """Convert columnar data to Line Protocol format.

    This is useful when you have columnar data but need to send it
    using Line Protocol (e.g., for compatibility reasons).

    Args:
        measurement: The measurement name.
        columns: Dictionary of column names to value lists.
        tag_columns: Column names to treat as tags (dimensions).
        time_column: Name of the timestamp column.
        time_unit: Unit of timestamps.

    Returns:
        Multi-line string of Line Protocol formatted data.
    """
    if tag_columns is None:
        tag_columns = []

    if not columns:
        raise ArcValidationError("Columns cannot be empty")

    # Get number of records
    num_records = len(next(iter(columns.values())))

    lines = []
    for i in range(num_records):
        fields = {}
        tags = {}
        timestamp = None

        for col_name, values in columns.items():
            value = values[i]

            if col_name == time_column:
                timestamp = value
            elif col_name in tag_columns:
                tags[col_name] = str(value) if value is not None else ""
            else:
                if value is not None:
                    fields[col_name] = value

        if fields:  # Only create line if there are fields
            line = format_line_protocol(
                measurement=measurement,
                fields=fields,
                tags=tags if tags else None,
                timestamp=timestamp,
                time_unit=time_unit,
            )
            lines.append(line)

    return "\n".join(lines)


def _escape_measurement(measurement: str) -> str:
    """Escape special characters in measurement name."""
    # Escape commas and spaces
    return measurement.replace(",", r"\,").replace(" ", r"\ ")


def _escape_tag_key(key: str) -> str:
    """Escape special characters in tag key."""
    return key.replace(",", r"\,").replace("=", r"\=").replace(" ", r"\ ")


def _escape_tag_value(value: str) -> str:
    """Escape special characters in tag value."""
    return value.replace(",", r"\,").replace("=", r"\=").replace(" ", r"\ ")


def _escape_field_key(key: str) -> str:
    """Escape special characters in field key."""
    return key.replace(",", r"\,").replace("=", r"\=").replace(" ", r"\ ")


def _format_field_value(value: Any) -> str:
    """Format a field value for Line Protocol.

    Types:
    - Float: 1.5 (no suffix)
    - Integer: 1i (i suffix)
    - Boolean: true/false
    - String: "value" (quoted)
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    elif isinstance(value, int):
        return f"{value}i"
    elif isinstance(value, float):
        return str(value)
    elif isinstance(value, str):
        # Escape quotes and backslashes in strings
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    else:
        # Convert to string for unknown types
        escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'


def _normalize_to_nanoseconds(timestamp: int, time_unit: str) -> int:
    """Convert timestamp to nanoseconds for Line Protocol.

    Line Protocol expects timestamps in nanoseconds.
    """
    if time_unit == "s":
        return timestamp * 1_000_000_000
    elif time_unit == "ms":
        return timestamp * 1_000_000
    elif time_unit == "us":
        return timestamp * 1_000
    elif time_unit == "ns":
        return timestamp
    else:
        raise ArcValidationError(
            f"Invalid time_unit: {time_unit}. Must be 's', 'ms', 'us', or 'ns'"
        )
