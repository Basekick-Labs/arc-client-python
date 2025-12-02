"""MessagePack encoding for Arc ingestion.

This module provides high-performance MessagePack serialization for Arc's
binary ingestion protocol. It supports both columnar format (recommended,
25-35% faster) and row format (legacy).

Columnar Format (RECOMMENDED):
    {
        "m": "cpu",  # measurement name
        "columns": {
            "time": [1633024800000000, 1633024801000000],  # microseconds
            "host": ["server01", "server01"],
            "usage": [45.2, 47.8],
        }
    }

Row Format (LEGACY):
    {
        "m": "cpu",
        "t": 1633024800000000,  # timestamp in microseconds
        "fields": {"usage": 45.2},
        "tags": {"host": "server01"},
    }
"""

from __future__ import annotations

import time
from typing import Any, Optional

import msgpack

from arc_client.exceptions import ArcValidationError


def encode_columnar(
    measurement: str,
    columns: dict[str, list[Any]],
    time_unit: str = "us",
) -> bytes:
    """Encode columnar data to MessagePack format.

    This is the highest-performance ingestion format for Arc, providing
    25-35% faster ingestion than row format due to zero-copy passthrough.

    Args:
        measurement: The measurement (table) name.
        columns: Dictionary mapping column names to lists of values.
            Must include a 'time' column with timestamps.
            All arrays must have the same length.
        time_unit: Unit of timestamps in the 'time' column.
            Options: "s" (seconds), "ms" (milliseconds), "us" (microseconds).
            Default is "us" (microseconds).

    Returns:
        MessagePack encoded bytes ready for Arc ingestion.

    Raises:
        ArcValidationError: If columns are invalid or missing required 'time'.

    Example:
        >>> data = encode_columnar(
        ...     measurement="cpu",
        ...     columns={
        ...         "time": [1633024800000000, 1633024801000000],
        ...         "host": ["server01", "server01"],
        ...         "usage": [45.2, 47.8],
        ...     },
        ... )
    """
    if not measurement:
        raise ArcValidationError("Measurement name cannot be empty")

    if not columns:
        raise ArcValidationError("Columns cannot be empty")

    # Validate all arrays have the same length
    lengths = {name: len(values) for name, values in columns.items()}
    unique_lengths = set(lengths.values())
    if len(unique_lengths) > 1:
        raise ArcValidationError(f"All columns must have the same length. Got: {lengths}")

    # Ensure 'time' column exists
    if "time" not in columns:
        # Generate timestamps if missing
        num_records = next(iter(lengths.values())) if lengths else 0
        now_us = int(time.time() * 1_000_000)
        columns = dict(columns)  # Make a copy
        columns["time"] = [now_us + i for i in range(num_records)]

    # Normalize timestamps to microseconds if needed
    columns = _normalize_timestamps(columns, time_unit)

    payload = {
        "m": measurement,
        "columns": columns,
    }

    result: bytes = msgpack.packb(payload, use_bin_type=True)
    return result


def encode_records(records: list[dict[str, Any]]) -> bytes:
    """Encode multiple row-format records to MessagePack.

    This uses the legacy row format. For better performance, use
    encode_columnar() instead.

    Args:
        records: List of record dictionaries. Each record should have:
            - measurement: str - The measurement name
            - timestamp: int - Timestamp in microseconds (optional)
            - fields: dict - Field values (required)
            - tags: dict - Tag values (optional)

    Returns:
        MessagePack encoded bytes.

    Example:
        >>> data = encode_records([
        ...     {
        ...         "measurement": "cpu",
        ...         "timestamp": 1633024800000000,
        ...         "fields": {"usage": 45.2},
        ...         "tags": {"host": "server01"},
        ...     },
        ... ])
    """
    if not records:
        raise ArcValidationError("Records list cannot be empty")

    payload = []
    for record in records:
        item = _encode_single_record(record)
        payload.append(item)

    # Wrap in batch format
    result: bytes = msgpack.packb(payload, use_bin_type=True)
    return result


def encode_single_record(record: dict[str, Any]) -> bytes:
    """Encode a single row-format record to MessagePack.

    Args:
        record: Record dictionary with measurement, timestamp, fields, tags.

    Returns:
        MessagePack encoded bytes.
    """
    payload = _encode_single_record(record)
    result: bytes = msgpack.packb(payload, use_bin_type=True)
    return result


def encode_batch(items: list[dict[str, Any]]) -> bytes:
    """Encode a batch of mixed items (columnar or row format).

    Args:
        items: List of payload dictionaries. Each can be either:
            - Columnar: {"m": "...", "columns": {...}}
            - Row: {"m": "...", "t": ..., "fields": {...}, "tags": {...}}

    Returns:
        MessagePack encoded bytes with batch wrapper.
    """
    if not items:
        raise ArcValidationError("Batch cannot be empty")

    payload = {"batch": items}
    result: bytes = msgpack.packb(payload, use_bin_type=True)
    return result


def _encode_single_record(record: dict[str, Any]) -> dict[str, Any]:
    """Convert a record dict to Arc's row format payload."""
    measurement = record.get("measurement")
    if not measurement:
        raise ArcValidationError("Record must have 'measurement' field")

    fields = record.get("fields")
    if not fields:
        raise ArcValidationError("Record must have 'fields' field")

    timestamp = record.get("timestamp")
    if timestamp is None:
        timestamp = int(time.time() * 1_000_000)  # Default to now in microseconds

    tags = record.get("tags", {})

    return {
        "m": measurement,
        "t": timestamp,
        "fields": fields,
        "tags": tags,
    }


def _normalize_timestamps(columns: dict[str, list[Any]], time_unit: str) -> dict[str, list[Any]]:
    """Normalize timestamps to microseconds.

    Arc expects timestamps in microseconds. This function converts from
    the specified unit to microseconds.

    Args:
        columns: Column dictionary containing a 'time' column.
        time_unit: Input unit - "s", "ms", or "us".

    Returns:
        Column dictionary with normalized timestamps.
    """
    if "time" not in columns:
        return columns

    time_col = columns["time"]
    if not time_col:
        return columns

    # Determine multiplier
    if time_unit == "s":
        multiplier = 1_000_000
    elif time_unit == "ms":
        multiplier = 1_000
    elif time_unit == "us":
        multiplier = 1
    else:
        raise ArcValidationError(f"Invalid time_unit: {time_unit}. Must be 's', 'ms', or 'us'")

    if multiplier == 1:
        return columns

    # Convert timestamps
    columns = dict(columns)  # Make a copy
    columns["time"] = [int(t * multiplier) for t in time_col]
    return columns


def dataframe_to_columnar(
    df: Any,
    measurement: str,
    time_column: str = "time",
    tag_columns: Optional[list[str]] = None,
) -> dict[str, list[Any]]:
    """Convert a DataFrame to columnar format for Arc ingestion.

    Supports pandas DataFrames, Polars DataFrames, and PyArrow Tables.

    Args:
        df: DataFrame to convert (pandas, polars, or pyarrow).
        measurement: Measurement name (not used in output, just for validation).
        time_column: Name of the timestamp column.
        tag_columns: Columns to treat as tags (string dimensions).

    Returns:
        Dictionary of column name to list of values.

    Raises:
        ArcValidationError: If DataFrame is invalid or time column is missing.
    """
    if tag_columns is None:
        tag_columns = []

    # Detect DataFrame type and convert
    df_type = type(df).__module__

    if "pandas" in df_type:
        return _pandas_to_columnar(df, time_column, tag_columns)
    elif "polars" in df_type:
        return _polars_to_columnar(df, time_column, tag_columns)
    elif "pyarrow" in df_type:
        return _arrow_to_columnar(df, time_column, tag_columns)
    else:
        raise ArcValidationError(
            f"Unsupported DataFrame type: {type(df)}. "
            "Supported: pandas.DataFrame, polars.DataFrame, pyarrow.Table"
        )


def _pandas_to_columnar(df: Any, time_column: str, tag_columns: list[str]) -> dict[str, list[Any]]:
    """Convert pandas DataFrame to columnar format."""
    import pandas as pd

    if time_column not in df.columns:
        raise ArcValidationError(f"Time column '{time_column}' not found in DataFrame")

    columns: dict[str, list[Any]] = {}

    for col_name in df.columns:
        col = df[col_name]

        # Handle timestamp column
        if col_name == time_column:
            if pd.api.types.is_datetime64_any_dtype(col):
                # Convert datetime to microseconds since epoch
                columns["time"] = (col.astype("int64") // 1000).tolist()
            else:
                # Assume already numeric timestamps
                columns["time"] = col.tolist()
        else:
            # Convert to Python native types
            columns[col_name] = col.tolist()

    return columns


def _polars_to_columnar(df: Any, time_column: str, tag_columns: list[str]) -> dict[str, list[Any]]:
    """Convert Polars DataFrame to columnar format."""
    if time_column not in df.columns:
        raise ArcValidationError(f"Time column '{time_column}' not found in DataFrame")

    columns: dict[str, list[Any]] = {}

    for col_name in df.columns:
        col = df[col_name]

        # Handle timestamp column
        if col_name == time_column:
            # Check if it's a datetime type
            if "datetime" in str(col.dtype).lower() or "date" in str(col.dtype).lower():
                # Convert to microseconds
                columns["time"] = col.cast(df.__class__._from_arrow.__self__.Int64).to_list()
            else:
                columns["time"] = col.to_list()
        else:
            columns[col_name] = col.to_list()

    return columns


def _arrow_to_columnar(
    table: Any, time_column: str, tag_columns: list[str]
) -> dict[str, list[Any]]:
    """Convert PyArrow Table to columnar format."""
    if time_column not in table.column_names:
        raise ArcValidationError(f"Time column '{time_column}' not found in Table")

    columns: dict[str, list[Any]] = {}

    for col_name in table.column_names:
        col = table.column(col_name)

        # Handle timestamp column
        if col_name == time_column:
            # Convert to Python list - Arrow handles timestamp conversion
            columns["time"] = col.to_pylist()
        else:
            columns[col_name] = col.to_pylist()

    return columns
