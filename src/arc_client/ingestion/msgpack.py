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
        },
        "tag_keys": ["host"],  # optional: which columns are tags
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
    tag_keys: Optional[list[str]] = None,
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
        tag_keys: Columns Arc should store as tags rather than fields, sent as
            the payload's ``tag_keys`` (Arc 2027.01, internal/ingest/
            msgpack_typed.go; older servers ignore the key). Each must name a
            column other than ``time``. Omitted from the payload when empty.

    Returns:
        MessagePack encoded bytes ready for Arc ingestion.

    Raises:
        ArcValidationError: If columns are invalid or missing required 'time',
            or a tag key does not name a non-time column.

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

    payload: dict[str, Any] = {
        "m": measurement,
        "columns": columns,
    }
    tags = validate_tag_keys(tag_keys, columns)
    if tags:
        payload["tag_keys"] = tags

    result: bytes = msgpack.packb(payload, use_bin_type=True)
    return result


def validate_tag_keys(
    tag_keys: Optional[list[str]], columns: Any, time_column: str = "time"
) -> list[str]:
    """Validate tag column names against a column collection.

    Returns the tag keys de-duplicated in their given order. Raises
    ArcValidationError for a non-string name, the time column, or a name that
    is not one of ``columns``.
    """
    if not tag_keys:
        return []
    if isinstance(tag_keys, str):
        raise ArcValidationError("tag columns must be a list of column names, not a string")
    seen: list[str] = []
    for key in tag_keys:
        if not isinstance(key, str) or not key:
            raise ArcValidationError(f"Invalid tag column {key!r}: must be a column name")
        if key == time_column or key == "time":
            raise ArcValidationError(f"The time column {key!r} cannot be a tag")
        if key not in columns:
            raise ArcValidationError(f"Tag column {key!r} is not one of the columns")
        if key not in seen:
            seen.append(key)
    return seen


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
        tag_columns: Columns that are tags (dimensions) rather than fields.
            Validated here (each must be a column of ``df`` other than the time
            column); the caller sends them as the payload's ``tag_keys``, e.g.
            ``encode_columnar(..., tag_keys=tag_columns)``, which is what
            ``write_dataframe`` does. Without it Arc stores every non-time
            column as a field.

    Returns:
        Dictionary of column name to list of values. The tag/field split is not
        part of this dict; it travels separately as ``tag_keys``.

    Raises:
        ArcValidationError: If DataFrame is invalid, the time column is missing,
            or a tag column is not a non-time column of the DataFrame.
    """
    if tag_columns is None:
        tag_columns = []
    column_names = list(df.column_names) if hasattr(df, "column_names") else list(df.columns)
    if tag_columns and time_column not in column_names:
        raise ArcValidationError(f"Time column '{time_column}' not found in DataFrame")
    validate_tag_keys(tag_columns, column_names, time_column)

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
                columns["time"] = _pandas_datetime_to_micros(col)
            else:
                # Assume already numeric timestamps
                columns["time"] = col.tolist()
        else:
            # Convert to Python native types
            columns[col_name] = col.tolist()

    return columns


def _pandas_datetime_to_micros(col: Any) -> list[Any]:
    """Convert a pandas datetime64 column to microseconds since epoch.

    The int64 view of a datetime64 column is in the column's own resolution
    (ns/us/ms/s), so scale from that unit rather than assuming nanoseconds.
    ``datetime64[us]`` must not be divided by 1000 the way ``datetime64[ns]`` is.
    """
    import numpy as np

    dtype = col.dtype
    # tz-aware columns use pandas' DatetimeTZDtype, which exposes ``.unit``.
    # tz-naive columns use numpy datetime64, which does NOT — extract the unit
    # from the numpy dtype instead of silently defaulting to nanoseconds.
    unit = getattr(dtype, "unit", None)
    if unit is None:
        try:
            unit = np.datetime_data(dtype)[0]
        except (TypeError, ValueError):  # pragma: no cover - non-datetime dtype
            unit = "ns"
    as_int = col.astype("int64")
    # Scale factor to reach microseconds; negative means multiply (upscale).
    scale = {"ns": 1_000, "us": 1, "ms": -1_000, "s": -1_000_000}.get(unit, 1_000)
    if scale == 1:
        values = as_int
    elif scale > 0:
        values = as_int // scale
    else:
        values = as_int * (-scale)
    result: list[Any] = values.tolist()
    return result


def _polars_to_columnar(df: Any, time_column: str, tag_columns: list[str]) -> dict[str, list[Any]]:
    """Convert Polars DataFrame to columnar format."""
    import polars as pl

    if time_column not in df.columns:
        raise ArcValidationError(f"Time column '{time_column}' not found in DataFrame")

    columns: dict[str, list[Any]] = {}

    for col_name in df.columns:
        col = df[col_name]

        # Handle timestamp column
        if col_name == time_column:
            dtype = col.dtype
            if dtype == pl.Datetime:
                # Casting a Datetime to Int64 yields the underlying integer in
                # the series' own time_unit. Normalize that to microseconds.
                as_int = col.cast(pl.Int64)
                time_unit = getattr(dtype, "time_unit", "us")
                if time_unit == "ns":
                    columns["time"] = (as_int // 1_000).to_list()
                elif time_unit == "ms":
                    columns["time"] = (as_int * 1_000).to_list()
                else:  # "us"
                    columns["time"] = as_int.to_list()
            elif dtype == pl.Date:
                # Date is days since epoch; convert to microseconds.
                columns["time"] = (col.cast(pl.Int64) * 86_400_000_000).to_list()
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
