"""Decoder for the MessagePack query response format.

Arc's ``POST /api/v1/query/msgpack`` returns a **columnar** MessagePack envelope
(``application/msgpack``): a map with keys ``success, columns, types, data,
row_count, execution_time_ms, timestamp``. ``data`` is an array-of-columns
(one inner array per column), not row-major, and timestamp columns arrive as
native MessagePack timestamp extensions. Values exactly representable by Python
``datetime`` are decoded to UTC datetimes; submicrosecond or out-of-range values
remain ``msgpack.Timestamp`` objects, preserving their seconds and nanoseconds.
Per-cell nulls are possible. Optional keys: ``rows_capped``/``row_cap`` (a
governance row cap was reached) and ``profile`` (``x-arc-profile: true``).

Arc's msgpack writer declares the map's length before it streams the columns
and never emits ``truncated``/``truncation_reason`` (internal/api/
query_msgpack.go streamMsgPackFromBatches). A stream that fails after the 200
was sent (client disconnect, deadline, encoder panic) therefore arrives as an
incomplete MessagePack document. This decoder raises that as ``ArcQueryError``
rather than returning a partial result. ``truncated`` is still read if a
server ever sends it, for parity with the JSON envelope.

The error envelope is a 4-key map ``{success: false, error, execution_time_ms,
timestamp}``.

The endpoint requires the server's ``duckdb_arrow`` build tag; without it the
server responds 501, surfaced as ``ArcNotSupportedError`` by the HTTP error
handler before this decoder runs.
"""

from __future__ import annotations

from typing import Any, Optional

import msgpack

from arc_client.exceptions import ArcQueryError
from arc_client.models.query import QueryResponse


def decode_msgpack_query_response(content: bytes, query_id: Optional[str] = None) -> QueryResponse:
    """Decode a MessagePack query response envelope into a :class:`QueryResponse`.

    The columnar ``data`` is transposed to row-major so the result matches the
    JSON query path (``QueryResponse.data`` is a list of rows).
    Timestamp cells, including those nested in lists or maps, become UTC
    ``datetime`` values only when that conversion is lossless. Other timestamps
    remain ``msgpack.Timestamp``; use ``to_unix_nano()`` for exact epoch nanoseconds.

    Args:
        content: Raw MessagePack response body.
        query_id: The ``X-Arc-Query-ID`` response header, if any.

    Returns:
        A :class:`QueryResponse`. For a success envelope, ``data`` is row-major
        and ``types`` carries the per-column Arrow type names.

    Raises:
        ArcQueryError: If the body is incomplete (the server's stream ended
            early), cannot be decoded, or reports ``success=false``.
    """
    try:
        # timestamp=3 silently drops submicrosecond precision. Decode the native
        # values first, then convert only timestamps datetime can represent exactly.
        envelope = _decode_timestamps(msgpack.unpackb(content, timestamp=0, raw=False))
    except ValueError as e:
        # msgpack reports a body that ends inside the declared map as
        # "incomplete input", or, when a container header promises more items
        # than the remaining bytes could hold, "exceeds max_*_len" (both
        # ValueError). Arc sends such a body when its stream fails after the
        # 200 was committed; the rows received are partial.
        text = str(e).lower()
        if "incomplete" in text or "exceeds max_" in text:
            raise ArcQueryError(
                "MessagePack query response is incomplete: the server's stream "
                "ended before the result was complete (partial results are not "
                "returned)"
            ) from e
        raise ArcQueryError(f"Failed to decode msgpack query response: {e}") from e
    except Exception as e:  # pragma: no cover - malformed body is rare
        raise ArcQueryError(f"Failed to decode msgpack query response: {e}") from e

    if not isinstance(envelope, dict):
        raise ArcQueryError("Malformed msgpack query response: expected a map")

    if not envelope.get("success", False):
        message = envelope.get("error") or "Query failed"
        raise ArcQueryError(str(message))

    columns: list[str] = list(envelope.get("columns", []))
    types: list[str] = list(envelope.get("types", []))
    column_data: list[list[Any]] = list(envelope.get("data", []))
    row_count = int(envelope.get("row_count", 0))

    rows = _transpose_columns(column_data, row_count)

    execution_time_ms = envelope.get("execution_time_ms", 0)
    timestamp = envelope.get("timestamp")

    return QueryResponse(
        success=True,
        columns=columns,
        types=types,
        data=rows,
        row_count=row_count if row_count else len(rows),
        truncated=envelope.get("truncated", False),
        truncation_reason=envelope.get("truncation_reason"),
        rows_capped=envelope.get("rows_capped", False),
        row_cap=envelope.get("row_cap"),
        execution_time_ms=float(execution_time_ms),
        timestamp=str(timestamp) if timestamp is not None else None,
        profile=envelope.get("profile"),
        query_id=query_id,
    )


def _decode_timestamps(value: Any) -> Any:
    """Preserve precision and range while retaining datetime where lossless."""
    if isinstance(value, msgpack.Timestamp):
        if value.nanoseconds % 1000 == 0:
            try:
                return value.to_datetime()
            except (OverflowError, ValueError):
                # MessagePack supports a wider seconds range than datetime.
                pass
        return value
    if isinstance(value, list):
        return [_decode_timestamps(item) for item in value]
    if isinstance(value, dict):
        return {key: _decode_timestamps(item) for key, item in value.items()}
    return value


def _transpose_columns(column_data: list[list[Any]], row_count: int) -> list[list[Any]]:
    """Transpose an array-of-columns into row-major rows.

    Uses ``row_count`` when the columns are empty (e.g. an empty result set with
    known columns) and otherwise the longest column, so ragged input never drops
    trailing rows.
    """
    if not column_data:
        return []

    n_rows = max((len(col) for col in column_data), default=0)
    if row_count and row_count > n_rows:
        n_rows = row_count

    rows: list[list[Any]] = []
    for i in range(n_rows):
        rows.append([col[i] if i < len(col) else None for col in column_data])
    return rows
