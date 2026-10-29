"""Decoder for the experimental MessagePack query response format.

Arc's ``POST /api/v1/query/msgpack`` returns a **columnar** MessagePack envelope
(``application/msgpack``): a map with keys ``success, columns, types, data,
row_count, execution_time_ms, timestamp``. ``data`` is an array-of-columns
(one inner array per column), not row-major, and timestamp columns arrive as
native MessagePack timestamp extensions (decoded here to ``datetime``). Per-cell
nulls are possible.

The error envelope is a 4-key map ``{success: false, error, execution_time_ms,
timestamp}``.

This endpoint is experimental and requires the server's ``duckdb_arrow`` build
tag; without it the server responds 501, surfaced as ``ArcNotSupportedError`` by
the HTTP error handler before this decoder runs.
"""

from __future__ import annotations

from typing import Any

import msgpack

from arc_client.exceptions import ArcQueryError
from arc_client.models.query import QueryResponse


def decode_msgpack_query_response(content: bytes) -> QueryResponse:
    """Decode a MessagePack query response envelope into a :class:`QueryResponse`.

    The columnar ``data`` is transposed to row-major so the result matches the
    JSON query path (``QueryResponse.data`` is a list of rows).

    Args:
        content: Raw MessagePack response body.

    Returns:
        A :class:`QueryResponse`. For a success envelope, ``data`` is row-major
        and ``types`` carries the per-column Arrow type names.

    Raises:
        ArcQueryError: If the body cannot be decoded or reports ``success=false``.
    """
    try:
        # timestamp=3 decodes native msgpack timestamp extensions to datetime.
        envelope = msgpack.unpackb(content, timestamp=3, raw=False)
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
        execution_time_ms=float(execution_time_ms),
        timestamp=str(timestamp) if timestamp is not None else None,
    )


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
