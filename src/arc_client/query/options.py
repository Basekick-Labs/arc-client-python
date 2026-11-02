"""Request options and response helpers shared by the sync and async query clients."""

from __future__ import annotations

from typing import Any, Optional

import httpx

from arc_client.exceptions import ArcValidationError
from arc_client.models.query import QueryResponse
from arc_client.query.arrow_response import QUERY_ID_HEADER
from arc_client.validation import validate_name

# Arc accepts exactly these (case-insensitive) for x-arc-arrow-compression and
# answers 400 for anything else (internal/api/query_arrow.go).
ARROW_COMPRESSIONS = frozenset({"zstd", "lz4"})

# GET /api/v1/query/:measurement rejects a limit above this (query.go).
MAX_MEASUREMENT_LIMIT = 1_000_000


def status_code(error: Exception) -> Optional[int]:
    """The HTTP status an error carries, if any (None for transport failures)."""
    value = getattr(error, "status_code", None)
    return value if isinstance(value, int) else None


def query_headers(database: Optional[str], profile: bool) -> dict[str, str]:
    """Headers for the JSON/msgpack/estimate query endpoints."""
    headers: dict[str, str] = {}
    if database:
        headers["x-arc-database"] = database
    if profile:
        # Arc compares the value with == "true" (query.go executeQuery).
        headers["x-arc-profile"] = "true"
    return headers


def arrow_headers(
    database: Optional[str], dictionary: bool, compression: Optional[str]
) -> dict[str, str]:
    """Headers for the Arrow IPC endpoint, validating the opt-in encodings."""
    headers = {"Accept": "application/vnd.apache.arrow.stream"}
    if database:
        headers["x-arc-database"] = database
    if dictionary:
        headers["x-arc-arrow-dictionary"] = "true"
    if compression is not None:
        codec = compression.strip().lower()
        if codec not in ARROW_COMPRESSIONS:
            raise ArcValidationError(
                f"Invalid Arrow compression {compression!r}: must be 'zstd' or 'lz4'"
            )
        headers["x-arc-arrow-compression"] = codec
    return headers


def measurement_params(
    measurement: str,
    database: Optional[str],
    limit: Optional[int],
    offset: Optional[int],
    order_by: Optional[str],
    where: Optional[str],
) -> dict[str, Any]:
    """Validate and build the query string for GET /api/v1/query/:measurement."""
    validate_name(measurement, "measurement")
    params: dict[str, Any] = {}
    if database:
        params["database"] = validate_name(database, "database")
    if limit is not None:
        if isinstance(limit, bool) or not 0 <= limit <= MAX_MEASUREMENT_LIMIT:
            raise ArcValidationError(f"limit must be between 0 and {MAX_MEASUREMENT_LIMIT}")
        params["limit"] = limit
    if offset is not None:
        if isinstance(offset, bool) or offset < 0:
            raise ArcValidationError("offset must be a non-negative integer")
        params["offset"] = offset
    if order_by is not None:
        params["order_by"] = order_by
    if where is not None:
        params["where"] = where
    return params


def query_response_from_json(response: httpx.Response) -> QueryResponse:
    """Build a QueryResponse from a JSON body plus the X-Arc-Query-ID header."""
    data = response.json()
    query_id = response.headers.get(QUERY_ID_HEADER)
    if query_id and isinstance(data, dict) and not data.get("query_id"):
        data = {**data, "query_id": query_id}
    return QueryResponse.model_validate(data)
