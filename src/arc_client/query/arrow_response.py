"""Decode native Arrow data and authoritative completion metadata."""

from __future__ import annotations

import math
import re

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc

from arc_client.exceptions import ArcQueryError
from arc_client.http.arrow_trailers import TRAILERS_EXTENSION, ArrowTrailers

_CAP = b"arc-rows-capped"
_TRUNCATED = b"arc-stream-truncated"
_TIME = b"arc-execution-time-ms"
_FIELDS = {_CAP, _TRUNCATED, _TIME}
_RESERVED = {b"arc.rows_capped", b"arc.row_cap", b"arc.execution_time_ms"}


def decode_arrow_response(response: httpx.Response) -> pa.Table:
    """Keep the Table API and record server metadata in its schema.

    Missing legacy metadata is unknown. An announced Arc trailer requires a
    completed capture; malformed or truncated responses never become tables.
    """
    if "application/json" in response.headers.get("content-type", ""):
        raise ArcQueryError(response.json().get("error", "Query failed with unknown error"))

    announced = {
        name.strip().lower().encode("ascii")
        for value in response.headers.get_list("trailer")
        for name in value.split(",")
    }
    sink = response.extensions.get(TRAILERS_EXTENSION)
    complete = isinstance(sink, ArrowTrailers) and sink.complete
    if announced & _FIELDS and not complete:
        raise ArcQueryError(
            "Arrow completion metadata could not be captured by this HTTP transport"
        )

    values: dict[bytes, bytes] = {}
    fields = list(response.headers.raw)
    final_timing = False
    if isinstance(sink, ArrowTrailers) and sink.complete:
        fields.extend(sink.headers)
        final_timing = any(name.lower() == _TIME and value.strip() for name, value in sink.headers)
    for name, value in fields:
        name, value = name.lower(), value.strip()
        if name not in _FIELDS or not value:
            continue
        if name in values and values[name] != value:
            raise ArcQueryError("Conflicting Arrow completion metadata")
        values[name] = value

    if _TRUNCATED in values:
        reason = values[_TRUNCATED].decode("utf-8", errors="replace")
        raise ArcQueryError("Arrow stream truncated: " + reason)

    metadata: dict[bytes, bytes] = {}
    if _CAP in values:
        cap = values[_CAP]
        if not re.fullmatch(rb"[0-9]{1,19}", cap) or not 0 < int(cap) <= 2**63 - 1:
            raise ArcQueryError("Invalid Arrow row-cap metadata")
        metadata[b"arc.rows_capped"] = b"true"
        metadata[b"arc.row_cap"] = str(int(cap)).encode("ascii")
    elif complete and _CAP in announced and final_timing:
        # Both supported Arc versions announce this trailer on every result;
        # an empty (release) or absent (main) cap means uncapped. Require the
        # always-sent execution marker in FINAL trailers too: a gateway that
        # promotes timing but drops trailers cannot prove uncapped completion.
        metadata[b"arc.rows_capped"] = b"false"
    if _TIME in values:
        try:
            elapsed = float(values[_TIME])
        except ValueError as error:
            raise ArcQueryError("Invalid Arrow execution-time metadata") from error
        if not math.isfinite(elapsed) or elapsed < 0:
            raise ArcQueryError("Invalid Arrow execution-time metadata")
        metadata[b"arc.execution_time_ms"] = values[_TIME]

    with ipc.open_stream(response.content) as reader:
        table = reader.read_all()
    # These names are SDK-reserved. Wire completion metadata takes precedence
    # over schema content, including when the server supplied no cap signal.
    schema_metadata = {k: v for k, v in (table.schema.metadata or {}).items() if k not in _RESERVED}
    schema_metadata.update(metadata)
    return table.replace_schema_metadata(schema_metadata or None)
