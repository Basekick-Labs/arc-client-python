# arc-tsdb-client

Python SDK for [Arc](https://github.com/basekick-labs/arc) time-series database.

[![PyPI version](https://badge.fury.io/py/arc-tsdb-client.svg)](https://badge.fury.io/py/arc-tsdb-client)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Installation

```bash
pip install arc-tsdb-client

# With pandas support
pip install arc-tsdb-client[pandas]

# With polars support
pip install arc-tsdb-client[polars]

# With all optional dependencies
pip install arc-tsdb-client[all]
```

Or with uv:

```bash
uv add arc-tsdb-client
uv add arc-tsdb-client --extra pandas
uv add arc-tsdb-client --extra all
```

## Quick Start

```python
from arc_client import ArcClient

with ArcClient(host="localhost", token="your-token") as client:
    # Write data (columnar format - fastest)
    client.write.write_columnar(
        measurement="cpu",
        columns={
            "time": [1633024800000000, 1633024801000000],
            "host": ["server01", "server01"],
            "usage_idle": [95.0, 94.5],
        },
    )

    # Query to pandas DataFrame
    df = client.query.query_pandas("SELECT * FROM default.cpu WHERE host = 'server01'")
    print(df)
```

## Features

- **High-performance ingestion**: MessagePack columnar format (9M+ records/sec)
- **Multiple formats**: MessagePack columnar/row, InfluxDB Line Protocol, TLE
- **Compression**: gzip **and zstd** (zstd is the default and recommended codec)
- **DataFrame integration**: Pandas, Polars, PyArrow
- **Sync and async APIs**: Full async support with httpx
- **Buffered writes**: Automatic batching with a background flush timer
- **Automatic retries**: Exponential backoff for connection errors, 429, and 503
- **Query support**: SQL queries with JSON, Arrow IPC streaming, or MessagePack
- **Bulk import**: CSV, Parquet, line-protocol files, and TLE files
- **Databases**: List, create, delete databases and list measurements
- **Management**: Retention policies, continuous queries, delete operations
- **Enterprise**: RBAC, query governance, audit logs, query management, cluster,
  compaction, storage tiering, backup/restore, and MQTT ingestion
- **Authentication**: Token management (create, rotate, revoke)

## Data Ingestion

### Columnar Format (Recommended)

The fastest way to write data. Uses MessagePack with zstd compression by
default (the server recommends zstd; gzip and no compression are also supported —
see [Configuration](#configuration)):

```python
client.write.write_columnar(
    measurement="cpu",
    columns={
        "time": [1633024800000000, 1633024801000000],  # microseconds
        "host": ["server01", "server02"],
        "region": ["us-east", "us-west"],
        "usage_idle": [95.0, 87.3],
        "usage_system": [3.2, 8.1],
    },
    database="default",  # optional
)
```

### DataFrame Ingestion

Write directly from pandas or polars DataFrames:

```python
import pandas as pd

df = pd.DataFrame({
    "time": pd.to_datetime(["2024-01-01", "2024-01-02"]),
    "host": ["server01", "server02"],
    "value": [42.0, 43.5],
})

client.write.write_dataframe(
    df,
    measurement="metrics",
    time_column="time",
    tag_columns=["host"],
)
```

### Buffered Writes

For high-throughput scenarios, use buffered writes with automatic batching:

```python
with client.write.buffered(batch_size=10000, flush_interval=5.0) as buffer:
    for record in records:
        buffer.write(
            measurement="events",
            tags={"source": record.source},
            fields={"value": record.value},
            timestamp=record.timestamp,
        )
    # Auto-flushes on exit or when batch_size/flush_interval reached
```

### Line Protocol

For compatibility with InfluxDB tooling:

```python
# Single line
client.write.write_line_protocol("cpu,host=server01 usage=45.2 1633024800000000000")

# Multiple lines
lines = [
    "cpu,host=server01 usage=45.2",
    "cpu,host=server02 usage=67.8",
]
client.write.write_line_protocol(lines)
```

### TLE (Two-Line Element) Streaming

Stream satellite TLE text; the server parses it into orbital-element fields:

```python
tle = """ISS (ZARYA)
1 25544U 98067A   24001.50000000  .00016717  00000-0  10270-3 0  9994
2 25544  51.6416 247.4627 0006703 130.5360 325.0288 15.49309239432416"""

client.write.write_tle(tle, measurement="satellite_tle")
```

## Bulk Import

Import files directly (admin token required). Each returns a typed result with
the number of rows imported. Files may be gzip-compressed for LP/TLE.

```python
# CSV
result = client.import_.import_csv("metrics.csv", measurement="cpu",
                                   time_column="time", time_format="epoch_ms")
print(result.rows_imported)

# Parquet
client.import_.import_parquet("data.parquet", measurement="cpu")

# InfluxDB line-protocol file
client.import_.import_lp("dump.lp", precision="ns")

# TLE file
client.import_.import_tle("catalog.tle", measurement="satellite_tle")
```

`import_csv`/`import_parquet`/`import_lp`/`import_tle` accept a path, a binary
file-like object, or raw `bytes`.

## Databases

```python
client.databases.create("metrics")
for db in client.databases.list().databases:
    print(db.name, db.measurement_count)

info = client.databases.get("metrics")
measurements = client.databases.list_measurements("metrics")

# Destructive — requires confirm=True and server-side delete enabled
client.databases.delete("metrics", confirm=True)
```

The three read methods need a token with read permission. On a server that
restricts reads per database they also need a grant for the database in
question, and `list()` needs one covering every database — Arc refuses rather
than returning a filtered list, the same way `SHOW DATABASES` does:

```python
from arc_client.exceptions import ArcScopedAccessError

try:
    databases = client.databases.list().databases
except ArcScopedAccessError:
    # Expected for a tenant-scoped token: it cannot enumerate every database,
    # so ask for the one it is granted instead of retrying.
    measurements = client.databases.list_measurements("metrics")
```

`ArcScopedAccessError.database` carries the database the call was refused for
(empty or `"*"` when the call asked for every database). A token with no read
permission at all raises `ArcPermissionError` instead; Arc failing to load the
token's grants raises `ArcPermissionDataUnavailableError`, a server-side fault
rather than a statement about the token; and a database that does not exist
still raises `ArcNotFoundError` — "you may not read it" and "it is not there"
stay apart.

## Querying Data

### JSON Response

```python
result = client.query.query("SELECT * FROM default.cpu WHERE time > now() - INTERVAL '1 hour'")
print(result.columns)  # ['time', 'host', 'usage']
print(result.data)     # [[1633024800000000, 'server01', 45.2], ...]
print(result.row_count)

if result.truncated:
    print(f"The result is incomplete: {result.truncation_reason}")
if result.rows_capped:
    print(f"The result reached the configured cap of {result.row_cap} rows")
```

### pandas DataFrame

```python
df = client.query.query_pandas("SELECT * FROM default.cpu LIMIT 1000")
```

### Polars DataFrame

```python
pl_df = client.query.query_polars("SELECT * FROM default.cpu LIMIT 1000")
```

### PyArrow Table (Zero-Copy)

```python
table = client.query.query_arrow("SELECT * FROM cpu LIMIT 1000")
metadata = table.schema.metadata or {}
if metadata.get(b"arc.rows_capped") == b"true":
    print("Policy capped the result at", int(metadata[b"arc.row_cap"]))
```

`query_arrow()` still returns a `pyarrow.Table`, with authoritative completion
metadata under reserved schema keys `arc.rows_capped`, `arc.row_cap` and
`arc.execution_time_ms` (byte keys and values). A missing cap flag means unknown;
`false` means the supported server completed its uncapped trailer contract.
Timing promoted into initial headers cannot prove uncapped completion after
final trailers disappear.
Native column types and unrelated schema metadata are preserved. Truncated,
malformed, conflicting or unobservable announced metadata raises `ArcQueryError`.
DataFrame conversions can drop schema metadata; inspect the Arrow table first
when completeness matters.

Trailer capture is tested with HTTPX 0.27/0.28, httpcore 1.0.9 and h11 0.16.
Those dependency bounds are deliberate: the adapter observes HTTP/1.1 parser
completion without replacing HTTPX's TLS, proxy, timeout or pooling behavior.

### MessagePack Response (experimental)

A compact columnar MessagePack response with native timestamp decoding. Returns
the same `QueryResponse` shape as `query()` (row-major `data`), plus per-column
Arrow types in `.types`:

```python
result = client.query.query_msgpack("SELECT * FROM default.cpu LIMIT 1000")
print(result.columns, result.types)
```

> **Experimental.** This uses `/api/v1/query/msgpack`, which requires the Arc
> server to be built with the `duckdb_arrow` build tag. Against a server without
> it, the call raises `ArcNotSupportedError` (HTTP 501).

### Query Estimation

Preview query cost before execution:

```python
estimate = client.query.estimate("SELECT * FROM default.cpu")
print(f"Estimated rows: {estimate.estimated_rows}")
print(f"Warning level: {estimate.warning_level}")  # none, low, medium, high
```

### List Measurements

```python
measurements = client.query.list_measurements(database="default")
for m in measurements:
    print(f"{m.measurement}: {m.file_count} files, {m.total_size_mb:.1f} MB")
```

## Async Support

All operations have async equivalents:

```python
import asyncio
from arc_client import AsyncArcClient

async def main():
    async with AsyncArcClient(host="localhost", token="your-token") as client:
        # Write
        await client.write.write_columnar(
            measurement="cpu",
            columns={"time": [...], "host": [...], "usage": [...]},
        )

        # Query
        df = await client.query.query_pandas("SELECT * FROM default.cpu")

        # Buffered writes
        async with client.write.buffered(batch_size=5000) as buffer:
            for record in records:
                await buffer.write(measurement="events", fields={"v": record.v})

asyncio.run(main())
```

## Management Operations

### Retention Policies

Automatically delete old data:

```python
# Create policy
policy = client.retention.create(
    name="30-day-retention",
    database="default",
    retention_days=30,
    measurement="logs",  # optional, applies to all if not specified
)

# List policies
policies = client.retention.list()

# Execute (with dry_run first)
result = client.retention.execute(policy.id, dry_run=True)
print(f"Would delete {result.deleted_count} rows")

# Execute for real
result = client.retention.execute(policy.id, dry_run=False, confirm=True)
```

### Continuous Queries

Aggregate and downsample data automatically:

```python
# Create a CQ to downsample CPU metrics to 1-hour averages
cq = client.continuous_queries.create(
    name="cpu-hourly",
    database="default",
    source_measurement="cpu",
    destination_measurement="cpu_1h",
    query="SELECT time_bucket('1 hour', time) as time, host, avg(usage) as usage FROM default.cpu GROUP BY 1, 2",
    interval="1h",
)

# Manual execution with time range
result = client.continuous_queries.execute(
    cq.id,
    start_time="2024-01-01T00:00:00Z",
    end_time="2024-01-02T00:00:00Z",
    dry_run=True,
)

# List CQs
cqs = client.continuous_queries.list(database="default")
```

### Delete Operations

Delete data with WHERE clause:

```python
# Always dry_run first
result = client.delete.delete(
    database="default",
    measurement="logs",
    where="time < '2024-01-01'",
    dry_run=True,
)
print(f"Would delete {result.deleted_count} rows from {result.affected_files} files")

# Execute deletion
result = client.delete.delete(
    database="default",
    measurement="logs",
    where="time < '2024-01-01'",
    dry_run=False,
    confirm=True,  # Required for large deletes
)
```

### Authentication

```python
# Verify current token
verify = client.auth.verify()
if verify.valid:
    print(f"Token: {verify.token_info.name}")
    print(f"Permissions: {verify.permissions}")

# Create new token
result = client.auth.create_token(
    name="my-app-token",
    description="Token for my application",
    permissions=["read", "write"],
)
print(f"New token: {result.token}")  # Save this - shown only once!

# List tokens
tokens = client.auth.list_tokens()

# Rotate token
rotated = client.auth.rotate_token(token_id=123)
print(f"New token: {rotated.new_token}")

# Revoke token
client.auth.revoke_token(token_id=123)
```

## Enterprise Operations

These require an Arc server with the corresponding enterprise license feature.
Without it, calls raise `ArcLicenseError` (a subclass of `ArcAuthenticationError`).
Each sub-client is a thin wrapper that returns the server's JSON.

```python
# RBAC: organizations -> teams -> roles -> measurement permissions
org = client.rbac.create_organization("acme")
client.rbac.add_token_to_team(token_id=123, team_id=1)

# Query governance (per-token rate limits & quotas)
client.governance.create_policy(token_id=123, rate_limit_per_minute=600)
print(client.governance.usage(123))

# Audit logs
client.audit.logs(event_type="query", since="2024-01-01T00:00:00Z", limit=100)

# Query management
client.queries.active()
client.queries.kill("query-id")

# Cluster / compaction / tiering / backup / MQTT
client.cluster.status()
client.compaction.trigger(tier="hourly,daily")
client.tiering.set_policy("metrics", hot_only=True, hot_max_age_days=7)
client.backup.create()
client.mqtt.list_subscriptions()
```

## Configuration

```python
from arc_client import ArcClient, Compression

client = ArcClient(
    host="localhost",              # Arc server hostname
    port=8000,                     # Arc server port (default: 8000)
    token="your-token",            # API token
    database="default",            # Default database
    timeout=30.0,                  # Request timeout in seconds
    compression=Compression.ZSTD,  # "zstd" (default), "gzip", "none", or a bool
    ssl=False,                     # Use HTTPS
    verify_ssl=True,               # Verify SSL certificates
    max_retries=3,                 # Retries for connection errors / 429 / 503
    retry_backoff=0.5,             # Base backoff (seconds) for exponential retry
)
```

`compression` accepts a `Compression` enum, a string (`"zstd"`/`"gzip"`/`"none"`),
or a bool for backward compatibility (`True` → gzip, `False` → none). Arc detects
the codec from the payload's magic bytes, so no `Content-Encoding` header is sent.

## Error Handling

```python
from arc_client.exceptions import (
    ArcError,              # Base exception
    ArcConnectionError,    # Connection failures
    ArcAuthenticationError,# Token missing, invalid or expired (401)
    ArcPermissionError,    # Token lacks the required permission (403)
    ArcScopedAccessError,  # Token may read, but not this database (403)
    ArcPermissionDataUnavailableError, # Arc could not load the grants (403)
    ArcLicenseError,       # Enterprise feature not licensed (403)
    ArcQueryError,         # Query execution errors
    ArcIngestionError,     # Write failures
    ArcValidationError,    # Invalid input
    ArcNotFoundError,      # Resource not found (404)
    ArcRateLimitError,     # Rate limited (429)
    ArcNotSupportedError,  # Operation unsupported by server (501)
    ArcServerError,        # Server errors (5xx)
)

try:
    client.query.query("INVALID SQL")
except ArcQueryError as e:
    print(f"Query failed: {e}")
except ArcConnectionError as e:
    print(f"Connection failed: {e}")
```

## Documentation

See the [full documentation](https://docs.basekick.net/arc/sdks/python]) for detailed guides and API reference.

## License

MIT License - see [LICENSE](LICENSE) for details.


## Server compatibility and safe management updates

The SDK distinguishes partial updates from complete replacements:

- `update()` sends only supplied non-`None` fields, without fetching a definition.
  Use this on servers with partial PUT semantics. Existing `None` arguments remain omitted;
  use `from arc_client import CLEAR` to send JSON null for nullable update fields.
- `replace()` sends every mutable field explicitly in one PUT. Arc 26.09.3 uses
  full-definition PUT semantics for retention policies and continuous queries, so use
  `replace()` on that release. The method also works on newer servers. It intentionally
  overwrites the whole definition; coordinate concurrent edits before calling it.

There is no server-capability guessing or retry with a different update payload.
Both sync and async clients provide the same methods. CQ replacements include
`tag_columns`; retention replacements include `database`, `measurement`, and `buffer_days`.

Query results retain `truncated`, `truncation_reason`, `rows_capped`, and `row_cap`
when their wire format supplies them. Experimental `query_msgpack()` returns UTC
`datetime` values when a timestamp is exactly representable at microsecond resolution.
Submicrosecond or out-of-range timestamps remain `msgpack.Timestamp`, preserving
`.seconds`, `.nanoseconds`, and `.to_unix_nano()`. Such cells require an explicit
serialization choice when exporting JSON. Arrow completion metadata is available
in the returned table schema as described above.

DELETE results expose failed, unreachable, unflushed, and cold-tier omission details.
HTTP 207 remains a result that callers must inspect. Non-2xx typed exceptions retain
structured details in `error.response_body`. Data DELETE requests are never automatically
replayed after a server or transport failure, because work may already have committed.

Automatic 429/503 and ambiguous transport retries apply only to read-only HTTP methods.
Other mutations retry only connection-establishment failures; unknown write outcomes
are surfaced for the caller to reconcile, rather than replayed automatically.

Query requests already carry the configured database in `x-arc-database`. Use
unqualified measurement names in those SQL requests, for example
`client.query.query("SELECT * FROM cpu LIMIT 10")`.
