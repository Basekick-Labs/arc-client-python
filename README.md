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
- **Multiple formats**: MessagePack columnar/row, InfluxDB Line Protocol
- **DataFrame integration**: Pandas, Polars, PyArrow
- **Sync and async APIs**: Full async support with httpx
- **Buffered writes**: Automatic batching with size and time thresholds
- **Query support**: SQL queries with JSON or Arrow IPC streaming
- **Management**: Retention policies, continuous queries, delete operations
- **Authentication**: Token management (create, rotate, revoke)

## Data Ingestion

### Columnar Format (Recommended)

The fastest way to write data. Uses MessagePack with gzip compression:

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

## Querying Data

### JSON Response

```python
result = client.query.query("SELECT * FROM default.cpu WHERE time > now() - INTERVAL '1 hour'")
print(result.columns)  # ['time', 'host', 'usage']
print(result.data)     # [[1633024800000000, 'server01', 45.2], ...]
print(result.row_count)
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
table = client.query.query_arrow("SELECT * FROM default.cpu LIMIT 1000")
```

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

## Configuration

```python
client = ArcClient(
    host="localhost",       # Arc server hostname
    port=8000,              # Arc server port (default: 8000)
    token="your-token",     # API token
    database="default",     # Default database
    timeout=30.0,           # Request timeout in seconds
    compression=True,       # Enable gzip compression for writes
    ssl=False,              # Use HTTPS
    verify_ssl=True,        # Verify SSL certificates
)
```

## Error Handling

```python
from arc_client.exceptions import (
    ArcError,              # Base exception
    ArcConnectionError,    # Connection failures
    ArcAuthenticationError,# Auth failures (401)
    ArcQueryError,         # Query execution errors
    ArcIngestionError,     # Write failures
    ArcValidationError,    # Invalid input
    ArcNotFoundError,      # Resource not found (404)
    ArcRateLimitError,     # Rate limited (429)
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

See the [full documentation]([https://docs.basekick.net/arc/sdks/python/])) for detailed guides and API reference.

## License

MIT License - see [LICENSE](LICENSE) for details.
