# Changelog

All notable changes to `arc-tsdb-client` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **`db.table` queries work without naming a database
  ([#35](https://github.com/Basekick-Labs/arc-client-python/issues/35)).**
  The client sent `x-arc-database: default` on every query, and Arc rejects
  `db.table` names while that header is set, so
  `client.query.query_pandas("SELECT * FROM default.cpu")` failed with 400.
  Queries (JSON, msgpack, Arrow, estimate) now send the header only for a
  database you chose: `database=` on the call, or on `ArcClient` /
  `AsyncArcClient`. Without one, Arc resolves bare names to `default`, as
  before.
  - Writes and imports still target `default` when no database is given.
  - Setting a client database (even `database="default"`) keeps the old
    behaviour: bare names resolve to it and `db.table` is rejected. Pass
    `database=` per call to query another database.
  - `ArcClient(database=...)` and `ClientConfig.database` now default to
    `None`. `ClientConfig.default_database` is what writes use (`"default"`
    when unset) and `ClientConfig.query_database` what queries send.
  - Arc's audit log records the database from the header, so a query without
    one is logged with an empty database instead of `default`.

## [0.2.0] - 2026-11-05

A large feature-parity release bringing the SDK up to date with the Arc server,
plus bug fixes and the first real HTTP-layer test coverage.

### Added

- **Write path (Arc 2027.01.1 parity).**
  - `client.write.flush_server_buffers()` (sync + async): flushes the node's
    ingest buffers to Parquet now (`POST /api/v1/write/line-protocol/flush`,
    admin). Arc flushes every protocol and database on that node.
  - `ArcPartialWriteError` (`dropped`, `others_written`) for Arc's
    `400 "partial write"`. It is never retried, and buffered writers discard
    that batch and report it once, so a resend cannot duplicate stored rows.
  - `ArcIngestRefusedError` (`reason`, `retry_safe`) for 503 refusals
    (`X-Arc-Ingest-Fenced`, `X-Arc-Ingest-Refused`). Writes are resent
    automatically except for `wal_not_durable` and line-protocol bodies
    spanning several measurements, where Arc re-checks its fence per
    measurement. A buffered writer keeps a refused batch and stays usable.
  - `precision=` on `write_line_protocol`/`write_point`.
  - `tag_columns=` on `write_columnar`/`write_dataframe` and buffered writes,
    sent as msgpack `tag_keys`.
- **Query path.**
  - Result completeness (Arc#726): `QueryResponse.truncated` and
    `truncation_reason` say Arc's stream failed part way, so `data` is missing
    rows and must not be treated as the full result. `query()` returns them
    rather than raising, so check `truncated` before using `data`.
    `rows_capped` and `row_cap` say a governance row cap stopped the query:
    the result is valid up to the cap. On Arrow, a truncated stream raises
    `ArcQueryError` and the cap is in the schema metadata (`arc.rows_capped`,
    `arc.row_cap`); on MessagePack, an incomplete body raises `ArcQueryError`.
  - `profile=True` on `query`/`query_msgpack`, returned as `QueryResponse.profile`.
  - `dictionary=`/`compression=` Arrow wire options.
  - `QueryResponse.query_id`, and `arc.query_id` in the Arrow schema metadata,
    for `client.queries.kill()`.
  - `client.query.query_measurement()` for `GET /api/v1/query/:measurement`.
- **Measurement schema**: `databases.get_measurement_schema()` and
  `rebuild_measurement_schema()`.
- **Readiness**: `client.readiness()` and `client.write_readiness()` return
  `ReadyResponse` (`status`, `reason`, `tier`). A 503 is an answer, not an
  error, and is not retried.
- **Compaction**:
  - `compaction.cycles()`, `compaction.cycle(id)`, `compaction.too_wide()`,
    `compaction.clear_too_wide()` and `history(cycle_id=)` (Arc 2027.01.1+).
  - `trigger(measurement=)` (Arc 2026.09.2+).
- **Backup**: `create(databases=[...])`, `list(include_foreign=True)` and
  `restore(mode="merge"|"replace")` (Arc 2027.01.1+). Restore still requires
  `confirm=True`.
- **Delete**: `allow_unreachable=True` on `delete.delete()` and
  `databases.delete()` (Arc 2027.01.1+). A cluster delete then proceeds
  despite unreachable files or unflushed peers, leaves those rows in place,
  and names them in the response.
- **Continuous queries**: `ExecuteCQResponse.watermark_status` (Arc 2027.01.1+).
- **Cluster operator controls** (admin):
  - `delete_file`, `demote_writer`, `assign_compactor`, `converge_voters`
    (Arc 2026.09.2+).
  - `stream_claims`, `release_stream_claim`, `abort_compaction_job`,
    `policies`, `adopt_policies` (Arc 2027.01.1+).
  - Destructive controls require `confirm=True`. `converge_voters` only
    plans unless `dry_run=False, confirm=True`.
- **Reconciliation** (`client.reconciliation`): `status()`, `trigger()` and
  `get_run()`. `trigger()` is a dry run by default; acting requires
  `dry_run=False, confirm=True`.
- **Operational diagnostics** (`client.diagnostics`):
  - Public JSON metrics, Prometheus text and timeseries.
  - Admin `logs`, `memstats`, `duckdb_memory` and
    `free_os_memory(confirm=True)`.
  - Write-protocol diagnostics (any token, Arc 25.12.1+):
    `line_protocol_stats()`, `msgpack_stats()`, `msgpack_spec()` and
    `line_protocol_health()`. Arc's Line Protocol health always answers
    healthy; `write_readiness()` says whether the node takes writes.
  - `auth.cache_stats()` and `auth.invalidate_cache()`.
- **Edge sync administration** (`client.edge_sync`):
  - Hub: spoke registry and air-gap bundle import.
  - Spoke: status, ledger, sync, requeue/dismiss, and bundle export/ack.
  - `rotate_spoke_secret`, `delete_spoke` and `dismiss_failed` require
    `confirm=True`.

- **Installation ID for Arc's client telemetry.** Requests carry a random
  per-installation `Arc-Client-Installation-Id` and a `User-Agent` that names
  the Python version and platform, so Arc's opt-out telemetry can count SDK
  installations the same way it counts arcli. The SDK itself still talks only
  to the configured Arc server. Opt out with `DO_NOT_TRACK=1`,
  `ARC_CLIENT_SEND_INSTALLATION_ID=0` or `send_installation_id=False`; pin the
  ID in containers with `ARC_CLIENT_INSTALLATION_ID`. See README, "Privacy".

- **Scheduler management** (`client.schedulers`, sync + async): inspect CQ and retention
  scheduler status, reload active continuous-query schedules, and manually trigger
  retention. The status route accepts authenticated tokens; control routes preserve
  Arc administrator and enterprise-license gates. Retention triggers require
  `confirm=True` because they may permanently delete data. Supported by Arc 26.09.3 and later.
  On Arc 27.01.1 and later, a control refused because the license is missing, expired
  or of a tier without the scheduler raises `ArcLicenseError` (HTTP 403); a scheduler
  off in config is `ArcServerError` with `status_code == 503`, and a retention trigger
  on a node that is not the primary writer, or while a cycle runs, is `ArcServerError`
  with `status_code == 409`.
- Arrow completion metadata in the existing `pyarrow.Table` schema for sync and
  async queries: policy cap and execution time are retained; failed, malformed,
  conflicting or unobservable announced completion metadata is rejected.
  Native column types and unrelated schema metadata remain intact. Trailer
  capture uses explicitly bounded HTTPX/httpcore/h11 compatibility versions.
  Promoted timing headers cannot prove uncapped completion when final trailers
  are missing.


- **MessagePack query** (`query.query_msgpack`, experimental): decodes the
  columnar MessagePack response with native timestamp handling and returns the
  same `QueryResponse` shape as `query()` (with per-column Arrow `types`). Raises
  `ArcNotSupportedError` when the server lacks the `duckdb_arrow` build tag (501).
- **Bulk import client** (`client.import_`): `import_csv`, `import_parquet`,
  `import_lp`, `import_tle`, and `import_stats`. Accepts paths, file-like objects,
  or bytes. New models: `ImportResult`, `LPImportResult`, `TLEImportResult`,
  `ImportStats`.
- **TLE streaming write** (`write.write_tle`) and `write.tle_stats`.
- **Databases management** (`client.databases`): `list`, `get`,
  `list_measurements`, `create`, `delete`. New models under `models.database`.
- **Enterprise sub-clients** (sync + async): `rbac`, `governance`, `audit`,
  `queries` (query management), `cluster`, `compaction`, `tiering`, `backup`,
  `mqtt`.
- **zstd write compression** (now the default; gzip and none still supported).
  Compression is configurable via a `Compression` enum, a string, or a bool.
- **Automatic retries** with exponential backoff + jitter for read-only
  connection/transport failures, 429 (honoring `Retry-After`) and 503. Mutations
  retry only connection establishment; data DELETE never retries. Configure
  via `max_retries` / `retry_backoff`.
- New exceptions: `ArcNotSupportedError` (501) and `ArcLicenseError` (403 for a
  missing enterprise-license feature; subclass of `ArcAuthenticationError`).
- `ArcPermissionError` (403: the token lacks the permission the endpoint
  requires), `ArcScopedAccessError` (403 from Arc's per-database check: the
  token may read, but not the database asked for) and
  `ArcPermissionDataUnavailableError` (403 because Arc could not load the
  token's grants and failed closed — a server-side fault, not a statement
  about the token). `ArcScopedAccessError` carries the refused database on
  `.database`, empty or `"*"` when the call asked for every database. The
  latter two subclass `ArcPermissionError`, which subclasses
  `ArcAuthenticationError`, so existing `except` blocks keep working.
- Real HTTP round-trip test coverage (via `pytest-httpx`) for headers, error
  mapping, compression on the wire, retries, msgpack decode, imports, databases,
  and enterprise wrappers — sync and async. Opt-in live integration tests under
  `tests/integration`: a single node (`ARC_TEST_URL`), a local-storage cluster
  (`ARC_TEST_CLUSTER_URLS`, with an opt-in stream-takeover claim test), and a
  shared-storage cluster with a compactor node, a cold tier and MQTT
  (`ARC_TEST_SHARED_URLS`).

### Changed

- MessagePack queries are stable. A stream that fails after the 200 raises
  `ArcQueryError` (incomplete body) instead of returning partial rows.
- `ready()` is a single probe: a 503 "not ready" is no longer retried with backoff.
- `httpcore` is bounded `>=1.0.9,<1.1` instead of pinned `==1.0.9`.
- Compaction docstrings state that every `/api/v1/compaction` route, reads
  included, requires an admin token, as Arc enforces.
- `User-Agent` now includes the Python version and platform
  (`arc-client-python/0.2.0 (python/3.12.4; linux/amd64)`).

- Default write compression is now **zstd** (was gzip). Passing
  `compression=True` still maps to gzip for backward compatibility.
- Writes no longer send a `Content-Encoding` header; Arc auto-detects the codec
  from the payload's magic bytes.
- `User-Agent` is now derived from the package version instead of a hardcoded
  string, and the package version is a single source of truth read by the build.
- HTTP 403 no longer collapses onto `ArcAuthenticationError`, which now means
  only 401. The database listing endpoints need read permission and, where the
  server restricts reads per database, a grant for the database — and Arc
  refuses `databases.list()` for a tenant-scoped token rather than returning a
  filtered list, matching `SHOW DATABASES`. That refusal is an expected state,
  not a fault to retry: it raises `ArcScopedAccessError` naming the database,
  while a token with no read permission raises `ArcPermissionError`.

### Fixed

- Query POSTs (`query`, `query_arrow`, `query_msgpack`, `estimate`) now retry
  503 with `Retry-After`, which Arc sends during replication catch-up, for a
  vanished partition, and when the cold tier is unavailable. A governance 429
  (rate limit or quota) is raised at once. A transport failure mid-response
  is still not replayed.
- Query and write clients no longer hide typed errors. Auth, permission,
  scoped-access, license, rate-limit and not-supported errors surface as
  themselves.
- `create_token(permissions=[])` and `update_token(permissions=[])` send the
  empty list, creating an RBAC-only token, instead of silently producing a
  read/write token. On Arc, such a token gets access only where RBAC grants
  are consulted (the `/api/v1/databases` routes); SQL query routes check the
  coarse `read` permission, so it cannot run queries.
- `HealthResponse` models Arc's real `/health` (`time`, `uptime_sec`,
  `storage`, `license`).
- `dataframe_to_columnar(tag_columns=...)` is no longer a no-op.

- MessagePack queries preserve cap metadata and exact timestamp nanoseconds.
  DELETE results retain partial-failure, cold-storage and peer diagnostics.
- Retention and continuous-query clients accept Arc's list/null/flat response
  shapes. Sparse updates remain sparse; explicit full replacements support
  released servers, and CLEAR requests explicit null values on newer servers.
- Buffered close waits for active writes, retains unacknowledged batches and
  surfaces failures without replay. Typed errors retain structured diagnostics.

- Polars datetime → columnar conversion used a fragile private attribute that
  raised at runtime; it now casts via the public `polars` API and normalizes the
  series' `time_unit` to microseconds.
- Pandas `datetime64[us]` (and `ms`/`s`) timestamps were mis-scaled by assuming
  nanoseconds; conversion now respects the column's resolution — including
  tz-naive columns, whose numpy `datetime64` dtype exposes no `.unit` (the unit
  is read via `numpy.datetime_data`). Previously these landed ~1000x too small.
- The buffered writer's time-based flush was passive (fired only on the next
  write). It now runs a background timer (thread for sync, task for async) so
  idle buffers still flush on `flush_interval`.
- `show_tables` interpolated the database name into SQL without validation; the
  identifier is now validated against Arc's naming rules.
- Removed the dead, empty `dataframe` package stub.

## [0.1.2]

- Pad missing columns with `None` in the buffered columnar merge (#202).
