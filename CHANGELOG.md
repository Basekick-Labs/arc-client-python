# Changelog

All notable changes to `arc-tsdb-client` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-07-03

A large feature-parity release bringing the SDK up to date with the Arc server,
plus bug fixes and the first real HTTP-layer test coverage.

### Added

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
- **Automatic retries** with exponential backoff + jitter for connection errors,
  429 (honoring `Retry-After`), and 503. Configurable via `max_retries` /
  `retry_backoff`.
- New exceptions: `ArcNotSupportedError` (501) and `ArcLicenseError` (403 for a
  missing enterprise-license feature; subclass of `ArcAuthenticationError`).
- Real HTTP round-trip test coverage (via `pytest-httpx`) for headers, error
  mapping, compression on the wire, retries, msgpack decode, imports, databases,
  and enterprise wrappers — sync and async. Opt-in live integration tests under
  `tests/integration` (gated on `ARC_TEST_URL`).

### Changed

- Default write compression is now **zstd** (was gzip). Passing
  `compression=True` still maps to gzip for backward compatibility.
- Writes no longer send a `Content-Encoding` header; Arc auto-detects the codec
  from the payload's magic bytes.
- `User-Agent` is now derived from the package version instead of a hardcoded
  string, and the package version is a single source of truth read by the build.

### Fixed

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
