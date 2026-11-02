"""Live parity tests for the 0.2.0 SDK surface against a real Arc server.

Opt-in. Every test is skipped unless ``ARC_TEST_URL`` is set. They write,
delete, back up and restore data, so point them at a throwaway server only.

Environment:

``ARC_TEST_URL`` / ``ARC_TEST_TOKEN``
    The Arc server and an admin token. Required for everything.
``ARC_TEST_LICENSED=1``
    The server has an enterprise license with governance, RBAC, audit,
    query management, backup and compaction enabled. Gates those tests.
``ARC_TEST_STORAGE_DIR``
    The server's local ``storage.local_path`` (e.g. ``<workdir>/data/arc``),
    readable from the test process. Gates the checks that read the Parquet
    files Arc wrote (``arc:tags`` metadata).
``ARC_TEST_HUB_URL`` / ``ARC_TEST_HUB_TOKEN``
    An Arc started with ``edge_sync.enabled`` and ``edge_sync.import.enabled``.
``ARC_TEST_SPOKE_URL`` / ``ARC_TEST_SPOKE_TOKEN`` / ``ARC_TEST_SPOKE_ID``
    An Arc started with ``edge_sync.spoke.enabled`` and
    ``edge_sync.spoke.bundle.enabled``, registered on the hub under
    ``ARC_TEST_SPOKE_ID`` (default ``spoke-live``).
``ARC_TEST_EDGE_BUNDLE_DIR``
    A directory inside both the spoke's ``edge_sync.spoke.bundle.allowed_dirs``
    and the hub's ``edge_sync.import.allowed_dirs`` (same filesystem).

Run::

    ARC_TEST_URL=http://localhost:18081 ARC_TEST_TOKEN=<admin> ARC_TEST_LICENSED=1 \\
    ARC_TEST_STORAGE_DIR=<workdir>/data/arc \\
    ARC_TEST_HUB_URL=http://localhost:18082 ARC_TEST_HUB_TOKEN=<hub admin> \\
    ARC_TEST_SPOKE_URL=http://localhost:18083 ARC_TEST_SPOKE_TOKEN=<spoke admin> \\
    ARC_TEST_EDGE_BUNDLE_DIR=<shared dir> \\
    uv run pytest tests/integration/test_live_parity.py -v

The edge-sync hub needs ``ARC_ENCRYPTION_KEY`` (32 bytes, base64) and
``edge_sync.hub_id``; the spoke needs ``ARC_EDGE_SYNC_SPOKE_SECRET`` (from
``client.edge_sync.register_spoke`` on the hub), ``ARC_EDGE_SYNC_HUB_TOKEN``
(a hub token with write permission), ``edge_sync.spoke.hub_url``,
``spoke_id`` and ``hub_id``. Keep the spoke's ``sync_interval`` long (e.g.
``1h``) so nothing syncs between the steps of a test.

Tests that pin down a behavior the SDK merely passes through (e.g. cluster
reads on a standalone node) say so in their docstring.
"""

from __future__ import annotations

import contextlib
import glob
import os
import threading
import time
import uuid
from typing import Any, Callable, Iterator, Optional
from urllib.parse import urlparse

import httpx
import pytest

from arc_client import ArcClient, AsyncArcClient
from arc_client.exceptions import (
    ArcAuthenticationError,
    ArcError,
    ArcNotFoundError,
    ArcPartialWriteError,
    ArcPermissionError,
    ArcQueryError,
    ArcRateLimitError,
    ArcServerError,
    ArcValidationError,
)

ARC_URL = os.environ.get("ARC_TEST_URL")
ARC_TOKEN = os.environ.get("ARC_TEST_TOKEN")
LICENSED = os.environ.get("ARC_TEST_LICENSED") == "1"
STORAGE_DIR = os.environ.get("ARC_TEST_STORAGE_DIR")
HUB_URL = os.environ.get("ARC_TEST_HUB_URL")
HUB_TOKEN = os.environ.get("ARC_TEST_HUB_TOKEN")
SPOKE_URL = os.environ.get("ARC_TEST_SPOKE_URL")
SPOKE_TOKEN = os.environ.get("ARC_TEST_SPOKE_TOKEN")
SPOKE_ID = os.environ.get("ARC_TEST_SPOKE_ID", "spoke-live")
BUNDLE_DIR = os.environ.get("ARC_TEST_EDGE_BUNDLE_DIR")

pytestmark = pytest.mark.skipif(
    not ARC_URL,
    reason="Set ARC_TEST_URL (and ARC_TEST_TOKEN) to run live parity tests.",
)

licensed = pytest.mark.skipif(
    not LICENSED, reason="Set ARC_TEST_LICENSED=1 for enterprise-licensed targets."
)
needs_storage = pytest.mark.skipif(
    not STORAGE_DIR, reason="Set ARC_TEST_STORAGE_DIR to the server's local storage path."
)
needs_hub = pytest.mark.skipif(
    not (HUB_URL and HUB_TOKEN), reason="Set ARC_TEST_HUB_URL/ARC_TEST_HUB_TOKEN."
)
needs_spoke = pytest.mark.skipif(
    not (HUB_URL and HUB_TOKEN and SPOKE_URL and SPOKE_TOKEN),
    reason="Set ARC_TEST_HUB_URL/_TOKEN and ARC_TEST_SPOKE_URL/_TOKEN.",
)
needs_bundle = pytest.mark.skipif(
    not (HUB_URL and HUB_TOKEN and SPOKE_URL and SPOKE_TOKEN and BUNDLE_DIR),
    reason="Set the hub/spoke variables and ARC_TEST_EDGE_BUNDLE_DIR.",
)

INSTALLATION_HEADER = "Arc-Client-Installation-Id"
FLUSH_TIMEOUT = 20.0


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _target(url: Optional[str]) -> dict[str, Any]:
    parsed = urlparse(url or "http://localhost:8000")
    ssl = parsed.scheme == "https"
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or (443 if ssl else 80),
        "ssl": ssl,
    }


def make_client(url: Optional[str] = None, token: Optional[str] = None, **kwargs: Any) -> ArcClient:
    return ArcClient(**_target(url or ARC_URL), token=token or ARC_TOKEN, **kwargs)


def make_async_client(
    url: Optional[str] = None, token: Optional[str] = None, **kwargs: Any
) -> AsyncArcClient:
    return AsyncArcClient(**_target(url or ARC_URL), token=token or ARC_TOKEN, **kwargs)


def uid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def server_flush(url: Optional[str] = None, token: Optional[str] = None) -> None:
    """Flush Arc's ingest buffers (POST /api/v1/write/line-protocol/flush, admin).

    Not an SDK method; used only so the tests need not wait out
    ``ingest.max_buffer_age_ms``.
    """
    response = httpx.post(
        f"{url or ARC_URL}/api/v1/write/line-protocol/flush",
        headers={"Authorization": f"Bearer {token or ARC_TOKEN}"},
        timeout=30,
    )
    response.raise_for_status()


def wait_rows(
    client: ArcClient,
    sql: str,
    database: str,
    expected: int,
    timeout: float = FLUSH_TIMEOUT,
    url: Optional[str] = None,
    token: Optional[str] = None,
) -> list[list[Any]]:
    """Flush, then poll ``sql`` until it returns ``expected`` rows (or time out)."""
    deadline = time.monotonic() + timeout
    rows: list[list[Any]] = []
    while True:
        server_flush(url, token)
        try:
            rows = client.query.query(sql, database).data
        except ArcQueryError:
            rows = []  # table may not exist until the first flush lands
        if len(rows) >= expected or time.monotonic() >= deadline:
            return rows
        time.sleep(0.3)


class RequestCounter:
    """Count HTTP requests an SDK client sends, via an httpx event hook."""

    def __init__(self, client: ArcClient) -> None:
        self.requests: list[httpx.Request] = []
        http_client = client._get_http()._get_client()
        http_client.event_hooks["request"].append(self.requests.append)

    def count(self, path_fragment: str = "") -> int:
        return sum(1 for r in self.requests if path_fragment in r.url.path)


@contextlib.contextmanager
def timed() -> Iterator[Callable[[], float]]:
    start = time.monotonic()
    yield lambda: time.monotonic() - start


def parquet_files(database: str, measurement: str) -> list[str]:
    assert STORAGE_DIR
    return glob.glob(
        os.path.join(STORAGE_DIR, database, measurement, "**", "*.parquet"), recursive=True
    )


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


@pytest.fixture
def admin() -> Iterator[ArcClient]:
    with make_client() as client:
        yield client


@pytest.fixture
async def aadmin() -> Any:
    async with make_async_client() as client:
        yield client


@pytest.fixture
def db(admin: ArcClient) -> Iterator[str]:
    """A fresh database, deleted afterwards."""
    name = uid("sdk_lp")
    admin.databases.create(name)
    yield name
    with contextlib.suppress(ArcError):
        admin.databases.delete(name, confirm=True)


class TokenFactory:
    def __init__(self, admin: ArcClient) -> None:
        self.admin = admin
        self.ids: list[int] = []

    def create(self, permissions: Optional[list[str]], **kwargs: Any) -> tuple[int, str]:
        name = uid("sdk_tok")
        created = self.admin.auth.create_token(name, permissions=permissions, **kwargs)
        assert created.success and created.token
        token_id = next(t.id for t in self.admin.auth.list_tokens() if t.name == name)
        self.ids.append(token_id)
        return token_id, created.token

    def cleanup(self) -> None:
        for token_id in self.ids:
            with contextlib.suppress(ArcError):
                self.admin.auth.delete_token(token_id)


@pytest.fixture
def tokens(admin: ArcClient) -> Iterator[TokenFactory]:
    factory = TokenFactory(admin)
    yield factory
    factory.cleanup()


@pytest.fixture
def seeded(admin: ArcClient, db: str) -> tuple[str, str, int]:
    """A measurement with three rows (tag ``host``), flushed and queryable."""
    measurement = uid("cpu")
    base = (int(time.time()) - 60) * 1_000_000
    admin.write.write_columnar(
        measurement,
        {
            "time": [base, base + 1_000_000, base + 2_000_000],
            "host": ["a", "a", "b"],
            "usage": [1.0, 2.0, 3.0],
        },
        database=db,
        tag_columns=["host"],
    )
    rows = wait_rows(admin, f"SELECT * FROM {measurement}", db, 3)
    assert len(rows) == 3
    return db, measurement, base


# --------------------------------------------------------------------------
# Health and readiness
# --------------------------------------------------------------------------


def test_health_model_fields(admin: ArcClient) -> None:
    health = admin.health()
    assert health.status == "ok"
    assert health.time
    assert health.uptime and health.uptime_sec and health.uptime_sec > 0
    assert health.storage is not None and "hot" in health.storage
    assert health.storage["hot"].state == "ok"
    assert health.storage["hot"].backend
    if LICENSED:
        assert health.license is not None
        assert health.license.status == "active"
        assert health.license.tier
        assert health.license.days_remaining > 0


def test_readiness_and_write_readiness(admin: ArcClient) -> None:
    ready = admin.readiness()
    assert ready.ready is True
    assert ready.status == "ready"
    assert ready.uptime_sec and ready.uptime_sec > 0
    write_ready = admin.write_readiness()
    assert write_ready.ready is True
    assert write_ready.status == "ready"
    assert admin.ready() is True


def test_ready_false_when_unreachable() -> None:
    with ArcClient(host="127.0.0.1", port=1, max_retries=0, timeout=2) as client, timed() as t:
        assert client.ready() is False
    assert t() < 5


async def test_async_health_and_readiness(aadmin: AsyncArcClient) -> None:
    health = await aadmin.health()
    assert health.status == "ok" and health.storage
    assert (await aadmin.readiness()).ready is True
    assert (await aadmin.write_readiness()).status == "ready"
    assert await aadmin.ready() is True


# --------------------------------------------------------------------------
# Query
# --------------------------------------------------------------------------


def test_query_json_profile_and_query_id(admin: ArcClient, seeded: tuple[str, str, int]) -> None:
    db, measurement, _ = seeded
    result = admin.query.query(f"SELECT host, usage FROM {measurement} ORDER BY time", db)
    assert result.row_count == 3
    assert result.data == [["a", 1.0], ["a", 2.0], ["b", 3.0]]
    assert result.query_id, "X-Arc-Query-ID not surfaced on JSON query"
    assert result.profile is None

    profiled = admin.query.query(f"SELECT count(*) FROM {measurement}", db, profile=True)
    assert profiled.profile is not None, "profile=True returned no profile"
    assert profiled.profile.total_ms is not None and profiled.profile.total_ms >= 0


def test_query_msgpack_profile_and_query_id(admin: ArcClient, seeded: tuple[str, str, int]) -> None:
    db, measurement, _ = seeded
    result = admin.query.query_msgpack(
        f"SELECT host, usage FROM {measurement} ORDER BY time", db, profile=True
    )
    assert result.columns == ["host", "usage"]
    assert result.data == [["a", 1.0], ["a", 2.0], ["b", 3.0]]
    assert result.query_id, "X-Arc-Query-ID not surfaced on msgpack query"
    assert result.profile is not None, "profile=True returned no profile on msgpack"


def test_query_arrow_metadata_dictionary_compression(
    admin: ArcClient, seeded: tuple[str, str, int]
) -> None:
    import pyarrow as pa

    db, measurement, _ = seeded
    sql = f"SELECT host, usage FROM {measurement} ORDER BY time"
    plain = admin.query.query_arrow(sql, db)
    assert plain.num_rows == 3
    meta = plain.schema.metadata or {}
    assert meta.get(b"arc.query_id"), f"arc.query_id missing from Arrow metadata: {meta}"
    assert not pa.types.is_dictionary(plain.schema.field("host").type)

    # Arc dictionary-encodes only from 256 rows and <= rows/4 uniques
    # (internal/api/query_arrow_dict.go dictMinRows).
    small = admin.query.query_arrow(sql, db, dictionary=True)
    assert small.column("host").to_pylist() == ["a", "a", "b"]
    many = "SELECT 'h' || (i % 3)::VARCHAR AS host, i FROM range(1000) t(i) ORDER BY i"
    dictionary = admin.query.query_arrow(many, db, dictionary=True)
    assert pa.types.is_dictionary(dictionary.schema.field("host").type), dictionary.schema
    assert dictionary.column("host").to_pylist()[:4] == ["h0", "h1", "h2", "h0"]
    assert not pa.types.is_dictionary(admin.query.query_arrow(many, db).schema.field("host").type)

    for codec in ("zstd", "lz4"):
        compressed = admin.query.query_arrow(sql, db, compression=codec)
        assert compressed.column("usage").to_pylist() == [1.0, 2.0, 3.0]
        assert compressed.column("host").to_pylist() == ["a", "a", "b"]

    with pytest.raises(ArcValidationError):
        admin.query.query_arrow(sql, db, compression="gzip")


def test_query_pandas_and_polars(admin: ArcClient, seeded: tuple[str, str, int]) -> None:
    db, measurement, _ = seeded
    sql = f"SELECT host, usage FROM {measurement} ORDER BY time"
    df = admin.query.query_pandas(sql, db, dictionary=True, compression="zstd")
    assert list(df["host"].astype(str)) == ["a", "a", "b"]
    assert list(df["usage"]) == [1.0, 2.0, 3.0]
    pl_df = admin.query.query_polars(sql, db, compression="lz4")
    assert pl_df["usage"].to_list() == [1.0, 2.0, 3.0]


def test_query_measurement(admin: ArcClient, seeded: tuple[str, str, int]) -> None:
    db, measurement, _ = seeded
    result = admin.query.query_measurement(
        measurement, db, limit=2, order_by="time ASC", where="host = 'a'"
    )
    usage = result.columns.index("usage")
    assert [row[usage] for row in result.data] == [1.0, 2.0]
    offset = admin.query.query_measurement(measurement, db, limit=10, offset=2, order_by="time ASC")
    assert offset.row_count == 1
    default = admin.query.query_measurement(measurement, db)
    assert default.row_count == 3


def test_estimate(admin: ArcClient, seeded: tuple[str, str, int]) -> None:
    db, measurement, _ = seeded
    estimate = admin.query.estimate(f"SELECT * FROM {measurement}", db)
    assert estimate.success
    assert estimate.estimated_rows is not None


@licensed
def test_query_id_lookup_and_kill_long_query(admin: ArcClient) -> None:
    """A finished query is in history by its query_id; a running one can be killed."""
    finished = admin.query.query("SELECT 42 AS answer")
    assert finished.query_id
    info = admin.queries.get(finished.query_id)
    assert finished.query_id in str(info)

    sql = "SELECT sum(i % 7) FROM range(400000000000) t(i)"
    outcome: dict[str, Any] = {}

    def run() -> None:
        with make_client(timeout=120, max_retries=0) as long_client:
            try:
                outcome["result"] = long_client.query.query(sql)
            except BaseException as e:  # noqa: BLE001
                outcome["error"] = e

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    query_id = None
    deadline = time.monotonic() + 15
    while query_id is None and time.monotonic() < deadline:
        for q in admin.queries.active().get("queries") or []:
            if "range(400000000000)" in str(q.get("sql", "")):
                query_id = q.get("id") or q.get("query_id")
        time.sleep(0.2)
    assert query_id, "long query never showed up in queries.active()"
    killed = admin.queries.kill(query_id)
    assert killed
    thread.join(30)
    assert not thread.is_alive(), "killed query did not return"
    assert "error" in outcome, f"killed query returned a result: {outcome}"
    assert isinstance(outcome["error"], ArcError)


async def test_async_query_surfaces(aadmin: AsyncArcClient, seeded: tuple[str, str, int]) -> None:
    db, measurement, _ = seeded
    result = await aadmin.query.query(
        f"SELECT usage FROM {measurement} ORDER BY time", db, profile=True
    )
    assert result.data == [[1.0], [2.0], [3.0]]
    assert result.query_id and result.profile is not None
    table = await aadmin.query.query_arrow(f"SELECT host FROM {measurement}", db, dictionary=True)
    assert (table.schema.metadata or {}).get(b"arc.query_id")
    by_name = await aadmin.query.query_measurement(measurement, db, limit=1)
    assert by_name.row_count == 1


# --------------------------------------------------------------------------
# Governance: a 429 must be raised at once, never retried
# --------------------------------------------------------------------------


def _rate_limited_client(
    admin: ArcClient, tokens: TokenFactory
) -> tuple[ArcClient, RequestCounter, int]:
    token_id, token = tokens.create(["read"])
    admin.governance.create_policy(token_id, rate_limit_per_minute=1)
    client = make_client(token=token, max_retries=3, retry_backoff=2.0)
    return client, RequestCounter(client), token_id


@licensed
@pytest.mark.parametrize("surface", ["query", "query_arrow", "query_msgpack", "estimate"])
def test_governance_rate_limit_raised_not_retried(
    admin: ArcClient, tokens: TokenFactory, seeded: tuple[str, str, int], surface: str
) -> None:
    db, measurement, _ = seeded
    client, counter, token_id = _rate_limited_client(admin, tokens)
    sql = f"SELECT count(*) FROM {measurement}"
    try:
        call = getattr(client.query, surface)
        call(sql, db)
        with timed() as t, pytest.raises(ArcRateLimitError):
            call(sql, db)
        assert counter.count("/api/v1/query") == 2, [r.url for r in counter.requests]
        assert t() < 1.5
    finally:
        client.close()
        with contextlib.suppress(ArcError):
            admin.governance.delete_policy(token_id)


@licensed
def test_governance_rate_limit_on_query_measurement_not_retried(
    admin: ArcClient, tokens: TokenFactory, seeded: tuple[str, str, int]
) -> None:
    """GET /api/v1/query/:measurement is governed too (Arc query.go checkQueryGovernance).

    The SDK retries 429 on every GET, so a governance 429 here is retried,
    sleeping Retry-After (up to 60 s) between attempts.
    """
    db, measurement, _ = seeded
    client, counter, token_id = _rate_limited_client(admin, tokens)
    sleeps: list[float] = []
    import arc_client.http.sync_http as sync_http

    real_sleep = sync_http.time.sleep
    try:
        client.query.query_measurement(measurement, db, limit=1)
        sync_http.time.sleep = sleeps.append  # type: ignore[assignment]
        with pytest.raises(ArcRateLimitError):
            client.query.query_measurement(measurement, db, limit=1)
        assert counter.count(f"/api/v1/query/{measurement}") == 2, (
            f"governance 429 retried: {counter.count('/api/v1/query/')} requests, sleeps={sleeps}"
        )
    finally:
        sync_http.time.sleep = real_sleep  # type: ignore[assignment]
        client.close()
        with contextlib.suppress(ArcError):
            admin.governance.delete_policy(token_id)


@licensed
def test_governance_policy_crud_and_usage(admin: ArcClient, tokens: TokenFactory) -> None:
    token_id, _ = tokens.create(["read"])
    created = admin.governance.create_policy(
        token_id, rate_limit_per_minute=10, max_rows_per_query=5
    )
    assert created
    policy = admin.governance.get_policy(token_id)
    assert "10" in str(policy) and str(token_id) in str(policy)
    admin.governance.update_policy(token_id, rate_limit_per_minute=20)
    assert "20" in str(admin.governance.get_policy(token_id))
    listed = admin.governance.list_policies()
    assert any(p.get("token_id") == token_id for p in listed.get("policies", []))
    usage = admin.governance.usage(token_id)
    assert str(token_id) in str(usage)
    admin.governance.delete_policy(token_id)
    with pytest.raises(ArcNotFoundError):
        admin.governance.get_policy(token_id)


# --------------------------------------------------------------------------
# Write
# --------------------------------------------------------------------------


def test_line_protocol_precision(admin: ArcClient, db: str) -> None:
    measurement = uid("lp_prec")
    seconds = int(time.time()) - 120
    admin.write.write_line_protocol(
        f"{measurement},host=a v=1i {seconds}", database=db, precision="s"
    )
    admin.write.write_point(
        measurement,
        {"v": 2},
        tags={"host": "b"},
        timestamp=(seconds + 1) * 1000,
        database=db,
        precision="ms",
    )
    rows = wait_rows(admin, f"SELECT host, epoch_us(time) FROM {measurement} ORDER BY time", db, 2)
    assert rows == [["a", seconds * 1_000_000], ["b", (seconds + 1) * 1_000_000]]

    with pytest.raises(ArcValidationError):
        admin.write.write_line_protocol(f"{measurement} v=1i {seconds}", database=db, precision="h")


def _arc_tags(database: str, measurement: str) -> set[str]:
    import pyarrow.parquet as pq

    files = parquet_files(database, measurement)
    assert files, f"no parquet files for {database}/{measurement} under {STORAGE_DIR}"
    tags: set[str] = set()
    for path in files:
        meta = pq.read_schema(path).metadata or {}
        raw = meta.get(b"arc:tags")
        if raw:
            tags.update(t for t in raw.decode().split(",") if t)
    return tags


@needs_storage
def test_columnar_tag_columns_written_as_tags(admin: ArcClient, db: str) -> None:
    with_tags = uid("col_tags")
    without = uid("col_notags")
    base = (int(time.time()) - 60) * 1_000_000
    cols = {"time": [base, base + 1], "host": ["a", "b"], "region": ["x", "y"], "v": [1.0, 2.0]}
    admin.write.write_columnar(with_tags, dict(cols), database=db, tag_columns=["host", "region"])
    admin.write.write_columnar(without, dict(cols), database=db)
    wait_rows(admin, f"SELECT * FROM {with_tags}", db, 2)
    wait_rows(admin, f"SELECT * FROM {without}", db, 2)
    assert _arc_tags(db, with_tags) == {"host", "region"}
    assert _arc_tags(db, without) == set()


@needs_storage
def test_dataframe_tag_columns_written_as_tags(admin: ArcClient, db: str) -> None:
    import pandas as pd

    measurement = uid("df_tags")
    now = pd.Timestamp.now(tz="UTC").floor("s") - pd.Timedelta(minutes=1)
    df = pd.DataFrame(
        {"time": [now, now + pd.Timedelta(seconds=1)], "host": ["a", "b"], "v": [1.5, 2.5]}
    )
    admin.write.write_dataframe(df, measurement, database=db, tag_columns=["host"])
    rows = wait_rows(admin, f"SELECT host, v FROM {measurement} ORDER BY time", db, 2)
    assert rows == [["a", 1.5], ["b", 2.5]]
    assert _arc_tags(db, measurement) == {"host"}


def test_write_records(admin: ArcClient, db: str) -> None:
    measurement = uid("rec")
    base = (int(time.time()) - 60) * 1_000_000
    admin.write.write_records(
        [
            {
                "measurement": measurement,
                "timestamp": base,
                "tags": {"host": "a"},
                "fields": {"v": 1.0},
            },
            {
                "measurement": measurement,
                "timestamp": base + 1,
                "tags": {"host": "b"},
                "fields": {"v": 2.0},
            },
        ],
        database=db,
    )
    rows = wait_rows(admin, f"SELECT host, v FROM {measurement} ORDER BY time", db, 2)
    assert rows == [["a", 1.0], ["b", 2.0]]


def test_buffered_writer(admin: ArcClient, db: str) -> None:
    measurement = uid("buf")
    base = (int(time.time()) - 60) * 1_000_000
    client = make_client(database=db)
    with client, client.write.buffered(batch_size=1000, flush_interval=3600) as buffer:
        for i in range(5):
            buffer.write(
                {
                    "measurement": measurement,
                    "timestamp": base + i,
                    "tags": {"host": "a"},
                    "fields": {"v": float(i)},
                }
            )
        buffer.write_columnar(
            measurement, {"time": [base + 10], "host": ["b"], "v": [10.0]}, tag_columns=["host"]
        )
        assert buffer.pending_count == 6
    rows = wait_rows(admin, f"SELECT host, v FROM {measurement} ORDER BY time", db, 6)
    assert [r[1] for r in rows] == [0.0, 1.0, 2.0, 3.0, 4.0, 10.0]


def test_partial_write_line_protocol(admin: ArcClient, db: str) -> None:
    """A type conflict in one LP body: 400 "partial write", the rest stored once."""
    measurement = uid("pw")
    s = int(time.time()) - 60
    body = [
        f"{measurement},host=a v=1i {s}",
        f'{measurement},host=b v="oops" {s + 1}',
        f"{measurement},host=c v=3i {s + 2}",
    ]
    with make_client(max_retries=3, retry_backoff=2.0) as client:
        counter = RequestCounter(client)
        with timed() as t, pytest.raises(ArcPartialWriteError) as excinfo:
            client.write.write_line_protocol(body, database=db, precision="s")
    assert excinfo.value.dropped == 1
    assert excinfo.value.others_written is True
    assert counter.count("/api/v1/write/line-protocol") == 1
    assert t() < 1.5
    rows = wait_rows(admin, f"SELECT host, v FROM {measurement} ORDER BY time", db, 2)
    time.sleep(1)
    rows = wait_rows(admin, f"SELECT host, v FROM {measurement} ORDER BY time", db, 2)
    assert rows == [["a", 1], ["c", 3]]


def test_partial_write_nothing_written(admin: ArcClient, db: str) -> None:
    measurement = uid("pw_none")
    s = int(time.time()) - 60
    with pytest.raises(ArcPartialWriteError) as excinfo:
        admin.write.write_line_protocol(
            f"{measurement},host=a time=1i,v=2i {s}", database=db, precision="s"
        )
    assert excinfo.value.dropped == 1
    assert excinfo.value.others_written is False


def test_partial_write_msgpack_records(admin: ArcClient, db: str) -> None:
    """MessagePack row format follows line protocol's drop rules (Arc msgpack.go)."""
    measurement = uid("pw_rec")
    base = (int(time.time()) - 60) * 1_000_000
    with pytest.raises(ArcPartialWriteError) as excinfo:
        admin.write.write_records(
            [
                {"measurement": measurement, "timestamp": base, "fields": {"v": 1}},
                {"measurement": measurement, "timestamp": base + 1, "fields": {"v": "oops"}},
                {"measurement": measurement, "timestamp": base + 2, "fields": {"v": 3}},
            ],
            database=db,
        )
    assert excinfo.value.dropped == 1 and excinfo.value.others_written is True
    rows = wait_rows(admin, f"SELECT v FROM {measurement} ORDER BY time", db, 2)
    assert rows == [[1], [3]]


def test_partial_write_buffered_reported_once(admin: ArcClient, db: str) -> None:
    """A buffered batch Arc drops (partial write) is discarded and reported once.

    The buffered writer sends columnar MessagePack, where Arc drops a whole
    record whose names it cannot read back (a field named "time" in another
    case, internal/api/msgpack.go FilterRecordNames). The other measurement
    in the same flush is still written, exactly once.
    """
    dropped = uid("pw_buf")
    good = uid("pw_buf_ok")
    base = (int(time.time()) - 60) * 1_000_000
    with make_client(database=db, max_retries=3, retry_backoff=2.0) as client:
        counter = RequestCounter(client)
        buffer = client.write.buffered(batch_size=1000, flush_interval=3600)
        buffer.write({"measurement": dropped, "timestamp": base, "fields": {"v": 1.0, "TIME": 5.0}})
        buffer.write(
            {"measurement": dropped, "timestamp": base + 1, "fields": {"v": 2.0, "TIME": 6.0}}
        )
        buffer.write({"measurement": good, "timestamp": base, "fields": {"v": 1.0}})
        buffer.write({"measurement": good, "timestamp": base + 1, "fields": {"v": 2.0}})
        with timed() as t, pytest.raises(ArcPartialWriteError) as excinfo:
            buffer.flush()
        assert t() < 1.5
        assert excinfo.value.dropped == 2
        assert excinfo.value.others_written is False
        assert buffer.pending_count == 0, "partial-write batch should be discarded"
        buffer.flush()  # reported once: a second flush does not raise again
        buffer.write({"measurement": good, "timestamp": base + 2, "fields": {"v": 3.0}})
        buffer.close()
        writes = counter.count("/api/v1/write")
    assert writes == 3, f"expected 2 flush writes + 1 close write, got {writes}"
    assert wait_rows(admin, f"SELECT v FROM {good} ORDER BY time", db, 3) == [[1.0], [2.0], [3.0]]
    time.sleep(1)
    assert wait_rows(admin, f"SELECT v FROM {good} ORDER BY time", db, 3) == [[1.0], [2.0], [3.0]]
    try:
        stored = admin.query.query(f"SELECT * FROM {dropped}", db).row_count
    except ArcQueryError:
        stored = 0
    assert stored == 0, "a dropped batch was stored"


def test_columnar_type_conflict_status(admin: ArcClient, db: str) -> None:
    """A mixed-type column in one columnar batch is a client error.

    Line protocol and MessagePack rows answer it with a 400 partial write;
    columnar MessagePack rejects the whole batch. The status should be a 4xx:
    a 5xx tells the caller (and the buffered writer) that the server failed.
    """
    measurement = uid("col_conflict")
    base = (int(time.time()) - 60) * 1_000_000
    with pytest.raises(ArcError) as excinfo:
        admin.write.write_columnar(
            measurement, {"time": [base, base + 1], "v": [1, "oops"]}, database=db
        )
    status = getattr(excinfo.value, "status_code", None) or getattr(
        excinfo.value.__cause__, "status_code", None
    )
    assert status is not None and 400 <= status < 500, f"status {status}: {excinfo.value}"


async def test_async_write_and_partial(aadmin: AsyncArcClient, admin: ArcClient, db: str) -> None:
    measurement = uid("apw")
    s = int(time.time()) - 60
    with pytest.raises(ArcPartialWriteError) as excinfo:
        await aadmin.write.write_line_protocol(
            [f"{measurement} v=1i {s}", f'{measurement} v="x" {s + 1}'], database=db, precision="s"
        )
    assert excinfo.value.dropped == 1 and excinfo.value.others_written
    await aadmin.write.write_columnar(
        measurement + "_c",
        {"time": [s * 1_000_000], "host": ["z"], "v": [9.0]},
        database=db,
        tag_columns=["host"],
    )
    assert len(wait_rows(admin, f"SELECT * FROM {measurement}_c", db, 1)) == 1


# --------------------------------------------------------------------------
# Databases and delete
# --------------------------------------------------------------------------


def test_measurement_schema_get_and_rebuild(admin: ArcClient, seeded: tuple[str, str, int]) -> None:
    db, measurement, _ = seeded
    schema = admin.databases.get_measurement_schema(db, measurement)
    assert schema.database == db and schema.measurement == measurement
    names = {f.name: f.type for f in schema.fields}
    assert {"time", "host", "usage"} <= set(names), names
    rebuilt = admin.databases.rebuild_measurement_schema(db, measurement)
    assert rebuilt.status == "queued"
    assert rebuilt.database == db and rebuilt.measurement == measurement
    with pytest.raises(ArcNotFoundError):
        admin.databases.get_measurement_schema(db, uid("missing"))


def test_delete_allow_unreachable_on_standalone(
    admin: ArcClient, seeded: tuple[str, str, int]
) -> None:
    db, measurement, _ = seeded
    preview = admin.delete.delete(
        db, measurement, "host = 'a'", dry_run=True, allow_unreachable=True
    )
    assert preview.dry_run is True
    assert preview.deleted_count == 2
    done = admin.delete.delete(
        db, measurement, "host = 'a'", dry_run=False, confirm=True, allow_unreachable=True
    )
    assert done.deleted_count == 2
    assert not done.unreachable_files and not done.unflushed_nodes
    rows = admin.query.query(f"SELECT host FROM {measurement}", db).data
    assert rows == [["b"]]


def test_databases_delete_allow_unreachable(admin: ArcClient) -> None:
    name = uid("sdk_dbdel")
    admin.databases.create(name)
    admin.write.write_columnar("m", {"time": [int(time.time() * 1e6)], "v": [1.0]}, database=name)
    server_flush()
    with pytest.raises(ArcValidationError):
        admin.databases.delete(name)
    admin.databases.delete(name, confirm=True, allow_unreachable=True)
    with pytest.raises(ArcNotFoundError):
        admin.databases.get(name)


async def test_async_databases(aadmin: AsyncArcClient, seeded: tuple[str, str, int]) -> None:
    db, measurement, _ = seeded
    schema = await aadmin.databases.get_measurement_schema(db, measurement)
    assert any(f.name == "usage" for f in schema.fields)
    assert (await aadmin.databases.get(db)).name == db


# --------------------------------------------------------------------------
# Auth
# --------------------------------------------------------------------------


def test_create_token_with_empty_permissions(admin: ArcClient, tokens: TokenFactory) -> None:
    token_id, token = tokens.create([])
    info = admin.auth.get_token(token_id)
    assert info.permissions == [], f"empty permissions not kept: {info.permissions}"
    with make_client(token=token) as client:
        verified = client.auth.verify()
        assert verified.valid
        assert not verified.permissions, verified
        with pytest.raises(ArcAuthenticationError):
            client.query.query("SELECT 1")


def test_update_token_to_empty_permissions(admin: ArcClient, tokens: TokenFactory) -> None:
    token_id, token = tokens.create(["read"])
    with make_client(token=token) as client:
        assert client.query.query("SELECT 1").row_count == 1
    admin.auth.update_token(token_id, permissions=[])
    assert admin.auth.get_token(token_id).permissions == []
    admin.auth.invalidate_cache()
    with make_client(token=token) as client, pytest.raises(ArcAuthenticationError):
        client.query.query("SELECT 1")


def test_auth_cache_stats_and_invalidate(admin: ArcClient) -> None:
    stats = admin.auth.cache_stats()
    assert "cache_size" in stats and "hit_rate_percent" in stats, stats
    invalidated = admin.auth.invalidate_cache()
    assert invalidated.get("success") is not False, invalidated
    assert admin.auth.cache_stats()["cache_size"] <= stats["max_cache_size"]


def test_auth_cache_requires_admin(tokens: TokenFactory) -> None:
    _, token = tokens.create(["read"])
    with make_client(token=token) as client, pytest.raises(ArcPermissionError):
        client.auth.cache_stats()


async def test_async_auth(aadmin: AsyncArcClient, tokens: TokenFactory) -> None:
    token_id, _ = tokens.create([])
    info = await aadmin.auth.get_token(token_id)
    assert info.permissions == []
    await aadmin.auth.update_token(token_id, permissions=["read"])
    assert (await aadmin.auth.get_token(token_id)).permissions == ["read"]
    assert "cache_size" in await aadmin.auth.cache_stats()


# --------------------------------------------------------------------------
# Compaction
# --------------------------------------------------------------------------


@licensed
def test_compaction_reads(admin: ArcClient) -> None:
    status = admin.compaction.status()
    assert "manager" in status
    cycles = admin.compaction.cycles(limit=5)
    assert "cycles" in cycles
    too_wide = admin.compaction.too_wide()
    assert "partitions" in too_wide
    cleared = admin.compaction.clear_too_wide()
    assert isinstance(cleared, dict)
    assert isinstance(admin.compaction.history(limit=5), (dict, list))
    with pytest.raises(ArcNotFoundError):
        admin.compaction.cycle(987654321)


@licensed
def test_compaction_trigger_measurement(admin: ArcClient, db: str) -> None:
    """Write several old files into one hour partition, then compact that measurement."""
    measurement = uid("cmp")
    hour = (int(time.time()) // 3600 - 30) * 3600
    for i in range(4):
        admin.write.write_columnar(
            measurement,
            {"time": [(hour + i * 60) * 1_000_000], "host": [f"h{i}"], "v": [float(i)]},
            database=db,
            tag_columns=["host"],
        )
        server_flush()
    assert len(wait_rows(admin, f"SELECT * FROM {measurement}", db, 4)) == 4

    with pytest.raises(ArcValidationError):
        admin.compaction.trigger(measurement=measurement)
    before = {
        c.get("cycle_id") or c.get("id") for c in admin.compaction.cycles().get("cycles") or []
    }
    triggered = admin.compaction.trigger(tier="hourly", database=db, measurement=measurement)
    assert triggered
    cycle = None
    deadline = time.monotonic() + 60
    while cycle is None and time.monotonic() < deadline:
        for c in admin.compaction.cycles().get("cycles") or []:
            cid = c.get("cycle_id") or c.get("id")
            if cid not in before and c.get("state", c.get("status")) not in ("running", "pending"):
                cycle = c
        time.sleep(1)
    assert cycle, f"no finished compaction cycle after trigger: {admin.compaction.cycles()}"
    cycle_id = cycle.get("cycle_id") or cycle.get("id")
    detail = admin.compaction.cycle(cycle_id)
    assert str(cycle_id) in str(detail)
    history = admin.compaction.history(cycle_id=cycle_id)
    assert isinstance(history, (dict, list))
    rows = admin.query.query(f"SELECT v FROM {measurement} ORDER BY time", db).data
    assert rows == [[0.0], [1.0], [2.0], [3.0]]


async def test_async_compaction(aadmin: AsyncArcClient) -> None:
    if not LICENSED:
        pytest.skip("Set ARC_TEST_LICENSED=1")
    assert "cycles" in await aadmin.compaction.cycles(limit=1)
    assert "partitions" in await aadmin.compaction.too_wide()


# --------------------------------------------------------------------------
# Backup
# --------------------------------------------------------------------------


def _wait_backup_done(admin: ArcClient, timeout: float = 60) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    status: dict[str, Any] = {}
    while time.monotonic() < deadline:
        status = admin.backup.status()
        if status.get("status") != "running":
            return status
        time.sleep(0.5)
    raise AssertionError(f"backup never went idle: {status}")


@licensed
def test_backup_scoped_restore_merge_and_replace(
    admin: ArcClient, seeded: tuple[str, str, int]
) -> None:
    db, measurement, _ = seeded
    _wait_backup_done(admin)
    started = admin.backup.create(databases=[db])
    assert started.get("status") == "running"
    assert started.get("databases") == [db]
    _wait_backup_done(admin)

    listing = admin.backup.list(include_foreign=True)
    mine = [b for b in listing.get("backups") or [] if db in str(b)]
    assert mine, f"scoped backup not listed: {listing}"
    backup_id = mine[0].get("backup_id") or mine[0].get("id")
    try:
        manifest = admin.backup.get(backup_id)
        assert db in str(manifest)

        # Delete the data, then merge-restore it back.
        admin.delete.delete(db, measurement, "1=1", dry_run=False, confirm=True)
        assert admin.query.query(f"SELECT * FROM {measurement}", db).row_count == 0
        merged = admin.backup.restore(backup_id, confirm=True, mode="merge")
        assert merged.get("mode") == "merge", merged
        _wait_backup_done(admin)
        rows = wait_rows(admin, f"SELECT usage FROM {measurement} ORDER BY time", db, 3)
        assert rows == [[1.0], [2.0], [3.0]]

        # replace is cluster-only: a standalone server answers 400.
        with pytest.raises(ArcServerError) as excinfo:
            admin.backup.restore(backup_id, confirm=True, mode="replace")
        assert excinfo.value.status_code == 400
        with pytest.raises(ArcValidationError):
            admin.backup.restore(backup_id, confirm=True, mode="overwrite")
    finally:
        _wait_backup_done(admin)
        admin.backup.delete(backup_id)
    with pytest.raises(ArcNotFoundError):
        admin.backup.get(backup_id)


# --------------------------------------------------------------------------
# Continuous queries and retention
# --------------------------------------------------------------------------


def test_continuous_query_lifecycle(admin: ArcClient, seeded: tuple[str, str, int]) -> None:
    db, measurement, base = seeded
    dest = measurement + "_1m"
    cq = admin.continuous_queries.create(
        name=uid("cq"),
        database=db,
        source_measurement=measurement,
        destination_measurement=dest,
        query=(
            f"SELECT time_bucket(INTERVAL '1 minute', time) AS time, host, avg(usage) AS usage "
            f"FROM {db}.{measurement} WHERE time >= {{start_time}} AND time < {{end_time}} "
            f"GROUP BY 1, host"
        ),
        interval="1m",
    )
    try:
        start = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(base / 1e6 - 3600))
        end = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time()))
        executed = admin.continuous_queries.execute(cq.id, start_time=start, end_time=end)
        assert executed.error is None, executed
        # watermark_status is documented as cluster-only (None standalone).
        assert executed.watermark_status is None, executed
        updated = admin.continuous_queries.update(cq.id, interval="5m")
        assert updated.interval == "5m"
        replaced = admin.continuous_queries.replace(
            cq.id,
            name=cq.name,
            database=db,
            source_measurement=measurement,
            destination_measurement=dest,
            query=cq.query,
            interval="10m",
            description="replaced",
            tag_columns=None,
            retention_days=None,
            delete_source_after_days=None,
            is_active=True,
        )
        assert replaced.interval == "10m" and replaced.description == "replaced"
        assert admin.continuous_queries.get_executions(cq.id)
    finally:
        admin.continuous_queries.delete(cq.id)


def test_retention_lifecycle(admin: ArcClient, db: str) -> None:
    policy = admin.retention.create(name=uid("ret"), database=db, retention_days=30)
    try:
        assert admin.retention.get(policy.id).retention_days == 30
        updated = admin.retention.update(policy.id, retention_days=60)
        assert updated.retention_days == 60
        assert any(p.id == policy.id for p in admin.retention.list())
        dry = admin.retention.execute(policy.id, dry_run=True)
        assert dry is not None
    finally:
        admin.retention.delete(policy.id)


async def test_async_cq_and_retention(aadmin: AsyncArcClient, db: str) -> None:
    assert isinstance(await aadmin.continuous_queries.list(database=db), list)
    assert isinstance(await aadmin.retention.list(), list)


# --------------------------------------------------------------------------
# Reconciliation
# --------------------------------------------------------------------------


def test_reconciliation_on_standalone(admin: ArcClient) -> None:
    status = admin.reconciliation.status()
    assert status["enabled"] is False and status.get("reason")
    with pytest.raises(ArcServerError) as excinfo:
        admin.reconciliation.trigger()
    assert excinfo.value.status_code == 503
    with pytest.raises(ArcValidationError):
        admin.reconciliation.trigger(dry_run=False)


def test_reconciliation_get_run_not_retried_on_standalone(admin: ArcClient) -> None:
    """GET .../runs/:id answers 503 "not running" on standalone; the SDK should not stall on it."""
    with make_client(max_retries=3, retry_backoff=2.0) as client:
        counter = RequestCounter(client)
        with timed() as t, pytest.raises(ArcServerError) as excinfo:
            client.reconciliation.get_run("does-not-exist")
    assert excinfo.value.status_code == 503
    assert counter.count("/reconciliation/runs/") == 1, (
        f"503 'not running' retried {counter.count('/reconciliation/runs/')} times in {t():.1f}s"
    )


# --------------------------------------------------------------------------
# Diagnostics
# --------------------------------------------------------------------------


def test_diagnostics_reads(admin: ArcClient) -> None:
    assert isinstance(admin.diagnostics.metrics(), dict)
    assert isinstance(admin.diagnostics.memory_metrics(), dict)
    assert isinstance(admin.diagnostics.query_pool_metrics(), dict)
    assert isinstance(admin.diagnostics.endpoint_metrics(), dict)
    assert isinstance(admin.diagnostics.timeseries("system", duration_minutes=5), dict)
    prom = admin.diagnostics.prometheus()
    assert isinstance(prom, str) and "# TYPE" in prom
    logs = admin.diagnostics.logs(limit=5, level="info", since_minutes=60)
    assert isinstance(logs, dict)
    assert "heap_alloc" in str(admin.diagnostics.memstats()).lower() or admin.diagnostics.memstats()
    assert isinstance(admin.diagnostics.duckdb_memory(), dict)
    with pytest.raises(ArcValidationError):
        admin.diagnostics.timeseries("bogus")


def test_free_os_memory_and_throttle(admin: ArcClient) -> None:
    with pytest.raises(ArcValidationError):
        admin.diagnostics.free_os_memory()
    try:
        first = admin.diagnostics.free_os_memory(confirm=True)
    except ArcRateLimitError as e:
        wait = (e.response_body or {}).get("retry_after_seconds", 30)
        time.sleep(float(wait) + 1)
        first = admin.diagnostics.free_os_memory(confirm=True)
    assert "heap_released_after" in first
    with make_client(max_retries=3, retry_backoff=2.0) as client:
        counter = RequestCounter(client)
        with timed() as t, pytest.raises(ArcRateLimitError) as excinfo:
            client.diagnostics.free_os_memory(confirm=True)
    assert counter.count("free-os-memory") == 1
    assert t() < 1.5
    assert (excinfo.value.response_body or {}).get("retry_after_seconds")


def test_diagnostics_admin_routes_need_admin(tokens: TokenFactory) -> None:
    _, token = tokens.create(["read"])
    with make_client(token=token) as client:
        assert isinstance(client.diagnostics.metrics(), dict)
        with pytest.raises(ArcPermissionError):
            client.diagnostics.memstats()


async def test_async_diagnostics(aadmin: AsyncArcClient) -> None:
    assert isinstance(await aadmin.diagnostics.metrics(), dict)
    assert "# TYPE" in await aadmin.diagnostics.prometheus()
    assert isinstance(await aadmin.diagnostics.memstats(), dict)


# --------------------------------------------------------------------------
# Cluster methods on a standalone node
# --------------------------------------------------------------------------

CLUSTER_READS = ["status", "nodes", "local", "health", "files", "policies"]


@pytest.mark.parametrize("method", CLUSTER_READS)
def test_cluster_reads_on_standalone_report_disabled(admin: ArcClient, method: str) -> None:
    """Pins down pass-through behavior: these answer 200 ``{"enabled": false, "success": false}``.

    The SDK returns that dict as-is; nothing raises, so ``nodes()`` looks like
    a successful call. Callers must check ``enabled``.
    """
    result = getattr(admin.cluster, method)()
    assert result.get("enabled") is False and result.get("mode") == "standalone", result


def test_cluster_node_on_standalone(admin: ArcClient) -> None:
    result = admin.cluster.node("no-such-node")
    assert result.get("enabled") is False, result


def test_cluster_stream_claims_on_standalone(admin: ArcClient) -> None:
    with pytest.raises(ArcServerError) as excinfo:
        admin.cluster.stream_claims()
    assert excinfo.value.status_code == 409


CLUSTER_MUTATIONS: list[tuple[str, Callable[[Any], Any]]] = [
    ("remove_node", lambda c: c.remove_node("no-such-node")),
    ("demote_writer", lambda c: c.demote_writer("no-such-node", confirm=True)),
    ("assign_compactor", lambda c: c.assign_compactor("no-such-node")),
    ("converge_voters", lambda c: c.converge_voters(confirm=True)),
    ("release_stream_claim", lambda c: c.release_stream_claim("00000000000000ff", epoch=1)),
    ("abort_compaction_job", lambda c: c.abort_compaction_job("no-such-job", confirm=True)),
    ("adopt_policies", lambda c: c.adopt_policies("cq", "no-such-node", "0" * 64, confirm=True)),
    ("delete_file", lambda c: c.delete_file("db/m/2026/01/01/00/x.parquet", confirm=True)),
]


@pytest.mark.parametrize("name,call", CLUSTER_MUTATIONS, ids=[n for n, _ in CLUSTER_MUTATIONS])
def test_cluster_mutations_on_standalone_fail_clearly(
    admin: ArcClient, name: str, call: Callable[[Any], Any]
) -> None:
    """A cluster mutation on a standalone node must not look like it succeeded."""
    try:
        result = call(admin.cluster)
    except ArcError as e:
        print(f"cluster.{name}: {type(e).__name__} status={getattr(e, 'status_code', None)} {e}")
        return
    print(f"cluster.{name}: returned {result}")
    assert result.get("success") is not True and result.get("enabled") is False, result


# --------------------------------------------------------------------------
# Enterprise: RBAC, audit, query management
# --------------------------------------------------------------------------


@licensed
def test_rbac_full_lifecycle(
    admin: ArcClient, tokens: TokenFactory, seeded: tuple[str, str, int]
) -> None:
    db, measurement, _ = seeded
    org = admin.rbac.create_organization(uid("org"), description="sdk live")
    org_id = (org.get("organization") or org)["id"]
    try:
        assert admin.rbac.get_organization(org_id)
        admin.rbac.update_organization(org_id, description="updated")
        assert any(o["id"] == org_id for o in admin.rbac.list_organizations()["organizations"])

        team = admin.rbac.create_team(org_id, uid("team"))
        team_id = (team.get("team") or team)["id"]
        assert admin.rbac.get_team(team_id)  # /api/v1/rbac/teams/:id
        admin.rbac.update_team(team_id, description="t")
        assert any(t["id"] == team_id for t in admin.rbac.list_teams(org_id)["teams"])

        role = admin.rbac.create_role(team_id, db, ["read"])
        role_id = (role.get("role") or role)["id"]
        assert admin.rbac.get_role(role_id)
        admin.rbac.update_role(role_id, permissions=["read", "write"])
        assert any(r["id"] == role_id for r in admin.rbac.list_roles(team_id)["roles"])

        perm = admin.rbac.create_measurement_permission(role_id, measurement, ["read"])
        perm_id = (perm.get("permission") or perm.get("measurement_permission") or perm)["id"]
        assert admin.rbac.list_measurement_permissions(role_id)

        token_id, token = tokens.create([])
        admin.rbac.add_token_to_team(token_id, team_id)
        assert str(team_id) in str(admin.rbac.list_token_teams(token_id))
        effective = admin.rbac.get_token_permissions(token_id)
        assert db in str(effective), effective
        admin.auth.invalidate_cache()
        with make_client(token=token) as client:
            # The databases routes consult RBAC (internal/api/databases.go
            # withResourceReadAuth), so the team grant is honored there.
            assert client.databases.get(db).name == db
            assert any(
                m.name == measurement for m in client.databases.list_measurements(db).measurements
            )
            # The SQL routes check only the coarse "read" bit
            # (internal/api/query.go withReadAuth): an RBAC-only token cannot query.
            with pytest.raises(ArcPermissionError):
                client.query.query(f"SELECT count(*) FROM {measurement}", db)
        admin.rbac.remove_token_from_team(token_id, team_id)

        admin.rbac.delete_measurement_permission(perm_id)
        admin.rbac.delete_role(role_id)
        admin.rbac.delete_team(team_id)
        with pytest.raises(ArcNotFoundError):
            admin.rbac.get_team(team_id)
    finally:
        with contextlib.suppress(ArcError):
            admin.rbac.delete_organization(org_id)


@licensed
def test_audit_logs_and_stats(admin: ArcClient) -> None:
    marker = uid("audit")
    admin.query.query(f"SELECT '{marker}' AS m")
    stats = admin.audit.stats()
    assert isinstance(stats, dict)
    logs = admin.audit.logs(limit=50)
    assert isinstance(logs, (dict, list))
    assert logs


@licensed
def test_queries_active_history(admin: ArcClient) -> None:
    result = admin.query.query("SELECT 7 AS seven")
    history = admin.queries.history(limit=50)
    assert result.query_id in str(history)
    assert isinstance(admin.queries.active(), dict)
    with pytest.raises(ArcNotFoundError):
        admin.queries.get("no-such-query-id")


async def test_async_enterprise_sweep(aadmin: AsyncArcClient) -> None:
    if not LICENSED:
        pytest.skip("Set ARC_TEST_LICENSED=1")
    assert "organizations" in await aadmin.rbac.list_organizations()
    assert "policies" in await aadmin.governance.list_policies()
    assert isinstance(await aadmin.audit.stats(), dict)
    assert isinstance(await aadmin.queries.history(limit=1), dict)
    assert (await aadmin.reconciliation.status())["enabled"] is False
    assert (await aadmin.cluster.status())["enabled"] is False
    assert "status" in await aadmin.backup.status()


# --------------------------------------------------------------------------
# Edge sync
# --------------------------------------------------------------------------


@pytest.fixture
def hub() -> Iterator[ArcClient]:
    with make_client(HUB_URL, HUB_TOKEN, timeout=300) as client:
        yield client


@pytest.fixture
def spoke() -> Iterator[ArcClient]:
    with make_client(SPOKE_URL, SPOKE_TOKEN, timeout=300) as client:
        yield client


@needs_hub
def test_edge_register_spoke_without_name(hub: ArcClient) -> None:
    """``register_spoke(spoke_id)`` uses the signature's default ``name=""``.

    Arc requires a non-blank name (internal/edgesync/registry.go Register), so
    the default must not produce a request Arc always rejects.
    """
    spoke_id = uid("spk").replace("_", "-")
    try:
        secret = hub.edge_sync.register_spoke(spoke_id)
        assert secret.spoke_id == spoke_id and secret.secret
    finally:
        with contextlib.suppress(ArcError):
            hub.edge_sync.delete_spoke(spoke_id, confirm=True)


@needs_hub
def test_edge_hub_spoke_registration_lifecycle(hub: ArcClient) -> None:
    spoke_id = uid("spk").replace("_", "-")
    secret = hub.edge_sync.register_spoke(spoke_id, name="throwaway")
    try:
        assert secret.spoke_id == spoke_id and len(secret.secret) >= 32
        with pytest.raises(ArcServerError) as dup:
            hub.edge_sync.register_spoke(spoke_id, name="again")
        assert dup.value.status_code == 409

        got = hub.edge_sync.get_spoke(spoke_id)
        assert got.spoke_id == spoke_id and got.name == "throwaway" and got.enabled
        assert spoke_id in {s.spoke_id for s in hub.edge_sync.list_spokes()}

        hub.edge_sync.disable_spoke(spoke_id)
        assert hub.edge_sync.get_spoke(spoke_id).enabled is False
        hub.edge_sync.enable_spoke(spoke_id)
        assert hub.edge_sync.get_spoke(spoke_id).enabled is True

        with pytest.raises(ArcValidationError):
            hub.edge_sync.rotate_spoke_secret(spoke_id)
        rotated = hub.edge_sync.rotate_spoke_secret(spoke_id, confirm=True)
        assert rotated.secret and rotated.secret != secret.secret

        assert isinstance(hub.edge_sync.import_history(spoke_id, limit=5), dict)
    finally:
        with pytest.raises(ArcValidationError):
            hub.edge_sync.delete_spoke(spoke_id)
        hub.edge_sync.delete_spoke(spoke_id, confirm=True)
    with pytest.raises(ArcNotFoundError):
        hub.edge_sync.get_spoke(spoke_id)


def _spoke_write(spoke: ArcClient, measurement: str, n: int) -> None:
    base = (int(time.time()) - 120) * 1_000_000
    spoke.write.write_columnar(
        measurement,
        {"time": [base + i for i in range(n)], "v": [float(i) for i in range(n)]},
        database="edge",
    )
    server_flush(SPOKE_URL, SPOKE_TOKEN)


@needs_spoke
def test_edge_spoke_network_sync(hub: ArcClient, spoke: ArcClient) -> None:
    # The spoke discovers new files only inside a sync or export pass
    # (internal/edgesync/discover.go), so the ledger is checked afterwards.
    before = hub.edge_sync.get_spoke(SPOKE_ID).files_received
    synced_before = spoke.edge_sync.spoke_status().get("synced", 0)
    _spoke_write(spoke, uid("edge_net"), 3)
    result = spoke.edge_sync.run_sync()
    assert isinstance(result, dict)
    status = spoke.edge_sync.spoke_status()
    assert status.get("synced", 0) > synced_before, (result, status)
    assert status.get("pending", 0) == 0, status
    assert status.get("last_synced_at"), status
    assert hub.edge_sync.get_spoke(SPOKE_ID).files_received > before
    assert spoke.edge_sync.spoke_ledger(state="synced", limit=5)
    with pytest.raises(ArcValidationError):
        spoke.edge_sync.dismiss_failed(all_failed=True)
    assert "requeued" in spoke.edge_sync.requeue_failed(all_failed=True)


@needs_bundle
def test_edge_bundle_export_import_ack(hub: ArcClient, spoke: ArcClient) -> None:
    assert BUNDLE_DIR
    spoke.edge_sync.run_sync()  # drain anything earlier tests left pending
    exported_before = spoke.edge_sync.spoke_status().get("exported", 0)
    _spoke_write(spoke, uid("edge_bundle"), 2)
    out = os.path.join(BUNDLE_DIR, uid("b"))
    os.makedirs(out, exist_ok=True)
    exported = spoke.edge_sync.export_bundle(out, limit=100)
    assert exported.get("exported") is True, exported
    bundle_path = exported["dir"]
    assert bundle_path.startswith(out) and exported["files"] > 0, exported
    assert spoke.edge_sync.spoke_status().get("exported", 0) == exported_before + exported["files"]

    imported = hub.edge_sync.import_bundle(bundle_path)
    assert isinstance(imported, dict) and not imported.get("conflicts"), imported
    history = hub.edge_sync.import_history(SPOKE_ID, limit=5)
    assert exported.get("bundle_id", "") in str(history), history
    with pytest.raises(ArcServerError) as again:
        hub.edge_sync.import_bundle(bundle_path)
    assert again.value.status_code == 409

    acked = spoke.edge_sync.apply_ack(bundle_path)
    assert acked.get("applied") is True, acked
    assert spoke.edge_sync.spoke_status().get("exported", 0) == exported_before


@needs_bundle
def test_edge_bundle_revert_and_empty_export(spoke: ArcClient) -> None:
    assert BUNDLE_DIR
    spoke.edge_sync.run_sync()
    _spoke_write(spoke, uid("edge_revert"), 1)
    out = os.path.join(BUNDLE_DIR, uid("r"))
    os.makedirs(out, exist_ok=True)
    exported = spoke.edge_sync.export_bundle(out)
    assert exported.get("exported") is True, exported
    reverted = spoke.edge_sync.revert_export(exported["bundle_id"])
    assert reverted.get("reverted", 0) > 0, reverted
    # Drain via the network so the next export is empty.
    spoke.edge_sync.run_sync()
    empty_dir = os.path.join(BUNDLE_DIR, uid("e"))
    os.makedirs(empty_dir, exist_ok=True)
    empty = spoke.edge_sync.export_bundle(empty_dir)
    assert empty.get("exported") is False, empty


@needs_spoke
async def test_async_edge_sync(hub: ArcClient) -> None:
    async with make_async_client(HUB_URL, HUB_TOKEN) as ahub:
        assert SPOKE_ID in {s.spoke_id for s in await ahub.edge_sync.list_spokes()}
    async with make_async_client(SPOKE_URL, SPOKE_TOKEN) as aspoke:
        assert "pending" in await aspoke.edge_sync.spoke_status()


def test_edge_routes_absent_on_plain_server(admin: ArcClient) -> None:
    """Without edge_sync enabled the routes are not mounted: 404, not a crash."""
    with pytest.raises(ArcNotFoundError):
        admin.edge_sync.list_spokes()
    with pytest.raises(ArcNotFoundError):
        admin.edge_sync.spoke_status()


# --------------------------------------------------------------------------
# Telemetry header
# --------------------------------------------------------------------------


def test_installation_id_header_sent(admin: ArcClient) -> None:
    counter = RequestCounter(admin)
    admin.health()
    admin.query.query("SELECT 1")
    ids = {r.headers.get(INSTALLATION_HEADER) for r in counter.requests}
    assert len(ids) == 1 and None not in ids, ids
    uuid.UUID(ids.pop())
    assert "python" in counter.requests[0].headers.get("user-agent", "").lower()


@pytest.mark.parametrize("env", [{"DO_NOT_TRACK": "1"}, {"ARC_CLIENT_SEND_INSTALLATION_ID": "0"}])
def test_installation_id_opt_out(monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> None:
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    with make_client() as client:
        counter = RequestCounter(client)
        assert client.health().status == "ok"
        assert client.query.query("SELECT 1").row_count == 1
    assert all(INSTALLATION_HEADER not in r.headers for r in counter.requests)


def test_installation_id_param_opt_out() -> None:
    with make_client(send_installation_id=False) as client:
        counter = RequestCounter(client)
        assert client.health().status == "ok"
    assert all(INSTALLATION_HEADER not in r.headers for r in counter.requests)
