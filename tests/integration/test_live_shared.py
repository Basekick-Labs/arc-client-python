"""Opt-in live tests against a shared-storage Arc cluster with a cold tier.

Covers what the local-storage cluster suite cannot reach: a dedicated
compactor holding the compaction lease, tiering to a real cold bucket,
MQTT subscriptions against a broker, and ``unflushed_nodes`` (a delete on
shared storage asks every other writer to flush its buffered rows first).

Skipped unless ``ARC_TEST_SHARED_URLS`` is set: the direct URL of every node
(not a load balancer), plus ``ARC_TEST_TOKEN``, an admin token valid on all
of them. Written against Arc main (2027.01.1) running the
``deploy/docker-compose/enterprise-shared`` stack (three writers, one reader,
SeaweedFS as the shared bucket) plus a ``compactor`` node, cluster failover
on, ``tiered_storage`` and its cold tier enabled (an AWS S3 bucket), MQTT
enabled, ``compaction.daily_min_files=2``, and every compaction and
migration schedule pushed out of the way (``0 0 1 1 *``)::

    N=http://localhost
    ARC_TEST_SHARED_URLS=$N:18101,$N:18102,$N:18103,$N:18104,$N:18105 \\
    ARC_TEST_TOKEN=... ARC_TEST_MQTT_BROKER=tcp://arc-mosquitto:1883 \\
        uv run pytest tests/integration/test_live_shared.py -v

The suite migrates data to the cold tier, moves the compactor lease (and
hands it back), and pauses a writer container. Use a throwaway cluster.

MQTT tests need ``ARC_TEST_MQTT_BROKER``, the broker URL as Arc reaches it,
and publish with ``mosquitto_pub`` inside the broker container
(``ARC_TEST_MQTT_CONTAINER``, default ``arc-mosquitto``). Tests that pause a
writer drive containers with ``docker`` and assume each container is named
after its Arc node id.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import subprocess
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TypeVar
from urllib.parse import urlparse

import pytest

from arc_client import ArcClient
from arc_client.exceptions import ArcNotFoundError, ArcServerError

URLS = [u.strip() for u in os.environ.get("ARC_TEST_SHARED_URLS", "").split(",") if u.strip()]
TOKEN = os.environ.get("ARC_TEST_TOKEN")
MQTT_BROKER = os.environ.get("ARC_TEST_MQTT_BROKER")
MQTT_CONTAINER = os.environ.get("ARC_TEST_MQTT_CONTAINER", "arc-mosquitto")
HAVE_DOCKER = shutil.which("docker") is not None

pytestmark = pytest.mark.skipif(
    not URLS,
    reason="Set ARC_TEST_SHARED_URLS (and ARC_TEST_TOKEN) to run shared-storage live tests.",
)
needs_docker = pytest.mark.skipif(not HAVE_DOCKER, reason="needs the docker CLI")
needs_mqtt = pytest.mark.skipif(
    not (MQTT_BROKER and HAVE_DOCKER), reason="set ARC_TEST_MQTT_BROKER (and have docker)"
)

T = TypeVar("T")

# Days back the tiering data is written: past compaction's 7-day file-age
# check (compaction.daily_skip_file_age_check_days) and any hot_max_age_days
# the test sets.
BACKDATE_DAYS = 40


def _client_for(url: str, **kwargs: Any) -> ArcClient:
    parsed = urlparse(url)
    return ArcClient(
        host=parsed.hostname or "localhost",
        port=parsed.port or (443 if parsed.scheme == "https" else 80),
        ssl=parsed.scheme == "https",
        token=TOKEN,
        **kwargs,
    )


def wait_for(
    fn: Callable[[], T],
    until: Callable[[T], bool],
    timeout: float = 30.0,
    interval: float = 0.5,
    what: str = "condition",
) -> T:
    """Poll ``fn`` until ``until(result)``; fail with the last result on timeout."""
    deadline = time.monotonic() + timeout
    last: Any = None
    last_exc: Optional[BaseException] = None
    while time.monotonic() < deadline:
        try:
            last = fn()
            last_exc = None
            if until(last):
                return last
        except Exception as e:  # node restarting, leadership moving
            last_exc = e
        time.sleep(interval)
    pytest.fail(f"timed out waiting for {what}; last={last!r} exc={last_exc!r}")


def docker(*args: str) -> str:
    out = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=120)
    if out.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} failed: {out.stderr.strip()}")
    return out.stdout


def flush(client: ArcClient) -> None:
    """Force the node's ingest buffers to parquet (no SDK wrapper for this route)."""
    client._get_http().post("/api/v1/write/line-protocol/flush")


def unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def count(client: ArcClient, database: str, measurement: str) -> int:
    rows = client.query.query(f"SELECT count(*) FROM {measurement}", database=database).data
    return int(rows[0][0]) if rows else 0


@dataclass
class Shared:
    """Node-id keyed clients for a shared-storage cluster."""

    clients: dict[str, ArcClient] = field(default_factory=dict)
    roles: dict[str, str] = field(default_factory=dict)
    url_of: dict[str, str] = field(default_factory=dict)

    def of_role(self, role: str) -> list[str]:
        return sorted(nid for nid, r in self.roles.items() if r == role)

    def leader(self) -> ArcClient:
        status = next(iter(self.clients.values())).cluster.status()
        return self.clients[status["raft"]["leader_id"]]

    def active_compactor(self) -> dict[str, Any]:
        return self.leader().cluster.status()["active_compactor"]

    def compactor(self) -> ArcClient:
        """The client of the node holding the compactor lease."""
        return self.clients[self.active_compactor()["node_id"]]


@pytest.fixture(scope="module")
def shared() -> Iterator[Shared]:
    cluster = Shared()
    for url in URLS:
        client = _client_for(url, timeout=120)
        local = client.cluster.local()
        cluster.clients[local["id"]] = client
        cluster.roles[local["id"]] = local["role"]
        cluster.url_of[local["id"]] = url
    if not cluster.of_role("compactor"):
        pytest.skip("the cluster has no compactor node")
    yield cluster
    for client in cluster.clients.values():
        client.close()


# --------------------------------------------------------------------------
# Compactor lease
# --------------------------------------------------------------------------


def test_compactor_lease_moves_and_returns(shared: Shared) -> None:
    """``assign_compactor`` moves the lease to a writer and back to the compactor."""
    dedicated = shared.of_role("compactor")[0]
    writer = shared.of_role("writer")[0]
    leader = shared.leader()

    def move(to: str) -> None:
        if shared.active_compactor().get("node_id") == to:
            return
        result = leader.cluster.assign_compactor(to)
        assert result["success"] is True
        assert result["node_id"] == to
        wait_for(
            shared.active_compactor,
            lambda ac: ac.get("node_id") == to,
            what=f"compactor lease on {to}",
        )

    # A fresh cluster hands the lease to the dedicated compactor on its own.
    move(dedicated)
    try:
        move(writer)
        status = shared.active_compactor()
        assert status["is_dedicated"] is False
        assert status["role"] == "writer"
    finally:
        move(dedicated)
    status = shared.active_compactor()
    assert status["is_dedicated"] is True
    assert status["role"] == "compactor"

    # Assigning the holder is refused, not a silent no-op.
    with pytest.raises(ArcServerError) as exc:
        leader.cluster.assign_compactor(dedicated)
    assert exc.value.status_code == 409
    assert "already holds the compactor lease" in str(exc.value)


# --------------------------------------------------------------------------
# Daily compaction on the compactor, then tiering to the cold bucket
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def daily_partition(shared: Shared) -> Iterator[tuple[str, str, int]]:
    """A database with one daily-compacted partition BACKDATE_DAYS old."""
    db, measurement = unique("sdksh_tier"), "cpu"
    writers = [shared.clients[w] for w in shared.of_role("writer")]
    base = int(time.time() - BACKDATE_DAYS * 86400) * 10**9
    # Two flushes give two files, compaction.daily_min_files on the rig.
    for i, writer in enumerate(writers[:2]):
        writer.write.write_line_protocol(
            [f"{measurement},host=h{i} usage={i}.5 {base + i * 10**9}"], database=db
        )
        flush(writer)
    rows = 2
    wait_for(lambda: count(writers[0], db, measurement), lambda n: n == rows, what="rows")
    # The compactor plans from its own copy of the cluster manifest: a file
    # not applied there yet leaves the partition under daily_min_files, and
    # the cycle still reports the batch as succeeded.
    compactor = shared.compactor()
    wait_for(
        lambda: compactor.cluster.files(database=db)["files"],
        lambda files: len(files) == 2,
        what="the compactor's manifest to list both files",
    )

    assert shared.roles[shared.active_compactor()["node_id"]] == "compactor"
    candidates = compactor.compaction.candidates()["candidates"]
    assert any(c["database"] == db and c["tier"] == "daily" for c in candidates), candidates
    triggered = compactor.compaction.trigger(tier="daily", database=db, measurement=measurement)
    cycle_id = triggered["cycle_id"]
    cycle = wait_for(
        lambda: compactor.compaction.cycle(cycle_id),
        lambda c: c.get("status") == "completed",
        timeout=60,
        what="daily compaction cycle",
    )
    assert cycle["succeeded_batches"] == 1
    jobs = [j for j in compactor.compaction.history()["recent_jobs"] if j["database"] == db]
    assert jobs and jobs[0]["files_compacted"] == 2, jobs
    yield db, measurement, rows

    leader = shared.leader()
    with contextlib.suppress(ArcNotFoundError):
        leader.tiering.delete_policy(db)
    went_cold = any(f["tier"] == "cold" for f in leader.tiering.files(database=db)["files"])
    dropped = leader.databases.delete(db, confirm=True)
    # The drop removes the cold copies too, not just the hot files.
    assert dropped["cold_files_deleted"] == (1 if went_cold else 0), dropped


def test_daily_compaction_ran_on_the_compactor(
    shared: Shared, daily_partition: tuple[str, str, int]
) -> None:
    db, _, rows = daily_partition
    history = shared.compactor().compaction.history()["recent_jobs"]
    jobs = [j for j in history if j["database"] == db]
    assert jobs and jobs[0]["tier"] == "daily" and jobs[0]["success"] is True
    assert jobs[0]["files_compacted"] == 2
    assert shared.compactor().compaction.jobs()["active_jobs"] == 0
    assert count(shared.leader(), db, "cpu") == rows


def test_tiering_policy_lifecycle(shared: Shared) -> None:
    leader = shared.leader()
    db = unique("sdksh_pol")
    default = leader.tiering.effective_policy(db)
    assert default["source"] != "custom"
    with pytest.raises(ArcNotFoundError):
        leader.tiering.get_policy(db)

    leader.tiering.set_policy(db, hot_max_age_days=3)
    try:
        assert leader.tiering.get_policy(db)["hot_max_age_days"] == 3
        effective = leader.tiering.effective_policy(db)
        assert effective == {**effective, "source": "custom", "hot_max_age_days": 3}
        listed = {p["database"]: p for p in leader.tiering.list_policies()["policies"]}
        assert listed[db]["hot_only"] is False
        leader.tiering.set_policy(db, hot_only=True)
        assert leader.tiering.effective_policy(db)["hot_only"] is True
    finally:
        leader.tiering.delete_policy(db)
    with pytest.raises(ArcNotFoundError):
        leader.tiering.get_policy(db)


def test_migrate_to_cold_and_read_back(
    shared: Shared, daily_partition: tuple[str, str, int]
) -> None:
    db, measurement, rows = daily_partition
    leader = shared.leader()
    leader.tiering.set_policy(db, hot_max_age_days=1)

    # The cycle completes before the compaction output is in place on object
    # storage (a staged commit finishes it); a scan registers it once it is.
    def registered_daily() -> list[dict[str, Any]]:
        leader.tiering.scan()
        files = leader.tiering.files(database=db)["files"]
        return [f for f in files if f["path"].endswith("_daily.parquet")]

    wait_for(registered_daily, bool, timeout=120, interval=2, what="daily file registered")

    plan = leader.tiering.migrate(database=db, dry_run=True)
    assert plan["status"] == "dry_run"
    assert plan["would_migrate"] == 1, (plan, leader.tiering.files(database=db))

    result = leader.tiering.migrate(database=db)
    assert result["errors"] == 0
    assert result["migrated"] == 1, result

    files = leader.tiering.files(database=db)["files"]
    daily = [f for f in files if f["path"].endswith("_daily.parquet")]
    assert len(daily) == 1 and daily[0]["tier"] == "cold"
    recent = leader.tiering.stats()["recent_migrations"]
    assert any(m["file_path"] == daily[0]["path"] and m["to_tier"] == "cold" for m in recent)
    assert leader.tiering.status()["tiers"]["cold"]["file_count"] >= 1

    # Queries read cold files in place, on writers and readers alike.
    for nid in shared.of_role("writer")[:1] + shared.of_role("reader"):
        assert count(shared.clients[nid], db, measurement) == rows


@pytest.mark.xfail(
    strict=True,
    reason="Arc bug Basekick-Labs/arc#1430: tiering files ignores tier when database is set",
)
def test_tiering_files_filters_tier_and_database(
    shared: Shared, daily_partition: tuple[str, str, int]
) -> None:
    db = daily_partition[0]
    leader = shared.leader()
    hot = leader.tiering.files(tier="hot", database=db)["files"]
    cold = leader.tiering.files(tier="cold", database=db)["files"]
    assert all(f["tier"] == "hot" for f in hot), hot
    assert all(f["tier"] == "cold" for f in cold), cold


# --------------------------------------------------------------------------
# MQTT
# --------------------------------------------------------------------------


@needs_mqtt
def test_mqtt_subscription_lifecycle(shared: Shared) -> None:
    """Create, start, ingest, pause/restart/stop, update, delete."""
    node = shared.clients[shared.of_role("writer")[0]]
    db = unique("sdksh_mqtt")
    topic = f"sdk/{db}"
    created = node.mqtt.create_subscription(
        {
            "name": db,
            "broker": MQTT_BROKER,
            "client_id": db,
            "topics": [f"{topic}/#"],
            "database": db,
            "qos": 1,
            # Starts the subscription when Arc boots, not on create.
            "auto_start": False,
        }
    )
    sid = created["subscription"]["id"]
    try:
        assert created["subscription"]["status"] == "stopped"
        assert node.mqtt.start_subscription(sid)["success"] is True
        wait_for(
            lambda: node.mqtt.get_subscription(sid)["subscription"]["status"],
            lambda s: s == "running",
            what="subscription running",
        )
        # Subscriptions live in the node's own database: other nodes do not list them.
        other = shared.clients[shared.of_role("writer")[1]]
        assert sid not in {s["id"] for s in other.mqtt.list_subscriptions()["subscriptions"] or []}

        for i in range(3):
            payload = json.dumps({"m": "env", "tags": {"room": "r1"}, "fields": {"temp": 20 + i}})
            docker(
                "exec",
                MQTT_CONTAINER,
                "mosquitto_pub",
                "-q",
                "1",
                "-t",
                f"{topic}/a",
                "-m",
                payload,
            )
        wait_for(
            lambda: node.mqtt.subscription_stats(sid)["stats"]["messages_received"],
            lambda n: n == 3,
            what="3 MQTT messages received",
        )

        def stored() -> int:
            flush(node)
            return count(node, db, "env")

        wait_for(stored, lambda n: n == 3, what="MQTT rows stored")
        assert node.mqtt.health()["subscriptions"]["running"] >= 1
        assert node.mqtt.stats()["stats"]["summary"]["total_subscriptions"] >= 1

        node.mqtt.pause_subscription(sid)
        assert node.mqtt.get_subscription(sid)["subscription"]["status"] == "paused"
        node.mqtt.restart_subscription(sid)

        # A running subscription cannot be edited.
        with pytest.raises(ArcServerError) as exc:
            node.mqtt.update_subscription(sid, {"qos": 0})
        assert exc.value.status_code == 409

        node.mqtt.stop_subscription(sid)
        updated = node.mqtt.update_subscription(sid, {"qos": 0})
        assert updated["subscription"]["qos"] == 0
        with pytest.raises(ArcServerError) as exc:
            node.mqtt.pause_subscription(sid)
        assert exc.value.status_code == 409
    finally:
        with contextlib.suppress(Exception):
            node.mqtt.stop_subscription(sid)
        node.mqtt.delete_subscription(sid)
        node.databases.delete(db, confirm=True)
    with pytest.raises(ArcNotFoundError):
        node.mqtt.get_subscription(sid)


# --------------------------------------------------------------------------
# unflushed_nodes: a writer that cannot flush before a delete
# --------------------------------------------------------------------------


@contextlib.contextmanager
def paused(shared: Shared, container: str) -> Iterator[None]:
    """Freeze a node's container, then bring it back into Raft.

    A node frozen for 15 s or more never rejoins Raft on unpause
    (Basekick-Labs/arc#1431) while still reported healthy, and every delete
    on the cluster is refused after that. Restart it instead, and wait until
    it follows the leader again.
    """
    docker("pause", container)
    try:
        yield
    finally:
        docker("unpause", container)
        docker("restart", container)
        leader = shared.leader().cluster.status()["raft"]["leader_id"]
        wait_for(
            lambda: shared.clients[container].cluster.status()["raft"]["leader_id"],
            lambda lid: lid == leader,
            timeout=90,
            what=f"{container} to rejoin Raft",
        )


@needs_docker
def test_delete_reports_unflushed_nodes(shared: Shared) -> None:
    writers = shared.of_role("writer")
    first, victim = writers[0], writers[-1]
    db = unique("sdksh_unf")
    # Default client settings: Arc waits for the paused peer before it
    # answers, and the SDK's delete timeout floor has to outlast that.
    with _client_for(shared.url_of[first]) as default_client:
        node = shared.clients[first]
        node.write.write_line_protocol(["m,host=a v=1", "m,host=b v=2"], database=db)
        flush(node)
        # Buffered on the victim, never flushed.
        shared.clients[victim].write.write_line_protocol(["m,host=a v=10"], database=db)
        try:
            with paused(shared, victim):
                with pytest.raises(ArcServerError) as exc:
                    node.delete.delete(db, "m", "host = 'a'", dry_run=False, confirm=True)
                assert exc.value.status_code == 503
                body = exc.value.response_body
                assert body["deleted_count"] == 0
                assert len(body["unflushed_nodes"]) == 1
                # Each entry is a message naming the node, not a bare id.
                assert victim in body["unflushed_nodes"][0]

                result = default_client.delete.delete(
                    db, "m", "host = 'a'", dry_run=False, confirm=True, allow_unreachable=True
                )
                assert result.success is True
                assert result.deleted_count == 1
                assert len(result.unflushed_nodes) == 1
                assert victim in result.unflushed_nodes[0]

                dropped = default_client.databases.delete(db, confirm=True, allow_unreachable=True)
                assert "deleted" in dropped["message"]
                assert len(dropped["unflushed_nodes"]) == 1
                assert victim in dropped["unflushed_nodes"][0]
        finally:
            # The victim's buffered row survives its restart (WAL) and lands
            # after the drop; remove the database again. Not suppressed: a
            # refusal here means the cluster did not recover.
            flush(shared.clients[victim])
            node.databases.delete(db, confirm=True)
