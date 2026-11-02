"""Opt-in live tests of the cluster-only SDK surface against a real Arc cluster.

Skipped unless ``ARC_TEST_CLUSTER_URLS`` is set: a comma-separated list of the
direct URL of every node (not a load balancer), plus ``ARC_TEST_TOKEN``, an
admin token valid on all of them. Written against a 4-node local-storage
Arc Enterprise cluster (three writers, one reader, peer replication, delete
and reconciliation enabled) built from Arc main (2027.01.1)::

    N=http://localhost
    ARC_TEST_CLUSTER_URLS=$N:8001,$N:8002,$N:8003,$N:8004 ARC_TEST_TOKEN=... \\
        uv run pytest tests/integration/test_live_cluster.py -v

The suite is DESTRUCTIVE: it deletes cluster files, demotes the primary
writer, converges the Raft voter set and restores backups. Use a throwaway
cluster. Tests run in file order and each restores what it changes (the
primary writer is handed back to the node that held it).

Tests that simulate node faults (a stopped writer, a manifest file missing
from the primary's disk) drive the containers with ``docker`` and assume
each container is named after its Arc node id; they are skipped when the
``docker`` CLI is not available. ``ARC_TEST_CLUSTER_DATA_DIR`` overrides the
in-container storage root (default ``/app/data/arc``).

``remove_node`` takes a node out of Raft for good, so it runs only with
``ARC_TEST_CLUSTER_REMOVE_NODE=1``; it removes the reader and then restarts
its container, which may leave the cluster needing a rebuild.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TypeVar
from urllib.parse import urlparse

import httpx
import pytest

from arc_client import ArcClient, AsyncArcClient
from arc_client.exceptions import (
    ArcConnectionError,
    ArcIngestRefusedError,
    ArcNotFoundError,
    ArcServerError,
    ArcValidationError,
)

URLS = [u.strip() for u in os.environ.get("ARC_TEST_CLUSTER_URLS", "").split(",") if u.strip()]
TOKEN = os.environ.get("ARC_TEST_TOKEN")
DATA_DIR = os.environ.get("ARC_TEST_CLUSTER_DATA_DIR", "/app/data/arc")
HAVE_DOCKER = shutil.which("docker") is not None

pytestmark = pytest.mark.skipif(
    not URLS,
    reason="Set ARC_TEST_CLUSTER_URLS (and ARC_TEST_TOKEN) to run live cluster tests.",
)
needs_docker = pytest.mark.skipif(not HAVE_DOCKER, reason="needs the docker CLI")

T = TypeVar("T")

# Arc's delete waits this long for manifest files missing on the node
# (internal/api/delete_records.go deleteLocalWait).
ARC_DELETE_LOCAL_WAIT = 30.0


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _client_for(url: str, **kwargs: Any) -> ArcClient:
    parsed = urlparse(url)
    return ArcClient(
        host=parsed.hostname or "localhost",
        port=parsed.port or (443 if parsed.scheme == "https" else 80),
        ssl=parsed.scheme == "https",
        token=TOKEN,
        **kwargs,
    )


def _async_client_for(url: str, **kwargs: Any) -> AsyncArcClient:
    parsed = urlparse(url)
    return AsyncArcClient(
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


def docker(*args: str, check: bool = True) -> str:
    out = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=120)
    if check and out.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} failed: {out.stderr.strip()}")
    return out.stdout


def exists_on(container: str, path: str) -> bool:
    """Whether ``path`` (a manifest path) is on the container's local storage."""
    rc = subprocess.run(
        ["docker", "exec", container, "test", "-e", f"{DATA_DIR}/{path}"],
        capture_output=True,
        timeout=60,
    ).returncode
    return rc == 0


def flush(client: ArcClient) -> None:
    """Force the node's ingest buffers to parquet (no SDK wrapper for this route)."""
    client._get_http().post("/api/v1/write/line-protocol/flush")


def unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@dataclass
class Cluster:
    """Node-id keyed clients and dynamic leader / primary lookup."""

    urls: list[str]
    clients: dict[str, ArcClient] = field(default_factory=dict)
    url_of: dict[str, str] = field(default_factory=dict)

    def any(self) -> ArcClient:
        for client in self.clients.values():
            with contextlib.suppress(Exception):
                client.cluster.health()
                return client
        raise RuntimeError("no cluster node answers")

    def status(self) -> dict[str, Any]:
        # Ask the leader-agnostic status of the first node that answers.
        return self.any().cluster.status()

    def leader_id(self) -> str:
        return self.status()["raft"]["leader_id"]

    def primary_id(self) -> str:
        return self.status()["primary_writer_id"]

    def leader(self) -> ArcClient:
        return self.clients[self.leader_id()]

    def follower(self) -> ArcClient:
        leader = self.leader_id()
        return next(c for nid, c in self.clients.items() if nid != leader)

    def role_of(self, node_id: str) -> str:
        return self.clients[node_id].cluster.local()["role"]

    def writers(self) -> list[str]:
        return [nid for nid in self.clients if self.role_of(nid) == "writer"]

    def readers(self) -> list[str]:
        return [nid for nid in self.clients if self.role_of(nid) == "reader"]

    def standby_writer(self, exclude: tuple[str, ...] = ()) -> str:
        primary = self.primary_id()
        leader = self.leader_id()
        candidates = [w for w in self.writers() if w != primary and w not in exclude]
        # Prefer a standby that is not the Raft leader either.
        candidates.sort(key=lambda w: w == leader)
        if not candidates:
            pytest.skip("cluster has no standby writer")
        return candidates[0]

    def wait_settled(self, timeout: float = 90.0) -> dict[str, Any]:
        """Wait for every node healthy, a leader, a primary, and no failover running."""
        n = len(self.clients)

        def settled(s: dict[str, Any]) -> bool:
            return (
                s.get("healthy_count") == n
                and bool(s.get("primary_writer_id"))
                and bool(s.get("raft", {}).get("leader_id"))
                and not s.get("writer_failover", {}).get("failover_in_progress", False)
            )

        return wait_for(
            self.status, settled, timeout=timeout, interval=1.0, what="cluster to settle"
        )


@pytest.fixture(scope="module")
def cluster() -> Iterator[Cluster]:
    c = Cluster(URLS)
    for url in URLS:
        client = _client_for(url)
        node_id = client.cluster.local()["id"]
        c.clients[node_id] = client
        c.url_of[node_id] = url
    yield c
    for client in c.clients.values():
        client.close()


@pytest.fixture
def dead() -> Iterator[ArcClient]:
    """A client pointed at a closed port: any request raises ArcConnectionError."""
    client = ArcClient(host="127.0.0.1", port=1, token="x", max_retries=0, timeout=2)
    yield client
    client.close()


def write_and_locate(
    cluster: Cluster, database: str, measurement: str, lines: list[str]
) -> list[dict[str, Any]]:
    """Write via the primary, flush it, and wait for the manifest to list the files."""
    primary = cluster.clients[cluster.primary_id()]
    primary.write.write_line_protocol(lines, database=database)
    flush(primary)
    return wait_for(
        lambda: [
            f
            for f in cluster.leader().cluster.files(database=database)["files"]
            if f["measurement"] == measurement
        ],
        bool,
        what=f"manifest to list {database}.{measurement}",
    )


# --------------------------------------------------------------------------
# Read routes
# --------------------------------------------------------------------------


def test_status_on_every_node(cluster: Cluster) -> None:
    leader_ids = set()
    for node_id, client in cluster.clients.items():
        status = client.cluster.status()
        assert status["enabled"] is True
        assert status["mode"] == "cluster"
        assert status["local_node_id"] == node_id
        assert status["node_count"] == len(cluster.clients)
        assert status["license"]["valid"] is True
        assert "clustering" in status["license"]["features"]
        leader_ids.add(status["raft"]["leader_id"])
    # Every node agrees on who leads.
    assert len(leader_ids) == 1


def test_nodes_node_local_health(cluster: Cluster) -> None:
    ids = set(cluster.clients)
    for node_id, client in cluster.clients.items():
        listing = client.cluster.nodes()
        assert listing["total"] == len(ids)
        assert {n["id"] for n in listing["nodes"]} == ids

        local = client.cluster.local()
        assert local["id"] == node_id
        assert local["is_local"] is True
        assert set(local["capabilities"]) == {
            "can_ingest",
            "can_query",
            "can_compact",
            "can_coordinate",
        }

        for other in ids:
            assert client.cluster.node(other)["id"] == other

        health = client.cluster.health()
        assert health["total"] == len(ids)
        assert health["healthy"] == len(ids)
        assert health["unhealthy"] == 0

    # Exactly one writer reports itself primary, and it matches status().
    primaries = [
        n["id"] for n in cluster.any().cluster.nodes()["nodes"] if n["writer_state"] == "primary"
    ]
    assert primaries == [cluster.primary_id()]


def test_nodes_filters(cluster: Cluster) -> None:
    client = cluster.any()
    readers = client.cluster.nodes(role="reader")
    assert readers["total"] == len(cluster.readers())
    assert all(n["role"] == "reader" for n in readers["nodes"])
    assert client.cluster.nodes(role="writer", state="healthy")["total"] == len(cluster.writers())
    with pytest.raises(ArcServerError) as exc:
        client.cluster.nodes(role="bogus")
    assert exc.value.status_code == 400


def test_node_unknown_raises_not_found(cluster: Cluster) -> None:
    with pytest.raises(ArcNotFoundError):
        cluster.any().cluster.node("no-such-node")


def test_files_pagination_shape(cluster: Cluster) -> None:
    db = unique("sdkcl_pg")
    write_and_locate(cluster, db, "pg", ["pg,host=a v=1", "pg2,host=a v=2"])
    wait_for(
        lambda: cluster.leader().cluster.files(database=db)["total"],
        lambda t: t >= 2,
        what="both files in the manifest",
    )
    page = cluster.leader().cluster.files(database=db, limit=1)
    assert page["total"] == 1
    assert page["next_cursor"]
    rest = cluster.leader().cluster.files(database=db, cursor=page["next_cursor"], limit=10)
    assert {f["path"] for f in rest["files"]}.isdisjoint({f["path"] for f in page["files"]})
    assert rest["next_cursor"] == ""


async def test_async_cluster_reads_and_errors(cluster: Cluster) -> None:
    leader_id = cluster.leader_id()
    follower_id = next(n for n in cluster.clients if n != leader_id)
    async with _async_client_for(cluster.url_of[leader_id]) as leader:
        status = await leader.cluster.status()
        assert status["raft"]["leader_id"] == leader_id
        assert (await leader.cluster.local())["id"] == leader_id
        assert (await leader.cluster.nodes())["total"] == len(cluster.clients)
        assert (await leader.cluster.node(follower_id))["id"] == follower_id
        assert (await leader.cluster.health())["healthy"] == len(cluster.clients)
        assert "files" in await leader.cluster.files(limit=1)
        claims = await leader.cluster.stream_claims()
        assert isinstance(claims["claims"], list)
        with pytest.raises(ArcNotFoundError):
            await leader.cluster.abort_compaction_job("sdk-no-such-job", confirm=True)
        plan = await leader.cluster.converge_voters()
        assert plan["success"] is True
        assert plan["result"]["dry_run"] is True
    async with _async_client_for(cluster.url_of[follower_id]) as follower:
        with pytest.raises(ArcServerError) as exc:
            await follower.cluster.policies()
        assert exc.value.status_code == 409
        assert exc.value.response_body["leader"] == leader_id


# --------------------------------------------------------------------------
# Readiness
# --------------------------------------------------------------------------


def test_write_readiness_by_role(cluster: Cluster) -> None:
    primary = cluster.primary_id()
    for node_id, client in cluster.clients.items():
        ready = client.write_readiness()
        if node_id == primary:
            assert ready.ready is True
            assert ready.status == "ready"
        else:
            # Readers and standby writers in local-storage mode.
            assert ready.ready is False, node_id
            assert ready.status == "not_write_target", node_id
            assert ready.reason
        # General readiness is unaffected by the write role.
        assert client.readiness().ready is True


# --------------------------------------------------------------------------
# confirm=False gates: nothing may be sent (a dead endpoint proves it)
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "call",
    [
        pytest.param(lambda c: c.cluster.delete_file("db/m/x.parquet"), id="delete_file"),
        pytest.param(lambda c: c.cluster.demote_writer("arc-writer1"), id="demote_writer"),
        pytest.param(lambda c: c.cluster.converge_voters(dry_run=False), id="converge_act"),
        pytest.param(
            lambda c: c.cluster.release_stream_claim("00000000000000ab", 1, abandon=True),
            id="release_abandon",
        ),
        pytest.param(lambda c: c.cluster.abort_compaction_job("job"), id="abort_compaction"),
        pytest.param(
            lambda c: c.cluster.adopt_policies("retention", "arc-writer1", "h"),
            id="adopt_policies",
        ),
        pytest.param(lambda c: c.backup.restore("backup-x"), id="backup_restore"),
        pytest.param(lambda c: c.databases.delete("somedb"), id="databases_delete"),
        pytest.param(lambda c: c.reconciliation.trigger(dry_run=False), id="reconcile_act"),
    ],
)
def test_confirm_gates_send_nothing(dead: ArcClient, call: Callable[[ArcClient], Any]) -> None:
    with pytest.raises(ArcValidationError):
        call(dead)


def test_dead_client_really_sends(dead: ArcClient) -> None:
    # Control for the gate test: the same client does reach the network.
    with pytest.raises(ArcConnectionError):
        dead.cluster.delete_file("db/m/x.parquet", confirm=True)


# --------------------------------------------------------------------------
# delete_file
# --------------------------------------------------------------------------


def test_delete_file_removes_it_cluster_wide(cluster: Cluster) -> None:
    db = unique("sdkcl_df")
    files = write_and_locate(cluster, db, "df", ["df,host=a v=1", "df,host=b v=2"])
    path = files[0]["path"]
    # Wait for every node's manifest to carry it first, so "gone" means deleted.
    for client in cluster.clients.values():
        wait_for(
            lambda c=client: {f["path"] for f in c.cluster.files(database=db)["files"]},
            lambda paths: path in paths,
            what=f"{path} on every node",
        )
    if HAVE_DOCKER:
        for node_id in cluster.clients:
            wait_for(
                lambda n=node_id: exists_on(n, path), bool, what=f"replicated copy on {node_id}"
            )

    result = cluster.leader().cluster.delete_file(path, confirm=True, reason="sdk live test")
    assert result["success"] is True
    assert result["path"] == path

    for client in cluster.clients.values():
        wait_for(
            lambda c=client: {f["path"] for f in c.cluster.files(database=db)["files"]},
            lambda paths: path not in paths,
            what=f"{path} gone from every manifest",
        )
    if HAVE_DOCKER:
        for node_id in cluster.clients:
            wait_for(
                lambda n=node_id: exists_on(n, path),
                lambda present: not present,
                what=f"{path} unlinked on {node_id}",
            )

    # A second delete of the same path is a 404.
    with pytest.raises(ArcNotFoundError):
        cluster.leader().cluster.delete_file(path, confirm=True)


def test_delete_file_from_follower(cluster: Cluster) -> None:
    """The manifest delete goes through Raft; a follower must still accept it."""
    db = unique("sdkcl_dff")
    path = write_and_locate(cluster, db, "dff", ["dff,host=a v=1"])[0]["path"]
    follower = cluster.follower()
    wait_for(
        lambda: {f["path"] for f in follower.cluster.files(database=db)["files"]},
        lambda paths: path in paths,
        what="follower manifest",
    )
    result = follower.cluster.delete_file(path, confirm=True)
    assert result["success"] is True
    wait_for(
        lambda: {f["path"] for f in cluster.leader().cluster.files(database=db)["files"]},
        lambda paths: path not in paths,
        what="gone on the leader",
    )


def test_delete_file_unknown_path(cluster: Cluster) -> None:
    with pytest.raises(ArcNotFoundError):
        cluster.leader().cluster.delete_file("sdkcl_none/m/2020/01/01/00/x.parquet", confirm=True)


# --------------------------------------------------------------------------
# Reconciliation
# --------------------------------------------------------------------------


def test_reconciliation_status_trigger_get_run(cluster: Cluster) -> None:
    for node_id, client in cluster.clients.items():
        status = client.reconciliation.status()
        assert status["enabled"] is True, node_id
        run = client.reconciliation.trigger()
        assert run.dry_run is True
        assert run.role == cluster.role_of(node_id)
        # Runs live in the node's own in-memory buffer.
        fetched = client.reconciliation.get_run(run.id)
        assert fetched.id == run.id
        assert fetched.dry_run is True
    with pytest.raises(ArcNotFoundError):
        cluster.leader().reconciliation.get_run("sdk-no-such-run")


def test_reconciliation_act_when_dry_run_is_clean(cluster: Cluster) -> None:
    leader = cluster.leader()
    plan = leader.reconciliation.trigger()
    if plan.manifest_deletes or plan.storage_deletes or plan.orphan_manifest_count:
        pytest.skip(f"dry run would delete something: {plan.model_dump()}")
    run = leader.reconciliation.trigger(dry_run=False, confirm=True)
    assert run.dry_run is False
    assert run.manifest_deletes == 0
    assert run.storage_deletes == 0
    assert leader.reconciliation.get_run(run.id).dry_run is False


# --------------------------------------------------------------------------
# Data DELETE routing and allow_unreachable
# --------------------------------------------------------------------------


def test_delete_on_standby_writer_is_refused_not_forwarded(cluster: Cluster) -> None:
    """Writes are forwarded to the primary; a data DELETE is not (503, no retry)."""
    db = unique("sdkcl_sb")
    write_and_locate(cluster, db, "sb", ["sb,host=a v=1"])
    standby = cluster.clients[cluster.standby_writer()]
    # A dry run is allowed anywhere.
    assert standby.delete.delete(db, "sb", "host = 'a'").success is True
    with pytest.raises(ArcServerError) as exc:
        standby.delete.delete(db, "sb", "host = 'a'", dry_run=False, confirm=True)
    assert exc.value.status_code == 503
    assert "not primary writer" in str(exc.value)
    assert not isinstance(exc.value, ArcIngestRefusedError)


@pytest.fixture
def missing_on_primary(cluster: Cluster) -> Iterator[tuple[str, str, str]]:
    """A manifest file removed from the primary's disk (kept on the other nodes).

    Yields ``(database, measurement, path)``. Teardown removes the path from
    the manifest cluster-wide with delete_file.
    """
    if not HAVE_DOCKER:
        pytest.skip("needs the docker CLI")
    db = unique("sdkcl_unr")
    path = write_and_locate(cluster, db, "unr", ["unr,host=a v=1", "unr,host=b v=2"])[0]["path"]
    primary = cluster.primary_id()
    docker("exec", primary, "rm", "-f", f"{DATA_DIR}/{path}")
    assert not exists_on(primary, path)
    yield db, "unr", path
    with contextlib.suppress(ArcNotFoundError):
        cluster.leader().cluster.delete_file(path, confirm=True, reason="sdk test cleanup")


def _long_timeout_primary(cluster: Cluster) -> ArcClient:
    return _client_for(
        cluster.url_of[cluster.primary_id()], timeout=ARC_DELETE_LOCAL_WAIT * 3, max_retries=0
    )


@needs_docker
def test_delete_reports_unreachable_files(
    cluster: Cluster, missing_on_primary: tuple[str, str, str]
) -> None:
    db, measurement, path = missing_on_primary
    with _long_timeout_primary(cluster) as primary:
        plan = primary.delete.delete(db, measurement, "host = 'a'")
        assert plan.dry_run is True
        assert plan.unreachable_files == [path]

        # Without the flag: Arc waits deleteLocalWait, then refuses with 503
        # naming the files. The SDK must not retry and must keep the body.
        with pytest.raises(ArcServerError) as exc:
            primary.delete.delete(db, measurement, "host = 'a'", dry_run=False, confirm=True)
        assert exc.value.status_code == 503
        assert exc.value.response_body["unreachable_files"] == [path]

        result = primary.delete.delete(
            db, measurement, "host = 'a'", dry_run=False, confirm=True, allow_unreachable=True
        )
        assert result.success is True
        assert result.unreachable_files == [path]
        assert result.unflushed_nodes == []  # local storage: no peer flush


@needs_docker
def test_delete_allow_unreachable_with_default_timeout(
    cluster: Cluster, missing_on_primary: tuple[str, str, str]
) -> None:
    """The documented allow_unreachable path, with the client's default settings.

    Arc waits up to 30 s for the missing files before it answers, which is
    also the SDK's default request timeout.
    """
    db, measurement, path = missing_on_primary
    with _client_for(cluster.url_of[cluster.primary_id()]) as primary:
        result = primary.delete.delete(
            db, measurement, "host = 'a'", dry_run=False, confirm=True, allow_unreachable=True
        )
    assert result.unreachable_files == [path]


@needs_docker
def test_delete_with_a_stopped_writer(cluster: Cluster) -> None:
    """On local storage a stopped standby is not "unreachable" for a delete."""
    db = unique("sdkcl_stop")
    # host=c stays, so the database still has data when it is dropped below.
    write_and_locate(cluster, db, "st", ["st,host=a v=1", "st,host=b v=2", "st,host=c v=3"])
    victim = cluster.standby_writer(exclude=(cluster.leader_id(),))
    primary = cluster.clients[cluster.primary_id()]
    docker("stop", victim)
    try:
        result = primary.delete.delete(db, "st", "host = 'a'", dry_run=False, confirm=True)
        assert result.success is True
        assert result.deleted_count == 1
        assert result.unreachable_files == []
        assert result.unflushed_nodes == []

        flagged = primary.delete.delete(
            db, "st", "host = 'b'", dry_run=False, confirm=True, allow_unreachable=True
        )
        assert flagged.success is True
        assert flagged.unreachable_files == []

        body = primary.databases.delete(db, confirm=True, allow_unreachable=True)
        assert "deleted" in body["message"]
        assert not body.get("unflushed_nodes")
    finally:
        docker("start", victim)
        cluster.wait_settled()


# --------------------------------------------------------------------------
# Backup / restore
# --------------------------------------------------------------------------


def test_scoped_backup_and_restore_modes(cluster: Cluster) -> None:
    db = unique("sdkcl_bk")
    write_and_locate(cluster, db, "bk", ["bk,host=a v=1"])
    node = cluster.clients[cluster.primary_id()]
    started = node.backup.create(databases=[db])
    assert started["status"] == "running"
    assert started["databases"] == [db]

    def latest() -> dict[str, Any]:
        return node.backup.status()

    done = wait_for(
        latest,
        lambda s: s.get("operation") == "backup" and s.get("status") == "completed",
        what="backup",
    )
    assert done["scope"] == [db]
    backup_id = done["backup_id"]
    try:
        listing = node.backup.list()
        assert any(b["backup_id"] == backup_id for b in listing["backups"])
        assert node.backup.get(backup_id)

        for mode in ("replace", "merge"):
            restored = node.backup.restore(backup_id, confirm=True, mode=mode)
            assert restored["mode"] == mode
            final = wait_for(
                latest,
                lambda s, m=mode: s.get("operation") == "restore"
                and s.get("mode") == m
                and s.get("status") in ("completed", "failed"),
                what=f"restore {mode}",
            )
            assert final["status"] == "completed", final
        # Arc's default for a scoped backup on a cluster node is replace.
        default = node.backup.restore(backup_id, confirm=True)
        assert default["mode"] == "replace"
        wait_for(latest, lambda s: s.get("status") == "completed", what="default restore")
        # The restored data is in the manifest again.
        assert cluster.leader().cluster.files(database=db)["total"] >= 1
    finally:
        with contextlib.suppress(Exception):
            node.backup.delete(backup_id)


# --------------------------------------------------------------------------
# Policies
# --------------------------------------------------------------------------


@pytest.fixture
def policies_present(cluster: Cluster) -> Iterator[None]:
    """A retention policy and a CQ, so there is something to adopt."""
    db = unique("sdkcl_pol")
    primary = cluster.clients[cluster.primary_id()]
    write_and_locate(cluster, db, "src", ["src,host=a v=1"])
    rp = primary.retention.create(unique("rp"), db, retention_days=30)
    cq = None
    # continuous_query.enabled=false: Arc registers no CQ routes at all
    # ("Cannot POST /api/v1/continuous_queries" -> ArcNotFoundError). The
    # policies endpoint still reports the cq kind, so the tests go on without one.
    with contextlib.suppress(ArcNotFoundError):
        cq = primary.continuous_queries.create(
            name=unique("cq"),
            database=db,
            source_measurement="src",
            destination_measurement="dst",
            query="SELECT time_bucket(INTERVAL '1 minute', time) AS time, avg(v) AS v "
            "FROM src WHERE time >= {start_time} AND time < {end_time} GROUP BY 1",
            interval="1m",
        )
    yield
    with contextlib.suppress(Exception):
        primary.retention.delete(rp.id)
    if cq is not None:
        with contextlib.suppress(Exception):
            primary.continuous_queries.delete(cq.id)


def test_policies_on_leader(cluster: Cluster, policies_present: None) -> None:
    leader = cluster.leader()
    both = leader.cluster.policies()
    assert both["success"] is True
    for kind in ("retention", "cq"):
        one = leader.cluster.policies(kind=kind)
        assert one["success"] is True
        assert kind in one
        other = "cq" if kind == "retention" else "retention"
        assert other not in one
        candidates = one[kind]
        if candidates["seeded"]:
            # The cluster already holds its policies in Raft: there is no
            # per-node candidate list to choose from.
            assert "nodes" not in candidates
        else:
            assert candidates["nodes"], candidates
            for node_set in candidates["nodes"].values():
                assert node_set.get("hash") or node_set.get("error")


def test_policies_on_follower_names_the_leader(cluster: Cluster) -> None:
    leader_id = cluster.leader_id()
    with pytest.raises(ArcServerError) as exc:
        cluster.follower().cluster.policies(kind="retention")
    assert exc.value.status_code == 409
    assert exc.value.response_body["leader"] == leader_id
    assert "leader" in str(exc.value).lower()


def test_policies_invalid_kind_is_local() -> None:
    client = ArcClient(host="127.0.0.1", port=1, max_retries=0)
    with pytest.raises(ArcValidationError):
        client.cluster.policies(kind="tiering")


def test_adopt_policies(cluster: Cluster, policies_present: None) -> None:
    leader = cluster.leader()
    for kind in ("retention", "cq"):
        candidates = leader.cluster.policies(kind=kind)[kind]
        if not candidates["seeded"]:
            node, node_set = next(
                (n, s) for n, s in candidates["nodes"].items() if s.get("hash") is not None
            )
            accepted = leader.cluster.adopt_policies(kind, node, node_set["hash"], confirm=True)
            assert accepted["success"] is True
            continue
        # Already seeded: Arc refuses with 409, which the SDK raises (the
        # 200/success:false path _check_adopted handles is not reached).
        with pytest.raises(ArcServerError) as exc:
            leader.cluster.adopt_policies(kind, cluster.leader_id(), "deadbeef", confirm=True)
        assert exc.value.status_code == 409
        assert "already holds" in str(exc.value)


# --------------------------------------------------------------------------
# Stream claims and wedged compaction jobs
# --------------------------------------------------------------------------


def test_stream_claims_listing(cluster: Cluster) -> None:
    for client in cluster.clients.values():
        claims = client.cluster.stream_claims()
        for key in ("enabled", "instances", "claims", "departed_writers", "delete_records"):
            assert key in claims
    status = cluster.status()
    assert status["stream_claims"]["claims"] == len(
        cluster.leader().cluster.stream_claims()["claims"]
    )


def test_release_stream_claim(cluster: Cluster) -> None:
    leader = cluster.leader()
    claims = leader.cluster.stream_claims()["claims"]
    unfinished = [c for c in claims if not c.get("finished")]
    if unfinished:  # pragma: no cover - not reachable without stream takeover
        claim = unfinished[0]
        result = leader.cluster.release_stream_claim(claim["instance"], claim["epoch"])
        assert result["success"] is True
        return
    # No claim exists (stream takeover off): Arc answers 404 on the leader...
    with pytest.raises(ArcNotFoundError):
        leader.cluster.release_stream_claim(0xAB, 1)
    with pytest.raises(ArcNotFoundError):
        leader.cluster.release_stream_claim("00000000000000ab", 1, abandon=True, confirm=True)
    # ...and 409 naming the leader on a follower.
    with pytest.raises(ArcServerError) as exc:
        cluster.follower().cluster.release_stream_claim(0xAB, 1)
    assert exc.value.status_code == 409
    assert "leader" in str(exc.value)


def test_abort_compaction_job_unknown(cluster: Cluster) -> None:
    status = cluster.status()
    wedged = status.get("compaction_commit", {}).get("wedged_jobs") or []
    assert not wedged or isinstance(wedged, list)
    with pytest.raises(ArcNotFoundError):
        cluster.leader().cluster.abort_compaction_job("sdk-no-such-job", confirm=True)
    # Path segments are escaped, so a job id with a slash cannot change the route.
    with pytest.raises(ArcNotFoundError):
        cluster.leader().cluster.abort_compaction_job("a/b", confirm=True)


# --------------------------------------------------------------------------
# Compactor lease
# --------------------------------------------------------------------------


def test_assign_compactor(cluster: Cluster) -> None:
    leader = cluster.leader()
    nodes = leader.cluster.nodes()["nodes"]
    holder = next((n["id"] for n in nodes if n["is_active_compactor"]), None)
    has_compactor_role = any(n["role"] == "compactor" for n in nodes)
    target = next(n["id"] for n in nodes if n["role"] == "writer" and n["id"] != holder)

    # Non-leader: always 409.
    follower = cluster.follower()
    with pytest.raises(ArcServerError) as exc:
        follower.cluster.assign_compactor(target)
    assert exc.value.status_code == 409

    if not has_compactor_role and holder is None:
        # No compactor node: nothing manages a lease, and Arc refuses before
        # it even looks at the target (so an unknown node is a 409, not 404).
        for node_id in (target, cluster.readers()[0] if cluster.readers() else target, "nope"):
            with pytest.raises(ArcServerError) as exc:
                leader.cluster.assign_compactor(node_id)
            assert exc.value.status_code == 409
            assert "does not manage a compactor lease" in str(exc.value)
        assert cluster.status()["active_compactor"] == {"assigned": False}
        return

    result = leader.cluster.assign_compactor(target)  # pragma: no cover - topology dependent
    assert result["success"] is True
    assert result["node_id"] == target
    assert "preemption_suppressed_seconds" in result
    wait_for(
        lambda: [n["id"] for n in leader.cluster.nodes()["nodes"] if n["is_active_compactor"]],
        lambda ids: ids == [target],
        what="compactor lease to move",
    )
    assert cluster.status()["active_compactor"]["node_id"] == target


# --------------------------------------------------------------------------
# Raft voter set
# --------------------------------------------------------------------------


def test_converge_voters(cluster: Cluster) -> None:
    leader = cluster.leader()
    plan = leader.cluster.converge_voters()
    assert plan["success"] is True
    result = plan["result"]
    assert result["dry_run"] is True
    before = result["voting_servers_before"]

    with pytest.raises(ArcServerError) as exc:
        cluster.follower().cluster.converge_voters()
    assert exc.value.status_code == 409

    if result.get("demoted") or result["voting_servers_after"] != before:
        pytest.skip(f"converge would change the voter set: {result}")
    acted = leader.cluster.converge_voters(dry_run=False, confirm=True)
    assert acted["success"] is True
    assert acted["result"]["dry_run"] is False
    assert acted["result"]["voting_servers_after"] == before
    assert not acted["result"].get("demoted")
    membership = cluster.status()["raft"]["membership"]
    assert membership["mismatches"] == 0
    assert membership["voting_servers"] == before


# --------------------------------------------------------------------------
# Fenced writes (ingest refusals)
# --------------------------------------------------------------------------

# A standby writer forwards writes to the primary unless the request says it
# was already forwarded; then it handles the write locally and refuses it with
# 503 + X-Arc-Ingest-Fenced, exactly as during a failover window.
FORWARDED_BY = "X-Arc-Forwarded-By"


def _fenced_client(cluster: Cluster, fence_first_only: bool, max_retries: int) -> Any:
    standby = cluster.standby_writer()
    client = _client_for(cluster.url_of[standby], max_retries=max_retries, database="sdkcl_fence")
    sent: list[int] = []

    def hook(request: httpx.Request) -> None:
        if "/write/" not in request.url.path:
            return
        sent.append(1)
        if not fence_first_only or len(sent) == 1:
            request.headers[FORWARDED_BY] = "sdk-live-test"

    client._get_http()._get_client().event_hooks["request"] = [hook]
    return client, sent


def test_fenced_single_measurement_write_is_resent(cluster: Cluster) -> None:
    client, sent = _fenced_client(cluster, fence_first_only=True, max_retries=2)
    with client:
        client.write.write_line_protocol(["fence_one,host=a v=1"])
    # First attempt fenced, the resend (after Retry-After) forwarded and stored.
    assert len(sent) == 2


def test_fenced_single_measurement_retries_then_raises(cluster: Cluster) -> None:
    client, sent = _fenced_client(cluster, fence_first_only=False, max_retries=1)
    with client, pytest.raises(ArcIngestRefusedError) as exc:
        client.write.write_line_protocol(["fence_one,host=a v=1"])
    assert exc.value.reason == "fenced"
    assert exc.value.retry_safe is True
    assert exc.value.status_code == 503
    assert len(sent) == 2


def test_fenced_multi_measurement_line_protocol_not_resent(cluster: Cluster) -> None:
    client, sent = _fenced_client(cluster, fence_first_only=False, max_retries=3)
    with client, pytest.raises(ArcIngestRefusedError) as exc:
        client.write.write_line_protocol(["fence_a,host=a v=1", "fence_b,host=a v=2"])
    assert exc.value.reason == "fenced"
    assert exc.value.retry_safe is False
    assert len(sent) == 1


def test_fenced_msgpack_write_is_resent(cluster: Cluster) -> None:
    client, sent = _fenced_client(cluster, fence_first_only=True, max_retries=2)
    with client:
        client.write.write_columnar(
            "fence_mp", {"time": [int(time.time() * 1e6)], "host": ["a"], "v": [1.0]}
        )
    assert len(sent) == 2


# --------------------------------------------------------------------------
# Primary writer hand-over (runs late: it moves the primary and restores it)
# --------------------------------------------------------------------------


def test_demote_writer_refusals(cluster: Cluster) -> None:
    primary = cluster.primary_id()
    standby = cluster.standby_writer()
    with pytest.raises(ArcServerError) as exc:
        cluster.leader().cluster.demote_writer(standby, confirm=True)
    assert exc.value.status_code == 409
    assert primary in str(exc.value)  # Arc names the real primary

    with pytest.raises(ArcServerError) as exc:
        cluster.follower().cluster.demote_writer(primary, confirm=True)
    assert exc.value.status_code == 409
    assert "not the leader" in str(exc.value)


def _demote_and_wait(cluster: Cluster, node_id: str) -> str:
    result = cluster.leader().cluster.demote_writer(node_id, confirm=True)
    assert result["success"] is True
    assert result["node_id"] == node_id
    new_primary = result["new_primary"]
    assert new_primary and new_primary != node_id
    wait_for(cluster.primary_id, lambda p: p == new_primary, what="new primary in status")
    cluster.wait_settled()
    wait_for(
        lambda: cluster.clients[new_primary].write_readiness().ready,
        bool,
        what="new primary ready for writes",
    )
    return new_primary


def test_demote_primary_and_restore(cluster: Cluster) -> None:
    cluster.wait_settled()
    original = cluster.primary_id()
    writers = cluster.writers()
    if len(writers) < 2:
        pytest.skip("needs two writers")

    new_primary = _demote_and_wait(cluster, original)
    assert new_primary in writers

    # How the cluster reports it.
    nodes = {n["id"]: n for n in cluster.any().cluster.nodes()["nodes"]}
    assert nodes[new_primary]["writer_state"] == "primary"
    assert nodes[original]["writer_state"] != "primary"
    old = cluster.clients[original].write_readiness()
    assert old.ready is False
    assert old.status == "not_write_target"
    # Writes still land through the old primary (forwarded to the new one).
    db = unique("sdkcl_dm")
    cluster.clients[original].write.write_line_protocol(["dm,host=a v=1"], database=db)
    flush(cluster.clients[new_primary])
    wait_for(
        lambda: cluster.leader().cluster.files(database=db)["files"],
        bool,
        what="write after hand-over in the manifest",
    )
    assert {f["origin_node_id"] for f in cluster.leader().cluster.files(database=db)["files"]} == {
        new_primary
    }

    # Restore: there is no promote; demote until the original holds it again.
    current = new_primary
    for _ in range(len(writers) * 2):
        if current == original:
            break
        current = _demote_and_wait(cluster, current)
    assert cluster.primary_id() == original


# --------------------------------------------------------------------------
# remove_node (opt-in; may require a cluster rebuild)
# --------------------------------------------------------------------------


@needs_docker
@pytest.mark.skipif(
    os.environ.get("ARC_TEST_CLUSTER_REMOVE_NODE") != "1",
    reason="Set ARC_TEST_CLUSTER_REMOVE_NODE=1 to remove (and try to rejoin) the reader.",
)
def test_remove_node_reader(cluster: Cluster) -> None:
    readers = cluster.readers()
    if not readers:
        pytest.skip("no reader to remove")
    target = readers[0]
    leader = cluster.leader()

    with pytest.raises(ArcServerError) as exc:
        cluster.clients[target].cluster.remove_node(target)
    assert exc.value.status_code == 400  # self-removal is refused

    result = leader.cluster.remove_node(target)
    assert result["success"] is True
    assert result["node_id"] == target
    wait_for(
        lambda: {n["id"] for n in leader.cluster.nodes()["nodes"]},
        lambda ids: target not in ids,
        what="node gone from the registry",
    )

    docker("restart", target)
    wait_for(
        lambda: {n["id"] for n in leader.cluster.nodes()["nodes"]},
        lambda ids: target in ids,
        timeout=120,
        what=f"{target} to rejoin after restart",
    )
    cluster.wait_settled(timeout=120)
