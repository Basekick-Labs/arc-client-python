"""Asynchronous cluster-management client for Arc (enterprise).

Wraps ``/api/v1/cluster``. The status/node/health reads are available to any
valid token; every other route (files, node removal, the operator controls
below) requires an admin token. None of them is gated on a license feature
beyond clustering itself. When clustering is disabled the reads (and
``policies``) return 200 with ``{"enabled": false, ...}``; the operator
controls answer 409 instead, and :meth:`AsyncClusterClient.adopt_policies` raises
:class:`~arc_client.exceptions.ArcError`, so "nothing happened" never reads
as success.

Responses are returned as raw dicts: they are operator diagnostics whose
fields Arc extends between releases, not a promised contract. Controls that
lose data or leave the cluster without a role holder require ``confirm=True``
and raise :class:`~arc_client.exceptions.ArcValidationError` otherwise.
"""

from __future__ import annotations

from typing import Any, Optional, Union
from urllib.parse import quote

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError, ArcValidationError
from arc_client.http.async_http import AsyncHTTPClient
from arc_client.management.cluster import (
    _check_adopted,
    _check_kind,
    _instance_segment,
    _require_confirm,
    _segment,
)


class AsyncClusterClient:
    """Inspect and manage the Arc cluster (enterprise)."""

    def __init__(self, http: AsyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    async def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            response = await getattr(self._http, method)(path, **kwargs)
            return response.json()
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"Cluster request failed ({method.upper()} {path}): {e}") from e

    async def status(self) -> Any:
        """Get overall cluster status (includes enabled/mode/license)."""
        return await self._json("get", "/api/v1/cluster")

    async def nodes(self, role: Optional[str] = None, state: Optional[str] = None) -> Any:
        """List cluster nodes, optionally filtered by role and/or state."""
        params = {k: v for k, v in {"role": role, "state": state}.items() if v}
        return await self._json("get", "/api/v1/cluster/nodes", params=params or None)

    async def node(self, node_id: str) -> Any:
        """Get a single node by id."""
        return await self._json("get", f"/api/v1/cluster/nodes/{node_id}")

    async def local(self) -> Any:
        """Get the local node's info and capabilities."""
        return await self._json("get", "/api/v1/cluster/local")

    async def health(self) -> Any:
        """Get cluster health counts."""
        return await self._json("get", "/api/v1/cluster/health")

    async def files(
        self,
        database: Optional[str] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Any:
        """List cluster files (admin). Supports cursor pagination."""
        params = {
            k: v
            for k, v in {"database": database, "cursor": cursor, "limit": limit}.items()
            if v is not None
        }
        return await self._json("get", "/api/v1/cluster/files", params=params or None)

    async def delete_file(
        self, path: str, confirm: bool = False, reason: Optional[str] = None
    ) -> Any:
        """Remove a file from the cluster manifest and delete it cluster-wide (admin).

        Destructive: on local storage every node unlinks the file; on shared
        object storage the object is orphaned until the reconciliation
        sweeper removes it. ``path`` is matched exactly, so malformed keys
        that retention cannot remove can still be targeted. Needs Arc
        2026.09.2+.

        Args:
            path: Manifest path of the file (max 4096 bytes).
            confirm: Must be True.
            reason: Recorded in the audit log and Raft entry (max 256
                characters; Arc defaults to ``"operator"``).

        Raises:
            ArcValidationError: ``confirm`` is not True or ``path`` is empty.
            ArcNotFoundError: The path is not in the cluster manifest.
        """
        if not path:
            raise ArcValidationError("path is required")
        _require_confirm(confirm, "delete a cluster file")
        params = {"path": path, "confirm": "true"}
        if reason:
            params["reason"] = reason
        return await self._json("delete", "/api/v1/cluster/files", params=params)

    async def remove_node(self, node_id: str) -> Any:
        """Remove a node from the cluster via Raft (admin).

        Raises:
            ArcError: If Arc did not remove it. A node without clustering
                answers 200 with ``success: false``, which would otherwise
                read as a successful removal.
        """
        if not node_id:
            raise ArcValidationError("node_id is required")
        result = await self._json("delete", f"/api/v1/cluster/nodes/{quote(node_id, safe='')}")
        if isinstance(result, dict) and result.get("success") is False:
            reason = result.get("error") or result.get("reason") or "clustering is not enabled"
            raise ArcError(f"Node {node_id!r} was not removed: {reason}")
        return result

    async def demote_writer(self, node_id: str, confirm: bool = False) -> Any:
        """Hand the primary-writer role off ``node_id`` so the cluster elects another (admin).

        Use it to drain a writer, or to recover a cluster without automatic
        failover whose primary is gone for good. Not gated on the
        writer-failover license feature. If no other writer can take the
        role, the response has ``new_primary: None`` and the cluster has no
        primary writer until one can, hence ``confirm``. Arc answers 409 when
        ``node_id`` is not the current primary or this node is not the Raft
        leader. Needs Arc 2026.09.2+.

        Raises:
            ArcValidationError: ``confirm`` is not True.
        """
        segment = _segment(node_id, "node_id")
        _require_confirm(confirm, "demote the primary writer")
        return await self._json("post", f"/api/v1/cluster/writers/{segment}/demote")

    async def assign_compactor(self, node_id: str) -> Any:
        """Hand the compactor lease to ``node_id`` (admin).

        The target is explicit and nothing is deleted, so no ``confirm``.
        Arc refuses (409) readers, unhealthy nodes, the current holder and
        non-leader nodes, and, before it even checks the target, a cluster
        that does not manage a compactor lease (no node has the compactor
        role), so an unknown node then gets 409 rather than 404. The
        response's ``preemption_suppressed_seconds`` says how long the choice
        is protected from automatic preemption.
        Needs Arc 2026.09.2+.
        """
        if not node_id:
            raise ArcValidationError("node_id is required")
        return await self._json(
            "post", "/api/v1/cluster/compactor/assign", json={"node_id": node_id}
        )

    async def converge_voters(
        self,
        dry_run: bool = True,
        allow_single_voter: bool = False,
        confirm: bool = False,
    ) -> Any:
        """Converge the Raft voter set onto the role-based rule (admin).

        Plans by default. With ``dry_run=False`` it revokes votes the nodes'
        roles do not grant, which changes Raft membership, so it also needs
        ``confirm=True``. A partial converge answers 409 with the result.
        Needs Arc 2026.09.2+.

        Args:
            dry_run: Plan only (default).
            allow_single_voter: Permit converging down to one voter.
            confirm: Required when ``dry_run`` is False.

        Raises:
            ArcValidationError: ``dry_run`` is False and ``confirm`` is not True.
        """
        if not dry_run:
            _require_confirm(confirm, "change the Raft voter set")
        body: dict[str, Any] = {"dry_run": dry_run}
        if allow_single_voter:
            body["allow_single_voter"] = True
        return await self._json("post", "/api/v1/cluster/voters/converge", json=body)

    async def stream_claims(self) -> Any:
        """List stream-takeover writer processes, claims and departed writers (admin).

        Read from this node's manifest. Needs Arc 2027.01.1+.
        """
        return await self._json("get", "/api/v1/cluster/stream-claims")

    async def release_stream_claim(
        self,
        instance: Union[str, int],
        epoch: int,
        abandon: bool = False,
        confirm: bool = False,
    ) -> Any:
        """Release an unfinished stream claim its claimant cannot finish (admin, leader only).

        Another writer (its claimant too) may then take the instance over.
        With ``abandon=True`` the claim is finished instead: batches it has
        not published are left to their origin and lost when the origin is
        gone, so ``abandon`` requires ``confirm=True``. Needs Arc 2027.01.1+.

        Args:
            instance: The 16-digit hex id :meth:`stream_claims` lists (an int
                is formatted as that hex id).
            epoch: The claim's epoch, so a later claim is not released by
                mistake.
            abandon: Finish the claim instead of releasing it.
            confirm: Required when ``abandon`` is True.

        Raises:
            ArcValidationError: Bad ``instance``/``epoch``, or ``abandon``
                without ``confirm``.
        """
        segment = _instance_segment(instance)
        if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 1:
            raise ArcValidationError("epoch must be a positive integer")
        if abandon:
            _require_confirm(confirm, "abandon a stream claim")
        body: dict[str, Any] = {"epoch": epoch}
        if abandon:
            body["abandon"] = True
        return await self._json(
            "post", f"/api/v1/cluster/stream-claims/{segment}/release", json=body
        )

    async def abort_compaction_job(self, job: str, confirm: bool = False) -> Any:
        """End a wedged compaction job that waits for an operator (admin).

        Meant for an operator who has checked the job's data: its entry is
        dropped and its staged object removed; the manifest and the bucket's
        committed files are not changed. ``GET /api/v1/cluster`` lists the
        candidates as ``compaction_commit.wedged_jobs``. Arc answers 404 for
        an unknown job and 409 for one that is not wedged.
        Needs Arc 2027.01.1+.

        Raises:
            ArcValidationError: ``confirm`` is not True.
        """
        segment = _segment(job, "job")
        _require_confirm(confirm, "abort a compaction job")
        return await self._json("post", f"/api/v1/cluster/compaction-jobs/{segment}/abort")

    async def policies(self, kind: Optional[str] = None) -> Any:
        """List each node's own retention and/or CQ policies with their hashes (admin).

        Run on the Raft leader (others answer 409 naming it). Pass the hash
        to :meth:`adopt_policies`. Needs Arc 2027.01.1+.

        Args:
            kind: ``"retention"`` or ``"cq"``; both by default.
        """
        _check_kind(kind)
        params = {"kind": kind} if kind else None
        return await self._json("get", "/api/v1/cluster/policies", params=params)

    async def adopt_policies(self, kind: str, node: str, hash: str, confirm: bool = False) -> Any:
        """Choose the node whose policies of ``kind`` the cluster adopts (admin, leader only).

        The leader adopts them only if ``node`` still holds exactly the set
        ``hash`` names. Every other node's policies of that kind then stop
        running for good (kept as dormant rows) and IDs may change, so this
        requires ``confirm=True``. Answers 202; ``GET /api/v1/cluster`` shows
        the outcome under ``cluster_policies``. Needs Arc 2027.01.1+.

        Raises:
            ArcValidationError: Bad ``kind``, missing ``node``/``hash``, or
                ``confirm`` is not True.
            ArcError: Clustering is not enabled (Arc answers 200 with
                ``success: false`` rather than an error status).
        """
        _check_kind(kind, required=True)
        if not node or not hash or not hash.strip():
            raise ArcValidationError("node and hash are required (see policies())")
        _require_confirm(confirm, "adopt cluster policies")
        result = await self._json(
            "post",
            "/api/v1/cluster/policies/adopt",
            json={"kind": kind, "node": node, "hash": hash},
        )
        return _check_adopted(result)
