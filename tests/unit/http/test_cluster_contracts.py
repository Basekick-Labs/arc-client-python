"""Cluster operator-control wire contracts shared by sync and async public clients."""

from __future__ import annotations

import inspect
import json
from contextlib import AsyncExitStack

import pytest

from arc_client import ArcClient, AsyncArcClient
from arc_client.exceptions import (
    ArcError,
    ArcNotFoundError,
    ArcPermissionError,
    ArcValidationError,
)


async def call(client, method, *args, **kwargs):
    result = getattr(client.cluster, method)(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def async_mode(request):
    return request.param


async def enter_client(stack, async_mode):
    if async_mode:
        return await stack.enter_async_context(AsyncArcClient(token="t", max_retries=0))
    return stack.enter_context(ArcClient(token="t", max_retries=0))


def body_of(request):
    return json.loads(request.content) if request.content else None


# (method, args, kwargs, http method, path, query params, json body)
CASES = [
    (
        "delete_file",
        ("db/cpu/f.parquet",),
        {"confirm": True},
        "DELETE",
        "/api/v1/cluster/files",
        {"path": "db/cpu/f.parquet", "confirm": "true"},
        None,
    ),
    (
        "delete_file",
        ("db/cpu/f.parquet",),
        {"confirm": True, "reason": "corrupt"},
        "DELETE",
        "/api/v1/cluster/files",
        {"path": "db/cpu/f.parquet", "confirm": "true", "reason": "corrupt"},
        None,
    ),
    (
        "demote_writer",
        ("writer-1",),
        {"confirm": True},
        "POST",
        "/api/v1/cluster/writers/writer-1/demote",
        {},
        None,
    ),
    (
        "assign_compactor",
        ("node-2",),
        {},
        "POST",
        "/api/v1/cluster/compactor/assign",
        {},
        {"node_id": "node-2"},
    ),
    (
        "converge_voters",
        (),
        {},
        "POST",
        "/api/v1/cluster/voters/converge",
        {},
        {"dry_run": True},
    ),
    (
        "converge_voters",
        (),
        {"dry_run": False, "allow_single_voter": True, "confirm": True},
        "POST",
        "/api/v1/cluster/voters/converge",
        {},
        {"dry_run": False, "allow_single_voter": True},
    ),
    ("stream_claims", (), {}, "GET", "/api/v1/cluster/stream-claims", {}, None),
    (
        "release_stream_claim",
        ("00000000000000ab", 3),
        {},
        "POST",
        "/api/v1/cluster/stream-claims/00000000000000ab/release",
        {},
        {"epoch": 3},
    ),
    (
        "release_stream_claim",
        (0xAB, 3),
        {"abandon": True, "confirm": True},
        "POST",
        "/api/v1/cluster/stream-claims/00000000000000ab/release",
        {},
        {"epoch": 3, "abandon": True},
    ),
    (
        "abort_compaction_job",
        ("job-9",),
        {"confirm": True},
        "POST",
        "/api/v1/cluster/compaction-jobs/job-9/abort",
        {},
        None,
    ),
    ("policies", (), {}, "GET", "/api/v1/cluster/policies", {}, None),
    ("policies", (), {"kind": "cq"}, "GET", "/api/v1/cluster/policies", {"kind": "cq"}, None),
    (
        "adopt_policies",
        ("retention", "node-1", "abc123"),
        {"confirm": True},
        "POST",
        "/api/v1/cluster/policies/adopt",
        {},
        {"kind": "retention", "node": "node-1", "hash": "abc123"},
    ),
]


@pytest.mark.parametrize(
    ("method", "args", "kwargs", "verb", "path", "params", "body"),
    CASES,
    ids=[f"{c[0]}-{i}" for i, c in enumerate(CASES)],
)
async def test_wire_contract(
    httpx_mock, async_mode, method, args, kwargs, verb, path, params, body
):
    httpx_mock.add_response(method=verb, json={"success": True})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert (await call(client, method, *args, **kwargs))["success"] is True
    request = httpx_mock.get_request()
    assert request.method == verb
    assert request.url.path == path
    assert dict(request.url.params) == params
    assert body_of(request) == body


@pytest.mark.parametrize(
    ("method", "args", "kwargs"),
    [
        ("delete_file", ("db/cpu/f.parquet",), {}),
        ("delete_file", ("",), {"confirm": True}),
        ("demote_writer", ("writer-1",), {}),
        ("demote_writer", ("",), {"confirm": True}),
        ("assign_compactor", ("",), {}),
        ("converge_voters", (), {"dry_run": False}),
        ("release_stream_claim", ("ab", 3), {"abandon": True}),
        ("release_stream_claim", ("ab", 0), {}),
        ("release_stream_claim", (0, 3), {}),
        ("release_stream_claim", ("", 3), {}),
        ("abort_compaction_job", ("job-9",), {}),
        ("policies", (), {"kind": "both"}),
        ("adopt_policies", ("retention", "node-1", "abc"), {}),
        ("adopt_policies", ("tiering", "node-1", "abc"), {"confirm": True}),
        ("adopt_policies", ("cq", "", "abc"), {"confirm": True}),
        ("adopt_policies", ("cq", "node-1", " "), {"confirm": True}),
    ],
)
async def test_refused_before_request(httpx_mock, async_mode, method, args, kwargs):
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, method, *args, **kwargs)
    assert httpx_mock.get_requests() == []


async def test_node_id_is_path_escaped(httpx_mock, async_mode):
    httpx_mock.add_response(method="POST", json={"success": True})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        await call(client, "demote_writer", "a/b", confirm=True)
    assert httpx_mock.get_request().url.raw_path == b"/api/v1/cluster/writers/a%2Fb/demote"


async def test_adopt_on_standalone_raises(httpx_mock, async_mode):
    httpx_mock.add_response(
        method="POST", json={"success": False, "enabled": False, "mode": "standalone"}
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcError):
            await call(client, "adopt_policies", "cq", "n1", "h", confirm=True)


@pytest.mark.parametrize(
    ("status", "exc"), [(403, ArcPermissionError), (404, ArcNotFoundError), (409, ArcError)]
)
async def test_server_refusals_surface(httpx_mock, async_mode, status, exc):
    httpx_mock.add_response(
        method="POST", status_code=status, json={"success": False, "error": "x"}
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(exc):
            await call(client, "abort_compaction_job", "job-9", confirm=True)
