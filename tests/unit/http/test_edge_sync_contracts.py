"""Edge-sync administration wire contracts.

Arc sources: internal/api/edgesync_admin.go (hub spokes),
edgesync_import.go (hub bundle import), edgesync_spoke.go (spoke controls).
"""

from __future__ import annotations

import inspect
import json
from contextlib import AsyncExitStack
from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcClient, AsyncArcClient
from arc_client.exceptions import (
    ArcError,
    ArcNotFoundError,
    ArcPermissionError,
    ArcServerError,
    ArcValidationError,
)
from arc_client.models.edge_sync import Spoke, SpokeSecret

ROOT = "http://localhost:8000"
SPOKES = f"{ROOT}/api/v1/sync-spokes"
IMPORT = f"{ROOT}/api/v1/bundle-import"
SPOKE_SYNC = f"{ROOT}/api/v1/spoke-sync"
SPOKE = {
    "spoke_id": "rocket-01",
    "name": "Rocket",
    "enabled": True,
    "files_received": 4,
    "bytes_received": 2048,
    "registered_at": "2027-01-05T10:00:00Z",
    "last_seen_at": "2027-01-06T10:00:00Z",
}


async def call(client: Any, method: str, *args: Any, **kwargs: Any) -> Any:
    result = getattr(client.edge_sync, method)(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def async_mode(request: Any) -> bool:
    return bool(request.param)


async def enter_client(stack: AsyncExitStack, async_mode: bool) -> Any:
    if async_mode:
        return await stack.enter_async_context(AsyncArcClient(token="t", max_retries=0))
    return stack.enter_context(ArcClient(token="t", max_retries=0))


def body_of(httpx_mock: HTTPXMock) -> Any:
    content = httpx_mock.get_request().content
    return json.loads(content) if content else None


# Hub: spokes -----------------------------------------------------------------


async def test_list_spokes(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    second = {k: v for k, v in SPOKE.items() if k != "last_seen_at"}
    second["spoke_id"] = "rocket-02"
    httpx_mock.add_response(method="GET", url=f"{SPOKES}/", json={"spokes": [SPOKE, second]})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        spokes = await call(client, "list_spokes")
    assert [s.spoke_id for s in spokes] == ["rocket-01", "rocket-02"]
    assert all(isinstance(s, Spoke) for s in spokes)
    assert spokes[0].last_seen_at is not None
    assert spokes[1].last_seen_at is None


@pytest.mark.parametrize("body", [{}, {"spokes": None}, []])
async def test_list_spokes_malformed(httpx_mock: HTTPXMock, async_mode: bool, body: Any) -> None:
    httpx_mock.add_response(method="GET", url=f"{SPOKES}/", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcError):
            await call(client, "list_spokes")


async def test_get_spoke(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(method="GET", url=f"{SPOKES}/rocket-01", json=SPOKE)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        spoke = await call(client, "get_spoke", "rocket-01")
    assert spoke.files_received == 4


async def test_get_spoke_not_found(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(status_code=404, json={"error": "spoke not registered"})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcNotFoundError):
            await call(client, "get_spoke", "nope")


async def test_register_spoke_returns_secret(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{SPOKES}/",
        status_code=201,
        json={"spoke_id": "rocket-01", "name": "Rocket", "secret": "s3cr3t", "warning": "once"},
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        result = await call(client, "register_spoke", "rocket-01", name="Rocket")
    assert isinstance(result, SpokeSecret)
    assert result.secret == "s3cr3t"
    assert body_of(httpx_mock) == {"spoke_id": "rocket-01", "name": "Rocket"}


async def test_register_conflict(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(status_code=409, json={"error": "spoke already registered"})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcServerError) as info:
            await call(client, "register_spoke", "rocket-01")
    assert info.value.status_code == 409


@pytest.mark.parametrize(
    ("method", "args", "kwargs"),
    [
        ("rotate_spoke_secret", ("rocket-01",), {}),
        ("delete_spoke", ("rocket-01",), {}),
        ("dismiss_failed", (), {"path": "db/cpu/a.parquet"}),
        ("dismiss_failed", (), {"all_failed": True}),
    ],
)
async def test_confirm_required(
    httpx_mock: HTTPXMock,
    async_mode: bool,
    method: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> None:
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, method, *args, **kwargs)
    assert httpx_mock.get_requests() == []


async def test_rotate_spoke_secret(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{SPOKES}/rocket-01/rotate",
        json={"spoke_id": "rocket-01", "secret": "new", "warning": "old secret is dead"},
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        result = await call(client, "rotate_spoke_secret", "rocket-01", confirm=True)
    assert result.secret == "new"


@pytest.mark.parametrize(("method", "enabled"), [("enable_spoke", True), ("disable_spoke", False)])
async def test_enable_disable(
    httpx_mock: HTTPXMock, async_mode: bool, method: str, enabled: bool
) -> None:
    verb = "enable" if enabled else "disable"
    body = {"spoke_id": "rocket-01", "enabled": enabled}
    httpx_mock.add_response(method="POST", url=f"{SPOKES}/rocket-01/{verb}", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, method, "rocket-01") == body


async def test_delete_spoke(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"spoke_id": "rocket-01", "deleted": True, "note": "files retained"}
    httpx_mock.add_response(method="DELETE", url=f"{SPOKES}/rocket-01", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "delete_spoke", "rocket-01", confirm=True) == body


async def test_spoke_id_is_path_escaped(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(method="GET", json=SPOKE)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        await call(client, "get_spoke", "a/b")
    assert httpx_mock.get_request().url.raw_path == b"/api/v1/sync-spokes/a%2Fb"


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("get_spoke", ("",)),
        ("register_spoke", ("",)),
        ("enable_spoke", ("",)),
        ("import_history", ("",)),
        ("revert_export", ("",)),
        ("import_bundle", ("",)),
        ("apply_ack", ("",)),
        ("export_bundle", ("",)),
    ],
)
async def test_required_arguments(
    httpx_mock: HTTPXMock, async_mode: bool, method: str, args: tuple[Any, ...]
) -> None:
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, method, *args)
    assert httpx_mock.get_requests() == []


@pytest.mark.parametrize(
    "method", ["list_spokes", "import_bundle", "spoke_status", "run_sync", "spoke_ledger"]
)
async def test_non_admin_refused(httpx_mock: HTTPXMock, async_mode: bool, method: str) -> None:
    httpx_mock.add_response(status_code=403, json={"error": "Admin permission required"})
    args = ("/mnt/drive",) if method == "import_bundle" else ()
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcPermissionError):
            await call(client, method, *args)


# Hub: bundle import ----------------------------------------------------------


async def test_import_bundle(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"imported": True, "bundle_id": "b1", "spoke_id": "rocket-01", "conflicts": []}
    httpx_mock.add_response(method="POST", url=f"{IMPORT}/", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "import_bundle", "/mnt/drive/b1") == body
    assert body_of(httpx_mock) == {"path": "/mnt/drive/b1"}


@pytest.mark.parametrize(
    ("status", "body"),
    [
        (409, {"imported": False, "reason": "already imported", "detail": "on 2027-01-01"}),
        (409, {"imported": False, "reason": "import in progress", "detail": "retry later"}),
        (422, {"imported": False, "error": "bundle checksum mismatch"}),
    ],
)
async def test_import_bundle_refusals_raise(
    httpx_mock: HTTPXMock, async_mode: bool, status: int, body: dict[str, Any]
) -> None:
    httpx_mock.add_response(method="POST", status_code=status, json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcServerError) as info:
            await call(client, "import_bundle", "/mnt/drive/b1")
    assert info.value.status_code == status
    assert info.value.response_body == body


async def test_import_history(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"spoke_id": "rocket-01", "bundles": [], "limit": 5}
    httpx_mock.add_response(method="GET", url=f"{IMPORT}/history/rocket-01?limit=5", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "import_history", "rocket-01", limit=5) == body


@pytest.mark.parametrize("limit", [0, 1001, True])
async def test_import_history_limit(httpx_mock: HTTPXMock, async_mode: bool, limit: Any) -> None:
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, "import_history", "rocket-01", limit=limit)


# Spoke: sync and ledger -----------------------------------------------------


async def test_spoke_status(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"hub_id": "hub", "pending": 3, "failed": 1, "seconds_since_last_sync": 60}
    httpx_mock.add_response(method="GET", url=f"{SPOKE_SYNC}/status", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "spoke_status") == body


async def test_spoke_ledger(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"entries": [{"path": "db/cpu/a.parquet", "state": "skipped"}], "limit": 10}
    httpx_mock.add_response(
        method="GET", url=f"{SPOKE_SYNC}/ledger?limit=10&state=skipped", json=body
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "spoke_ledger", limit=10, state="skipped") == body


async def test_spoke_ledger_default(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(method="GET", url=f"{SPOKE_SYNC}/ledger", json={"entries": []})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        await call(client, "spoke_ledger")


@pytest.mark.parametrize("kwargs", [{"state": "done"}, {"limit": 0}, {"limit": 1001}])
async def test_spoke_ledger_validation(
    httpx_mock: HTTPXMock, async_mode: bool, kwargs: dict[str, Any]
) -> None:
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, "spoke_ledger", **kwargs)


async def test_run_sync(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"discovered": 3, "sent": 3, "conflicts": []}
    httpx_mock.add_response(method="POST", url=f"{SPOKE_SYNC}/run", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "run_sync") == body


@pytest.mark.parametrize("status", [409, 503])
async def test_run_sync_refused(httpx_mock: HTTPXMock, async_mode: bool, status: int) -> None:
    httpx_mock.add_response(method="POST", status_code=status, json={"error": "nope"})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcServerError) as info:
            await call(client, "run_sync")
    assert info.value.status_code == status
    assert len(httpx_mock.get_requests()) == 1


@pytest.mark.parametrize(
    ("kwargs", "wire"),
    [
        ({"path": "db/cpu/a.parquet"}, {"path": "db/cpu/a.parquet"}),
        ({"all_failed": True}, {"all": True}),
    ],
)
async def test_requeue_failed(
    httpx_mock: HTTPXMock, async_mode: bool, kwargs: dict[str, Any], wire: dict[str, Any]
) -> None:
    httpx_mock.add_response(method="POST", url=f"{SPOKE_SYNC}/ledger/requeue", json={"requeued": 2})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "requeue_failed", **kwargs) == {"requeued": 2}
    assert body_of(httpx_mock) == wire


async def test_dismiss_failed(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(
        method="POST", url=f"{SPOKE_SYNC}/ledger/dismiss", json={"dismissed": 5}
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        result = await call(client, "dismiss_failed", all_failed=True, confirm=True)
    assert result == {"dismissed": 5}
    assert body_of(httpx_mock) == {"all": True}


@pytest.mark.parametrize("method", ["requeue_failed", "dismiss_failed"])
@pytest.mark.parametrize("kwargs", [{}, {"path": "x", "all_failed": True}])
async def test_selector_exactly_one(
    httpx_mock: HTTPXMock, async_mode: bool, method: str, kwargs: dict[str, Any]
) -> None:
    if method == "dismiss_failed":
        kwargs = {**kwargs, "confirm": True}
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, method, **kwargs)
    assert httpx_mock.get_requests() == []


async def test_requeue_path_miss(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(status_code=404, json={"error": "no failed entry at that path"})
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcNotFoundError):
            await call(client, "requeue_failed", path="missing")


# Spoke: air-gap export ------------------------------------------------------


async def test_export_bundle(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"exported": True, "bundle_id": "b1", "files": 2}
    httpx_mock.add_response(method="POST", url=f"{SPOKE_SYNC}/export", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "export_bundle", "/mnt/usb", limit=50) == body
    assert body_of(httpx_mock) == {"path": "/mnt/usb", "limit": 50}


async def test_export_nothing_is_not_an_error(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"exported": False, "reason": "nothing to export"}
    httpx_mock.add_response(method="POST", url=f"{SPOKE_SYNC}/export", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "export_bundle", "/mnt/usb") == body
    assert body_of(httpx_mock) == {"path": "/mnt/usb"}


@pytest.mark.parametrize("limit", [0, -1, False])
async def test_export_limit_validation(httpx_mock: HTTPXMock, async_mode: bool, limit: Any) -> None:
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcValidationError):
            await call(client, "export_bundle", "/mnt/usb", limit=limit)


async def test_revert_export(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"bundle_id": "b1", "reverted": 7}
    httpx_mock.add_response(method="POST", url=f"{SPOKE_SYNC}/export/b1/revert", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "revert_export", "b1") == body


async def test_apply_ack(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    body = {"applied": True, "bundle_id": "b1", "synced": 7, "conflicts": []}
    httpx_mock.add_response(method="POST", url=f"{SPOKE_SYNC}/ack", json=body)
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        assert await call(client, "apply_ack", "/mnt/usb/b1") == body
    assert body_of(httpx_mock) == {"path": "/mnt/usb/b1"}


async def test_export_disabled_503(httpx_mock: HTTPXMock, async_mode: bool) -> None:
    httpx_mock.add_response(
        method="POST",
        status_code=503,
        json={"error": "air-gap bundle export is not enabled (edge_sync.spoke.bundle.enabled)"},
    )
    async with AsyncExitStack() as stack:
        client = await enter_client(stack, async_mode)
        with pytest.raises(ArcServerError) as info:
            await call(client, "apply_ack", "/mnt/usb/b1")
    assert info.value.status_code == 503
