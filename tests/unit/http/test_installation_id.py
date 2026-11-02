"""Installation id and User-Agent sent for Arc's client telemetry."""

from __future__ import annotations

import platform
import re
import stat
import sys
from pathlib import Path

import pytest
from pytest_httpx import HTTPXMock

from arc_client import ArcClient, AsyncArcClient, _installation
from arc_client._installation import HEADER_INSTALLATION_ID

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


def _id_file(tmp_path: Path) -> Path:
    if sys.platform.startswith("win"):
        return tmp_path / "appdata" / "arc-client-python" / "installation-id"
    return tmp_path / "xdg" / "arc-client-python" / "installation-id"


async def _request_headers(httpx_mock: HTTPXMock, async_mode: bool, **kwargs: object) -> dict:
    httpx_mock.add_response(json={"status": "ok"})
    if async_mode:
        async with AsyncArcClient(host="h", token="t", max_retries=0, **kwargs) as client:
            await client.health()
    else:
        with ArcClient(host="h", token="t", max_retries=0, **kwargs) as client:
            client.health()
    return dict(httpx_mock.get_requests()[-1].headers)


@pytest.mark.parametrize("async_mode", [False, True])
async def test_sends_persisted_id(httpx_mock: HTTPXMock, async_mode: bool, tmp_path: Path) -> None:
    headers = await _request_headers(httpx_mock, async_mode)

    sent = headers[HEADER_INSTALLATION_ID.lower()]
    assert UUID_RE.match(sent)
    assert _id_file(tmp_path).read_text().strip() == sent


async def test_id_is_stable_across_processes(httpx_mock: HTTPXMock, tmp_path: Path) -> None:
    first = (await _request_headers(httpx_mock, False))[HEADER_INSTALLATION_ID.lower()]
    _installation._reset_for_tests()  # a new process reads the file instead of minting
    second = (await _request_headers(httpx_mock, False))[HEADER_INSTALLATION_ID.lower()]
    assert first == second


@pytest.mark.skipif(sys.platform.startswith("win"), reason="POSIX permissions")
def test_id_file_is_private(tmp_path: Path) -> None:
    _installation.installation_id()
    assert stat.S_IMODE(_id_file(tmp_path).stat().st_mode) == 0o600


def test_corrupt_file_is_replaced(tmp_path: Path) -> None:
    path = _id_file(tmp_path)
    path.parent.mkdir(parents=True)
    path.write_text("not-a-uuid\n")
    new_id = _installation.installation_id()
    assert UUID_RE.match(new_id)
    assert path.read_text().strip() == new_id


def test_env_override_wins(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fixed = "123e4567-e89b-42d3-a456-426614174000"
    monkeypatch.setenv("ARC_CLIENT_INSTALLATION_ID", fixed.upper())
    assert _installation.installation_id() == fixed
    assert not _id_file(tmp_path).exists()


def test_invalid_env_override_is_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ARC_CLIENT_INSTALLATION_ID", "my-container")
    assert UUID_RE.match(_installation.installation_id())


def test_unwritable_config_dir_falls_back_to_memory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    blocker = tmp_path / "file-not-dir"
    blocker.write_text("")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(blocker))
    monkeypatch.setenv("APPDATA", str(blocker))

    first = _installation.installation_id()
    assert UUID_RE.match(first)
    assert _installation.installation_id() == first  # stable for the process


@pytest.mark.parametrize(
    ("kwargs", "env"),
    [
        ({"send_installation_id": False}, {}),
        ({}, {"DO_NOT_TRACK": "1"}),
        ({}, {"DO_NOT_TRACK": "true"}),
        ({}, {"ARC_CLIENT_SEND_INSTALLATION_ID": "0"}),
        ({}, {"ARC_CLIENT_SEND_INSTALLATION_ID": "false"}),
    ],
)
@pytest.mark.parametrize("async_mode", [False, True])
async def test_opt_outs_send_nothing_and_write_nothing(
    httpx_mock: HTTPXMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    async_mode: bool,
    kwargs: dict,
    env: dict,
) -> None:
    for name, value in env.items():
        monkeypatch.setenv(name, value)

    headers = await _request_headers(httpx_mock, async_mode, **kwargs)

    assert HEADER_INSTALLATION_ID.lower() not in headers
    assert not _id_file(tmp_path).exists()
    assert headers["user-agent"].startswith("arc-client-python/")


def test_do_not_track_zero_does_not_opt_out() -> None:
    assert _installation.sends_installation_id(True)


def test_user_agent_shape() -> None:
    import arc_client

    ua = _installation.user_agent()
    assert ua.startswith(f"arc-client-python/{arc_client.__version__} (python/")
    assert f"python/{platform.python_version()};" in ua
    m = re.match(r"^arc-client-python/\S+ \(python/(\S+); (\S+)/(\S+)\)$", ua)
    assert m, ua
    # Arc keeps only printable ASCII up to 32 chars per field.
    for part in m.groups():
        assert 0 < len(part) <= 32 and part.isascii() and part.isprintable()


def test_unresolvable_home_never_fails_a_request(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("XDG_CONFIG_HOME")
    monkeypatch.delenv("APPDATA")

    def no_home() -> Path:
        raise RuntimeError("Could not determine home directory.")

    monkeypatch.setattr(Path, "home", staticmethod(no_home))
    assert UUID_RE.match(_installation.installation_id())
