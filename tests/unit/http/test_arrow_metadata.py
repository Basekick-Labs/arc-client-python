"""Arrow metadata must survive the real HTTP/1.1 trailer boundary."""

from __future__ import annotations

import asyncio
import contextlib
import select
import socket
import socketserver
import ssl
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from arc_client import ArcClient, AsyncArcClient
from arc_client.exceptions import ArcQueryError

# Synthetic localhost-only TLS fixture; never used outside these tests.
CERT = Path(__file__).parents[2] / "fixtures/localhost-cert.pem"
KEY = Path(__file__).parents[2] / "fixtures/localhost-key.pem"


def arrow_bytes(values=(1, 2), metadata=None):
    table = pa.table({"value": pa.array(values, type=pa.int64())}).replace_schema_metadata(metadata)
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def wire(body, trailers=b"Arc-Rows-Capped: 2\r\nArc-Execution-Time-Ms: 7\r\n", announce=True):
    headers = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/vnd.apache.arrow.stream\r\n"
        b"Transfer-Encoding: chunked\r\n"
    )
    if announce:
        headers += b"Trailer: Arc-Rows-Capped, Arc-Stream-Truncated, Arc-Execution-Time-Ms\r\n"
    return (
        headers + b"\r\n" + f"{len(body):x}\r\n".encode() + body + b"\r\n0\r\n" + trailers + b"\r\n"
    )


@contextlib.contextmanager
def server(replies, *, fragment=False, tls=False, close=False):
    lock = threading.Lock()
    seen = []

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            while True:
                line = self.rfile.readline()
                if not line:
                    return
                headers = {}
                while True:
                    h = self.rfile.readline()
                    if h == b"\r\n":
                        break
                    k, v = h.split(b":", 1)
                    headers[k.lower()] = v.strip()
                self.rfile.read(int(headers.get(b"content-length", b"0")))
                with lock:
                    seen.append((line, self.client_address, headers))
                    reply = replies.pop(0)
                if callable(reply):
                    reply = reply()
                try:
                    if fragment:
                        for start in range(0, len(reply), 3):
                            self.wfile.write(reply[start : start + 3])
                            self.wfile.flush()
                    else:
                        for piece in [reply] if isinstance(reply, bytes) else reply:
                            self.wfile.write(piece)
                            self.wfile.flush()
                    if close:
                        return
                except (BrokenPipeError, ConnectionResetError):
                    return

    class TCP(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    srv = TCP(("127.0.0.1", 0), Handler)
    if tls:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(CERT, KEY)
        srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        yield srv.server_address[1], seen
    finally:
        srv.shutdown()
        srv.server_close()
        thread.join()


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize("fragment", [False, True])
async def test_capped_native_arrow_and_schema_metadata(async_mode, fragment):
    body = arrow_bytes((9223372036854775807, None), {b"application": b"preserved"})
    with server([wire(body)], fragment=fragment) as (port, seen):
        if async_mode:
            async with AsyncArcClient(host="127.0.0.1", port=port, token="synthetic") as c:
                t = await c.query.query_arrow("SELECT value")
        else:
            with ArcClient(host="127.0.0.1", port=port, token="synthetic") as c:
                t = c.query.query_arrow("SELECT value")
        assert isinstance(t, pa.Table)
        assert t.column("value").to_pylist() == [9223372036854775807, None]
        assert t.schema.metadata[b"application"] == b"preserved"
        assert t.schema.metadata[b"arc.rows_capped"] == b"true"
        assert t.schema.metadata[b"arc.row_cap"] == b"2"
        assert t.schema.metadata[b"arc.execution_time_ms"] == b"7"
        assert len(seen) == 1
        assert seen[0][2][b"authorization"] == b"Bearer synthetic"


@pytest.mark.parametrize("async_mode", [False, True])
async def test_keepalive_does_not_leak_cap(async_mode):
    with server(
        [
            wire(arrow_bytes()),
            wire(arrow_bytes(), b"Arc-Rows-Capped: \r\nArc-Execution-Time-Ms: 0\r\n"),
        ]
    ) as (
        port,
        seen,
    ):
        if async_mode:
            async with AsyncArcClient(host="127.0.0.1", port=port) as c:
                a = await c.query.query_arrow("SELECT capped")
                b = await c.query.query_arrow("SELECT uncapped")
        else:
            with ArcClient(host="127.0.0.1", port=port) as c:
                a = c.query.query_arrow("SELECT capped")
                b = c.query.query_arrow("SELECT uncapped")
        assert a.schema.metadata[b"arc.row_cap"] == b"2"
        assert b.schema.metadata[b"arc.rows_capped"] == b"false"
        assert b"arc.row_cap" not in b.schema.metadata
        assert a.schema.metadata[b"arc.row_cap"] == b"2"
        assert seen[0][1] == seen[1][1]


@pytest.mark.parametrize(
    "trailer",
    [
        b"Arc-Rows-Capped: nope\r\n",
        b"Arc-Rows-Capped: -2\r\n",
        b"Arc-Rows-Capped: 0\r\n",
        b"Arc-Rows-Capped: 2\r\nArc-Rows-Capped: 3\r\n",
        b"Arc-Stream-Truncated: cancelled\r\n",
        b"Arc-Execution-Time-Ms: NaN\r\n",
    ],
)
@pytest.mark.parametrize("async_mode", [False, True])
async def test_invalid_or_failed_arrow_never_returns_table(trailer, async_mode):
    with server([wire(arrow_bytes(), trailer)]) as (port, seen):
        if async_mode:
            async with AsyncArcClient(host="127.0.0.1", port=port, max_retries=3) as c:
                with pytest.raises(ArcQueryError):
                    await c.query.query_arrow("SELECT value")
        else:
            with (
                ArcClient(host="127.0.0.1", port=port, max_retries=3) as c,
                pytest.raises(ArcQueryError),
            ):
                c.query.query_arrow("SELECT value")
        assert len(seen) == 1


def test_legacy_without_metadata_does_not_guess_cap():
    body = arrow_bytes(metadata={b"arc.row_cap": b"999", b"application": b"preserved"})
    with (
        server([wire(body, b"", announce=False)]) as (port, _),
        ArcClient(host="127.0.0.1", port=port) as c,
    ):
        t = c.query.query_arrow("SELECT value LIMIT 2")
    assert b"arc.rows_capped" not in (t.schema.metadata or {})
    assert b"arc.row_cap" not in (t.schema.metadata or {})
    assert t.schema.metadata[b"application"] == b"preserved"


@pytest.mark.parametrize("async_mode", [False, True])
async def test_concurrent_metadata_is_per_response(async_mode):
    replies = [
        wire(arrow_bytes(), b"Arc-Rows-Capped: 2\r\n"),
        wire(arrow_bytes((3,)), b"Arc-Rows-Capped: 1\r\n"),
    ]
    with server(replies) as (port, _):
        if async_mode:
            async with AsyncArcClient(host="127.0.0.1", port=port) as c:
                tables = await asyncio.gather(
                    c.query.query_arrow("SELECT a"), c.query.query_arrow("SELECT b")
                )
        else:
            with ArcClient(host="127.0.0.1", port=port) as c, ThreadPoolExecutor(2) as pool:
                tables = list(pool.map(c.query.query_arrow, ["SELECT a", "SELECT b"]))
        for t in tables:
            assert int(t.schema.metadata[b"arc.row_cap"]) == t.num_rows


async def test_async_cancellation_then_query_has_fresh_metadata():
    entered = threading.Event()
    release = threading.Event()

    def blocked():
        entered.set()
        release.wait(5)
        return wire(arrow_bytes(), b"Arc-Rows-Capped: 999\r\n")

    with server([blocked, wire(arrow_bytes((1,)), b"Arc-Rows-Capped: 1\r\n")]) as (port, _):
        async with AsyncArcClient(host="127.0.0.1", port=port) as c:
            task = asyncio.create_task(c.query.query_arrow("SELECT blocked"))
            try:
                assert await asyncio.to_thread(entered.wait, 3)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                t = await c.query.query_arrow("SELECT fresh")
                assert t.schema.metadata[b"arc.row_cap"] == b"1"
            finally:
                release.set()


@pytest.mark.parametrize("async_mode", [False, True])
async def test_unobservable_announced_metadata_fails_closed(httpx_mock, async_mode):
    httpx_mock.add_response(content=arrow_bytes(), headers={"Trailer": "Arc-Rows-Capped"})
    if async_mode:
        async with AsyncArcClient(host="h", max_retries=0) as c:
            with pytest.raises(ArcQueryError, match="could not be captured"):
                await c.query.query_arrow("SELECT value")
    else:
        with (
            ArcClient(host="h", max_retries=0) as c,
            pytest.raises(ArcQueryError, match="could not be captured"),
        ):
            c.query.query_arrow("SELECT value")
    assert len(httpx_mock.get_requests()) == 1


@pytest.mark.parametrize("async_mode", [False, True])
async def test_gateway_promoted_headers(httpx_mock, async_mode):
    httpx_mock.add_response(
        content=arrow_bytes(), headers={"Arc-Rows-Capped": "2", "Arc-Execution-Time-Ms": "0"}
    )
    if async_mode:
        async with AsyncArcClient(host="h") as c:
            t = await c.query.query_arrow("SELECT value")
    else:
        with ArcClient(host="h") as c:
            t = c.query.query_arrow("SELECT value")
    assert t.schema.metadata[b"arc.row_cap"] == b"2"
    assert t.schema.metadata[b"arc.execution_time_ms"] == b"0"


def test_header_trailer_conflict_is_rejected():
    reply = wire(arrow_bytes()).replace(
        b"Transfer-Encoding:", b"Arc-Rows-Capped: 3\r\nTransfer-Encoding:"
    )
    with (
        server([reply]) as (port, _),
        ArcClient(host="127.0.0.1", port=port) as c,
        pytest.raises(ArcQueryError, match="Conflicting"),
    ):
        c.query.query_arrow("SELECT value")


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize("bad_body", [True, False])
async def test_poisoned_or_incomplete_stream_is_rejected(async_mode, bad_body):
    if bad_body:
        body = arrow_bytes()[:-8] + b"\xff\xff\xff\xff\x40\x00\x00\x00\x00"
        reply = wire(body, b"")
    else:
        reply = wire(arrow_bytes())[:-30]
    with server([reply], close=True) as (port, seen):
        if async_mode:
            async with AsyncArcClient(host="127.0.0.1", port=port, max_retries=3) as c:
                with pytest.raises(ArcQueryError):
                    await c.query.query_arrow("SELECT value")
        else:
            with (
                ArcClient(host="127.0.0.1", port=port, max_retries=3) as c,
                pytest.raises(ArcQueryError),
            ):
                c.query.query_arrow("SELECT value")
        assert len(seen) == 1


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize("verify", [True, False])
async def test_tls_validation_is_preserved(async_mode, verify, monkeypatch):
    # Explicitly trust the fixture CA; verify=False must remain opt-out too.
    monkeypatch.setenv("SSL_CERT_FILE", str(CERT))
    with server([wire(arrow_bytes())], tls=True) as (port, _):
        if async_mode:
            async with AsyncArcClient(
                host="127.0.0.1", port=port, ssl=True, verify_ssl=verify
            ) as c:
                t = await c.query.query_arrow("SELECT value")
        else:
            with ArcClient(host="127.0.0.1", port=port, ssl=True, verify_ssl=verify) as c:
                t = c.query.query_arrow("SELECT value")
        assert t.schema.metadata[b"arc.row_cap"] == b"2"


@pytest.mark.parametrize("async_mode", [False, True])
async def test_untrusted_tls_certificate_still_fails(async_mode, monkeypatch):
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    with server([], tls=True) as (port, seen):
        if async_mode:
            async with AsyncArcClient(host="127.0.0.1", port=port, ssl=True, max_retries=0) as c:
                with pytest.raises(ArcQueryError):
                    await c.query.query_arrow("SELECT value")
        else:
            with (
                ArcClient(host="127.0.0.1", port=port, ssl=True, max_retries=0) as c,
                pytest.raises(ArcQueryError),
            ):
                c.query.query_arrow("SELECT value")
        assert not seen


@contextlib.contextmanager
def proxy():
    requests = []

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            request = self.rfile.readline()
            method, target, _ = request.decode().split()
            headers = []
            while True:
                line = self.rfile.readline()
                if line == b"\r\n":
                    break
                headers.append(line)
            requests.append((method, target, headers))
            if method == "CONNECT":
                host, port = target.rsplit(":", 1)
                initial = b""
            else:
                url = urlsplit(target)
                host, port = url.hostname, url.port
                length = next(
                    (
                        int(h.split(b":", 1)[1])
                        for h in headers
                        if h.lower().startswith(b"content-length:")
                    ),
                    0,
                )
                initial = f"{method} {url.path} HTTP/1.1\r\n".encode()
                initial += (
                    b"".join(
                        h for h in headers if not h.lower().startswith(b"proxy-authorization:")
                    )
                    + b"\r\n"
                    + self.rfile.read(length)
                )
            with socket.create_connection((host, int(port)), timeout=5) as upstream:
                if method == "CONNECT":
                    self.wfile.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
                    self.wfile.flush()
                else:
                    upstream.sendall(initial)
                while True:
                    ready, _, _ = select.select([self.connection, upstream], [], [], 5)
                    if not ready:
                        return
                    for source in ready:
                        data = source.recv(65536)
                        if not data:
                            return
                        (upstream if source is self.connection else self.connection).sendall(data)

    class TCP(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    srv = TCP(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        yield srv.server_address[1], requests
    finally:
        srv.shutdown()
        srv.server_close()
        thread.join()


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize("tls", [False, True])
@pytest.mark.parametrize("bypass", [False, True])
async def test_environment_proxy_and_no_proxy(async_mode, tls, bypass, monkeypatch):
    monkeypatch.setenv("SSL_CERT_FILE", str(CERT))
    with server([wire(arrow_bytes())], tls=tls) as (port, _), proxy() as (proxy_port, requests):
        monkeypatch.setenv("HTTP_PROXY", f"http://127.0.0.1:{proxy_port}")
        monkeypatch.setenv("HTTPS_PROXY", f"http://127.0.0.1:{proxy_port}")
        monkeypatch.setenv("NO_PROXY", "127.0.0.1" if bypass else "")
        monkeypatch.delenv("ALL_PROXY", raising=False)
        if async_mode:
            async with AsyncArcClient(host="127.0.0.1", port=port, ssl=tls) as c:
                t = await c.query.query_arrow("SELECT value")
        else:
            with ArcClient(host="127.0.0.1", port=port, ssl=tls) as c:
                t = c.query.query_arrow("SELECT value")
        assert t.schema.metadata[b"arc.row_cap"] == b"2"
        assert len(requests) == (0 if bypass else 1)
        if requests:
            assert requests[0][0] == ("CONNECT" if tls else "POST")


async def test_cancellation_during_captured_body_then_query(monkeypatch):
    from arc_client.http import async_http
    from arc_client.http.arrow_trailers import TRAILERS_EXTENSION

    hooked = asyncio.Event()
    release = threading.Event()
    responses = []
    original_hook = async_http.capture_async_arrow_trailers

    async def hook(response):
        await original_hook(response)
        responses.append(response)
        hooked.set()

    monkeypatch.setattr(async_http, "capture_async_arrow_trailers", hook)

    def slow_body():
        reply = wire(arrow_bytes(), b"Arc-Rows-Capped: 999\r\n")
        split = reply.index(b"\r\n\r\n") + 4
        yield reply[:split]
        release.wait(5)
        yield reply[split:]

    with server([slow_body, wire(arrow_bytes((1,)), b"Arc-Rows-Capped: 1\r\n")]) as (port, _):
        async with AsyncArcClient(host="127.0.0.1", port=port) as c:
            task = asyncio.create_task(c.query.query_arrow("SELECT slow"))
            try:
                await asyncio.wait_for(hooked.wait(), timeout=3)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert not responses[0].extensions[TRAILERS_EXTENSION].complete
                t = await c.query.query_arrow("SELECT fresh")
                assert t.schema.metadata[b"arc.row_cap"] == b"1"
                assert not responses[0].extensions[TRAILERS_EXTENSION].complete
                assert responses[1].extensions[TRAILERS_EXTENSION].complete
            finally:
                release.set()


def test_non_arrow_hook_is_inert(httpx_mock):
    from arc_client.config import ClientConfig
    from arc_client.http.arrow_trailers import TRAILERS_EXTENSION
    from arc_client.http.sync_http import SyncHTTPClient

    httpx_mock.add_response(json={"success": True}, headers={"Trailer": "Arc-Rows-Capped"})
    with SyncHTTPClient(ClientConfig(host="h")) as c:
        response = c.post("/api/v1/query", json={"sql": "SELECT 1"})
    assert TRAILERS_EXTENSION not in response.extensions


@pytest.mark.parametrize("tail", [b"", b"Arc-Rows-Capped: \r\n"])
def test_gateway_dropping_all_completion_values_does_not_mean_uncapped(tail):
    with (
        server([wire(arrow_bytes(), tail)]) as (port, _),
        ArcClient(host="127.0.0.1", port=port) as c,
    ):
        t = c.query.query_arrow("SELECT value LIMIT 2")
    assert b"arc.rows_capped" not in (t.schema.metadata or {})


def test_main_omitted_cap_with_execution_marker_is_uncapped():
    with (
        server([wire(arrow_bytes(), b"Arc-Execution-Time-Ms: 0\r\n")]) as (port, _),
        ArcClient(host="127.0.0.1", port=port) as c,
    ):
        t = c.query.query_arrow("SELECT value")
    assert t.schema.metadata[b"arc.rows_capped"] == b"false"
    assert b"arc.row_cap" not in t.schema.metadata


def test_promoted_timing_header_cannot_prove_uncapped_after_trailer_loss():
    reply = wire(arrow_bytes(), b"").replace(
        b"Transfer-Encoding:", b"Arc-Execution-Time-Ms: 7\r\nTransfer-Encoding:"
    )
    with server([reply]) as (port, _), ArcClient(host="127.0.0.1", port=port) as c:
        t = c.query.query_arrow("SELECT value")
    assert b"arc.rows_capped" not in (t.schema.metadata or {})
    assert t.schema.metadata[b"arc.execution_time_ms"] == b"7"
