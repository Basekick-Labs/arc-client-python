"""Bounded HTTPX/httpcore HTTP/1.1 compatibility adapter for Arrow trailers.

HTTPX 0.27/0.28 and httpcore 1.0.9 discard h11 EndOfMessage headers. The
response hook runs before body consumption, even if the body is already in
the parser's receive buffer. Only the owning connection's parser is observed;
TLS, proxies, timeouts, framing and pool ownership remain with HTTPX/httpcore.
An unsupported stream shape is detected by the Arrow decoder, never guessed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, cast

import h11
import httpcore
import httpx

TRAILERS_EXTENSION = "arc.arrow_trailers"


@dataclass
class ArrowTrailers:
    headers: list[tuple[bytes, bytes]] = field(default_factory=list)
    complete: bool = False


class _ObservedState:
    """Delegate protocol state unchanged, retaining only the current sink."""

    def __init__(self, state: Any) -> None:
        self._state = state
        self.sink: Optional[ArrowTrailers] = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._state, name)

    def next_event(self) -> Any:
        event = self._state.next_event()
        if isinstance(event, h11.EndOfMessage):
            sink, self.sink = self.sink, None
            if sink is not None:
                sink.headers = list(event.headers.raw_items())
                sink.complete = True
        return event

    def start_next_cycle(self) -> None:
        self.sink = None
        self._state.start_next_cycle()


def capture_arrow_trailers(response: httpx.Response) -> None:
    """Attach a fresh per-response sink to the supported HTTP/1.1 parser."""
    if (
        response.request.method != "POST"
        or response.request.url.path.rstrip("/") != "/api/v1/query/arrow"
    ):
        return
    sink = ArrowTrailers()
    response.extensions[TRAILERS_EXTENSION] = sink
    # Bound{Sync,Async}Stream -> HTTPX response stream -> PoolByteStream ->
    # HTTP11ConnectionByteStream -> owning HTTP11 connection. Deliberately
    # bounded to the dependency versions declared in pyproject.toml.
    owner: Any = response.stream
    for attribute in ("_stream", "_httpcore_stream", "_stream", "_connection"):
        owner = getattr(owner, attribute, None)
        if owner is None:
            return
    if not isinstance(owner, (httpcore.HTTP11Connection, httpcore.AsyncHTTP11Connection)):
        return
    state: Any = owner._h11_state
    if isinstance(state, _ObservedState):
        observer = state
    elif isinstance(state, h11.Connection):
        observer = _ObservedState(state)
        cast(Any, owner)._h11_state = observer
    else:
        return
    observer.sink = sink


async def capture_async_arrow_trailers(response: httpx.Response) -> None:
    """Async HTTPX hook; observing parser events itself performs no I/O."""
    capture_arrow_trailers(response)
