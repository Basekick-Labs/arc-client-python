"""Shared helpers for the sync/async parametrized HTTP contract tests."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from arc_client import ArcClient, AsyncArcClient

MODES = pytest.mark.parametrize("async_mode", [False, True], ids=["sync", "async"])


@asynccontextmanager
async def make_client(async_mode: bool, **kwargs: Any) -> AsyncIterator[Any]:
    """Yield an ArcClient or AsyncArcClient with instant retry backoff."""
    kwargs.setdefault("host", "h")
    kwargs.setdefault("token", "t")
    kwargs.setdefault("retry_backoff", 0)
    if async_mode:
        async with AsyncArcClient(**kwargs) as client:
            yield client
    else:
        with ArcClient(**kwargs) as client:
            yield client


async def call(result: Any) -> Any:
    """Await the result of an async client call; pass sync results through."""
    if inspect.isawaitable(result):
        return await result
    return result
