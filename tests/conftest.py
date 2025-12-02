"""Pytest configuration and fixtures for Arc client tests."""

from __future__ import annotations

import os

import pytest

from arc_client import ArcClient, AsyncArcClient


@pytest.fixture
def arc_server_url() -> str:
    """Arc server URL from environment or default."""
    return os.environ.get("ARC_TEST_URL", "http://localhost:8000")


@pytest.fixture
def arc_token() -> str | None:
    """Test token from environment."""
    return os.environ.get("ARC_TEST_TOKEN")


@pytest.fixture
def arc_host(arc_server_url: str) -> str:
    """Extract host from server URL."""
    # Parse http://localhost:8000 -> localhost
    return arc_server_url.split("://")[1].split(":")[0]


@pytest.fixture
def arc_port(arc_server_url: str) -> int:
    """Extract port from server URL."""
    # Parse http://localhost:8000 -> 8000
    return int(arc_server_url.split(":")[-1])


@pytest.fixture
def sync_client(arc_host: str, arc_port: int, arc_token: str | None) -> ArcClient:
    """Synchronous Arc client for testing."""
    with ArcClient(
        host=arc_host,
        port=arc_port,
        token=arc_token,
    ) as client:
        yield client


@pytest.fixture
async def async_client(arc_host: str, arc_port: int, arc_token: str | None) -> AsyncArcClient:
    """Asynchronous Arc client for testing."""
    async with AsyncArcClient(
        host=arc_host,
        port=arc_port,
        token=arc_token,
    ) as client:
        yield client


@pytest.fixture
def sample_columnar_data() -> dict:
    """Sample columnar data for testing."""
    import time

    base_ts = int(time.time() * 1_000_000)
    return {
        "measurement": "test_cpu",
        "columns": {
            "time": [base_ts, base_ts + 1_000_000, base_ts + 2_000_000],
            "host": ["server01", "server01", "server02"],
            "region": ["us-east", "us-east", "us-west"],
            "usage_idle": [95.0, 94.5, 92.1],
            "usage_user": [3.2, 3.8, 5.4],
        },
    }
