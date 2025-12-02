"""Unit tests for Arc client."""

from __future__ import annotations

import pytest

from arc_client import ArcClient, AsyncArcClient, ClientConfig


class TestClientConfig:
    """Tests for ClientConfig."""

    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = ClientConfig()
        assert config.host == "localhost"
        assert config.port == 8000
        assert config.token is None
        assert config.database == "default"
        assert config.timeout == 30.0
        assert config.compression is True
        assert config.ssl is False
        assert config.verify_ssl is True

    def test_base_url_http(self) -> None:
        """Test base URL generation for HTTP."""
        config = ClientConfig(host="example.com", port=9000)
        assert config.base_url == "http://example.com:9000"

    def test_base_url_https(self) -> None:
        """Test base URL generation for HTTPS."""
        config = ClientConfig(host="example.com", port=443, ssl=True)
        assert config.base_url == "https://example.com:443"

    def test_config_immutable(self) -> None:
        """Test that config is immutable."""
        from pydantic import ValidationError

        config = ClientConfig()
        with pytest.raises(ValidationError):
            config.host = "other"  # type: ignore


class TestArcClient:
    """Tests for ArcClient."""

    def test_client_creation(self) -> None:
        """Test client creation with default values."""
        client = ArcClient()
        assert client.config.host == "localhost"
        assert client.config.port == 8000
        client.close()

    def test_client_creation_with_params(self) -> None:
        """Test client creation with custom parameters."""
        client = ArcClient(
            host="example.com",
            port=9000,
            token="test-token",
            database="mydb",
        )
        assert client.config.host == "example.com"
        assert client.config.port == 9000
        assert client.config.token == "test-token"
        assert client.config.database == "mydb"
        client.close()

    def test_client_context_manager(self) -> None:
        """Test client as context manager."""
        with ArcClient() as client:
            assert client is not None
        # Client should be closed after context exits

    def test_client_repr(self) -> None:
        """Test client string representation."""
        client = ArcClient(host="example.com", port=9000)
        assert repr(client) == "ArcClient(host='example.com', port=9000)"
        client.close()


class TestAsyncArcClient:
    """Tests for AsyncArcClient."""

    def test_async_client_creation(self) -> None:
        """Test async client creation."""
        client = AsyncArcClient()
        assert client.config.host == "localhost"
        assert client.config.port == 8000

    def test_async_client_repr(self) -> None:
        """Test async client string representation."""
        client = AsyncArcClient(host="example.com", port=9000)
        assert repr(client) == "AsyncArcClient(host='example.com', port=9000)"
