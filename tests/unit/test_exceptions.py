"""Unit tests for Arc client exceptions."""

from __future__ import annotations

from arc_client.exceptions import (
    ArcAuthenticationError,
    ArcConnectionError,
    ArcError,
    ArcIngestionError,
    ArcNotFoundError,
    ArcQueryError,
    ArcRateLimitError,
    ArcServerError,
    ArcValidationError,
)


class TestExceptions:
    """Tests for exception classes."""

    def test_base_exception(self) -> None:
        """Test base ArcError."""
        error = ArcError("test message")
        assert str(error) == "test message"
        assert isinstance(error, Exception)

    def test_connection_error(self) -> None:
        """Test ArcConnectionError."""
        error = ArcConnectionError("connection failed")
        assert isinstance(error, ArcError)

    def test_authentication_error(self) -> None:
        """Test ArcAuthenticationError."""
        error = ArcAuthenticationError("auth failed")
        assert isinstance(error, ArcError)

    def test_query_error(self) -> None:
        """Test ArcQueryError."""
        error = ArcQueryError("query failed")
        assert isinstance(error, ArcError)

    def test_ingestion_error(self) -> None:
        """Test ArcIngestionError."""
        error = ArcIngestionError("ingestion failed")
        assert isinstance(error, ArcError)

    def test_validation_error(self) -> None:
        """Test ArcValidationError."""
        error = ArcValidationError("validation failed")
        assert isinstance(error, ArcError)

    def test_not_found_error(self) -> None:
        """Test ArcNotFoundError."""
        error = ArcNotFoundError("not found")
        assert isinstance(error, ArcError)

    def test_rate_limit_error(self) -> None:
        """Test ArcRateLimitError."""
        error = ArcRateLimitError("rate limited")
        assert isinstance(error, ArcError)

    def test_server_error(self) -> None:
        """Test ArcServerError with status code."""
        error = ArcServerError("server error", status_code=500)
        assert isinstance(error, ArcError)
        assert error.status_code == 500
        assert str(error) == "server error"

    def test_server_error_no_status_code(self) -> None:
        """Test ArcServerError without status code."""
        error = ArcServerError("server error")
        assert error.status_code is None
