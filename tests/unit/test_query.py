"""Unit tests for query module."""

from __future__ import annotations

import pytest

from arc_client.exceptions import ArcValidationError
from arc_client.models.query import EstimateResponse, MeasurementInfo, QueryResponse


class TestQueryResponse:
    """Tests for QueryResponse model."""

    def test_query_response_success(self) -> None:
        """Test successful query response parsing."""
        data = {
            "success": True,
            "columns": ["time", "host", "usage"],
            "data": [
                [1633024800000000, "server01", 45.2],
                [1633024801000000, "server01", 47.8],
            ],
            "row_count": 2,
            "execution_time_ms": 12.5,
            "timestamp": "2024-01-01T00:00:00Z",
        }
        response = QueryResponse(**data)

        assert response.success is True
        assert response.columns == ["time", "host", "usage"]
        assert len(response.data) == 2
        assert response.row_count == 2
        assert response.execution_time_ms == 12.5
        assert response.timestamp == "2024-01-01T00:00:00Z"
        assert response.error is None

    def test_query_response_error(self) -> None:
        """Test error query response parsing."""
        data = {
            "success": False,
            "error": "Query execution failed",
            "timestamp": "2024-01-01T00:00:00Z",
        }
        response = QueryResponse(**data)

        assert response.success is False
        assert response.error == "Query execution failed"
        assert response.columns == []
        assert response.data == []
        assert response.row_count == 0

    def test_query_response_defaults(self) -> None:
        """Test QueryResponse default values."""
        response = QueryResponse(success=True)

        assert response.success is True
        assert response.columns == []
        assert response.data == []
        assert response.row_count == 0
        assert response.execution_time_ms == 0.0
        assert response.timestamp is None
        assert response.error is None


class TestEstimateResponse:
    """Tests for EstimateResponse model."""

    def test_estimate_response_success(self) -> None:
        """Test successful estimate response."""
        data = {
            "success": True,
            "estimated_rows": 1500000,
            "warning_level": "high",
            "warning_message": "Large query: 1,500,000 rows",
            "execution_time_ms": 5.2,
        }
        response = EstimateResponse(**data)

        assert response.success is True
        assert response.estimated_rows == 1500000
        assert response.warning_level == "high"
        assert response.warning_message == "Large query: 1,500,000 rows"
        assert response.execution_time_ms == 5.2

    def test_estimate_response_defaults(self) -> None:
        """Test EstimateResponse default values."""
        response = EstimateResponse(success=True)

        assert response.success is True
        assert response.estimated_rows is None
        assert response.warning_level == "none"
        assert response.warning_message is None
        assert response.execution_time_ms == 0.0
        assert response.error is None


class TestMeasurementInfo:
    """Tests for MeasurementInfo model."""

    def test_measurement_info(self) -> None:
        """Test MeasurementInfo model."""
        data = {
            "database": "default",
            "measurement": "cpu",
            "file_count": 15,
            "total_size_mb": 256.5,
            "storage_path": "./data/default/cpu/**/*.parquet",
        }
        info = MeasurementInfo(**data)

        assert info.database == "default"
        assert info.measurement == "cpu"
        assert info.file_count == 15
        assert info.total_size_mb == 256.5
        assert info.storage_path == "./data/default/cpu/**/*.parquet"

    def test_measurement_info_defaults(self) -> None:
        """Test MeasurementInfo default values."""
        info = MeasurementInfo(database="default", measurement="cpu")

        assert info.database == "default"
        assert info.measurement == "cpu"
        assert info.file_count == 0
        assert info.total_size_mb == 0.0
        assert info.storage_path is None


class TestQueryClientValidation:
    """Tests for QueryClient input validation."""

    def test_empty_sql_raises_error(self) -> None:
        """Test that empty SQL raises validation error."""
        from unittest.mock import MagicMock

        from arc_client.query.executor import QueryClient

        # Create a mock HTTP client and config
        http = MagicMock()
        config = MagicMock()

        client = QueryClient(http, config)

        with pytest.raises(ArcValidationError, match="SQL query cannot be empty"):
            client.query("")

        with pytest.raises(ArcValidationError, match="SQL query cannot be empty"):
            client.query("   ")

    def test_empty_sql_estimate_raises_error(self) -> None:
        """Test that empty SQL in estimate raises validation error."""
        from unittest.mock import MagicMock

        from arc_client.query.executor import QueryClient

        http = MagicMock()
        config = MagicMock()

        client = QueryClient(http, config)

        with pytest.raises(ArcValidationError, match="SQL query cannot be empty"):
            client.estimate("")

    def test_empty_sql_arrow_raises_error(self) -> None:
        """Test that empty SQL in query_arrow raises validation error."""
        from unittest.mock import MagicMock

        from arc_client.query.executor import QueryClient

        http = MagicMock()
        config = MagicMock()

        client = QueryClient(http, config)

        with pytest.raises(ArcValidationError, match="SQL query cannot be empty"):
            client.query_arrow("")


class TestAsyncQueryClientValidation:
    """Tests for AsyncQueryClient input validation."""

    @pytest.mark.asyncio
    async def test_empty_sql_raises_error(self) -> None:
        """Test that empty SQL raises validation error."""
        from unittest.mock import MagicMock

        from arc_client.query.async_executor import AsyncQueryClient

        http = MagicMock()
        config = MagicMock()

        client = AsyncQueryClient(http, config)

        with pytest.raises(ArcValidationError, match="SQL query cannot be empty"):
            await client.query("")

        with pytest.raises(ArcValidationError, match="SQL query cannot be empty"):
            await client.query("   ")

    @pytest.mark.asyncio
    async def test_empty_sql_estimate_raises_error(self) -> None:
        """Test that empty SQL in estimate raises validation error."""
        from unittest.mock import MagicMock

        from arc_client.query.async_executor import AsyncQueryClient

        http = MagicMock()
        config = MagicMock()

        client = AsyncQueryClient(http, config)

        with pytest.raises(ArcValidationError, match="SQL query cannot be empty"):
            await client.estimate("")

    @pytest.mark.asyncio
    async def test_empty_sql_arrow_raises_error(self) -> None:
        """Test that empty SQL in query_arrow raises validation error."""
        from unittest.mock import MagicMock

        from arc_client.query.async_executor import AsyncQueryClient

        http = MagicMock()
        config = MagicMock()

        client = AsyncQueryClient(http, config)

        with pytest.raises(ArcValidationError, match="SQL query cannot be empty"):
            await client.query_arrow("")
