"""Unit tests for management modules (delete, retention, continuous query)."""

from __future__ import annotations

import pytest

from arc_client.exceptions import ArcValidationError
from arc_client.models.continuous_query import (
    ContinuousQuery,
    CQExecution,
    ExecuteCQResponse,
)
from arc_client.models.delete import DeleteConfigResponse, DeleteResponse
from arc_client.models.retention import (
    ExecuteRetentionResponse,
    RetentionExecution,
    RetentionPolicy,
)


class TestDeleteResponse:
    """Tests for DeleteResponse model."""

    def test_delete_response_success(self) -> None:
        """Test successful delete response."""
        data = {
            "success": True,
            "deleted_count": 1500,
            "affected_files": 3,
            "rewritten_files": 2,
            "dry_run": False,
            "files_processed": ["file1.parquet", "file2.parquet", "file3.parquet"],
        }
        response = DeleteResponse(**data)

        assert response.success is True
        assert response.deleted_count == 1500
        assert response.affected_files == 3
        assert response.rewritten_files == 2
        assert response.dry_run is False
        assert len(response.files_processed) == 3

    def test_delete_response_dry_run(self) -> None:
        """Test dry run delete response."""
        data = {
            "success": True,
            "deleted_count": 500,
            "affected_files": 1,
            "dry_run": True,
        }
        response = DeleteResponse(**data)

        assert response.success is True
        assert response.dry_run is True
        assert response.deleted_count == 500

    def test_delete_response_defaults(self) -> None:
        """Test DeleteResponse default values."""
        response = DeleteResponse(success=True)

        assert response.success is True
        assert response.deleted_count == 0
        assert response.affected_files == 0
        assert response.rewritten_files == 0
        assert response.dry_run is False
        assert response.files_processed == []


class TestDeleteConfigResponse:
    """Tests for DeleteConfigResponse model."""

    def test_delete_config_response(self) -> None:
        """Test delete config response."""
        data = {
            "enabled": True,
            "confirmation_threshold": 10000,
            "max_rows_per_delete": 1000000,
            "implementation": "rewrite-based",
        }
        response = DeleteConfigResponse(**data)

        assert response.enabled is True
        assert response.confirmation_threshold == 10000
        assert response.max_rows_per_delete == 1000000
        assert response.implementation == "rewrite-based"


class TestRetentionPolicy:
    """Tests for RetentionPolicy model."""

    def test_retention_policy_full(self) -> None:
        """Test RetentionPolicy with all fields."""
        data = {
            "id": 1,
            "name": "30-day-retention",
            "database": "default",
            "measurement": "cpu",
            "retention_days": 30,
            "buffer_days": 7,
            "is_active": True,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
        }
        policy = RetentionPolicy(**data)

        assert policy.id == 1
        assert policy.name == "30-day-retention"
        assert policy.database == "default"
        assert policy.measurement == "cpu"
        assert policy.retention_days == 30
        assert policy.buffer_days == 7
        assert policy.is_active is True

    def test_retention_policy_minimal(self) -> None:
        """Test RetentionPolicy with minimal fields."""
        policy = RetentionPolicy(id=1, name="test", database="default", retention_days=30)

        assert policy.id == 1
        assert policy.name == "test"
        assert policy.database == "default"
        assert policy.retention_days == 30
        assert policy.measurement is None
        assert policy.buffer_days == 7
        assert policy.is_active is True


class TestExecuteRetentionResponse:
    """Tests for ExecuteRetentionResponse model."""

    def test_execute_retention_response_success(self) -> None:
        """Test successful retention execution response."""
        data = {
            "policy_id": 1,
            "policy_name": "30-day-retention",
            "deleted_count": 10000,
            "files_deleted": 5,
            "execution_time_ms": 1500.5,
            "dry_run": False,
            "cutoff_date": "2024-01-01T00:00:00Z",
        }
        response = ExecuteRetentionResponse(**data)

        assert response.policy_id == 1
        assert response.policy_name == "30-day-retention"
        assert response.deleted_count == 10000
        assert response.files_deleted == 5
        assert response.dry_run is False

    def test_execute_retention_response_dry_run(self) -> None:
        """Test dry run retention execution response."""
        response = ExecuteRetentionResponse(
            policy_id=1, policy_name="test", dry_run=True, deleted_count=5000
        )

        assert response.policy_id == 1
        assert response.dry_run is True
        assert response.deleted_count == 5000


class TestRetentionExecution:
    """Tests for RetentionExecution model."""

    def test_retention_execution(self) -> None:
        """Test RetentionExecution model."""
        data = {
            "id": 123,
            "policy_id": 1,
            "execution_time": "2024-01-01T00:00:00Z",
            "status": "completed",
            "deleted_count": 5000,
            "cutoff_date": "2023-12-01T00:00:00Z",
            "execution_duration_ms": 1500.5,
            "error_message": None,
        }
        execution = RetentionExecution(**data)

        assert execution.id == 123
        assert execution.policy_id == 1
        assert execution.status == "completed"
        assert execution.deleted_count == 5000
        assert execution.error_message is None


class TestContinuousQuery:
    """Tests for ContinuousQuery model."""

    def test_continuous_query_full(self) -> None:
        """Test ContinuousQuery with all fields."""
        data = {
            "id": 1,
            "name": "downsample-cpu",
            "database": "default",
            "source_measurement": "cpu",
            "destination_measurement": "cpu_1h",
            "query": "SELECT time_bucket('1h', time) as time, avg(usage) FROM cpu GROUP BY 1",
            "interval": "1h",
            "description": "Downsample CPU data to 1 hour",
            "retention_days": 365,
            "delete_source_after_days": 7,
            "is_active": True,
            "created_at": "2024-01-01T00:00:00Z",
            "last_run_at": "2024-01-02T00:00:00Z",
        }
        cq = ContinuousQuery(**data)

        assert cq.id == 1
        assert cq.name == "downsample-cpu"
        assert cq.database == "default"
        assert cq.source_measurement == "cpu"
        assert cq.destination_measurement == "cpu_1h"
        assert cq.interval == "1h"
        assert cq.is_active is True
        assert cq.retention_days == 365
        assert cq.delete_source_after_days == 7

    def test_continuous_query_minimal(self) -> None:
        """Test ContinuousQuery with minimal fields."""
        cq = ContinuousQuery(
            id=1,
            name="test-cq",
            database="default",
            source_measurement="cpu",
            destination_measurement="cpu_agg",
            query="SELECT * FROM cpu",
            interval="1h",
        )

        assert cq.id == 1
        assert cq.name == "test-cq"
        assert cq.description is None
        assert cq.retention_days is None
        assert cq.delete_source_after_days is None
        assert cq.is_active is True


class TestExecuteCQResponse:
    """Tests for ExecuteCQResponse model."""

    def test_execute_cq_response_success(self) -> None:
        """Test successful CQ execution response."""
        data = {
            "query_id": 1,
            "query_name": "downsample-cpu",
            "execution_id": "exec-123",
            "status": "completed",
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-01T01:00:00Z",
            "records_read": 10000,
            "records_written": 100,
            "execution_time_seconds": 1.5,
            "destination_measurement": "cpu_1h",
        }
        response = ExecuteCQResponse(**data)

        assert response.query_id == 1
        assert response.query_name == "downsample-cpu"
        assert response.records_read == 10000
        assert response.records_written == 100
        assert response.status == "completed"

    def test_execute_cq_response_defaults(self) -> None:
        """Test ExecuteCQResponse default values."""
        response = ExecuteCQResponse(
            query_id=1,
            query_name="test",
            execution_id="exec-1",
            status="completed",
            start_time="2024-01-01T00:00:00Z",
            end_time="2024-01-01T01:00:00Z",
            destination_measurement="cpu_1h",
        )

        assert response.query_id == 1
        assert response.records_read is None
        assert response.records_written == 0


class TestCQExecution:
    """Tests for CQExecution model."""

    def test_cq_execution(self) -> None:
        """Test CQExecution model."""
        data = {
            "id": 456,
            "query_id": 1,
            "execution_id": "exec-456",
            "execution_time": "2024-01-01T00:00:00Z",
            "status": "completed",
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-01T01:00:00Z",
            "records_read": 5000,
            "records_written": 50,
            "execution_duration_seconds": 60.5,
            "error_message": None,
        }
        execution = CQExecution(**data)

        assert execution.id == 456
        assert execution.query_id == 1
        assert execution.status == "completed"
        assert execution.records_read == 5000
        assert execution.records_written == 50
        assert execution.error_message is None


class TestDeleteClientValidation:
    """Tests for DeleteClient input validation."""

    def test_empty_database_raises_error(self) -> None:
        """Test that empty database raises validation error."""
        from unittest.mock import MagicMock

        from arc_client.management.delete import DeleteClient

        http = MagicMock()
        config = MagicMock()

        client = DeleteClient(http, config)

        with pytest.raises(ArcValidationError, match="database is required"):
            client.delete("", "cpu", "time < '2024-01-01'")

    def test_empty_measurement_raises_error(self) -> None:
        """Test that empty measurement raises validation error."""
        from unittest.mock import MagicMock

        from arc_client.management.delete import DeleteClient

        http = MagicMock()
        config = MagicMock()

        client = DeleteClient(http, config)

        with pytest.raises(ArcValidationError, match="measurement is required"):
            client.delete("default", "", "time < '2024-01-01'")

    def test_empty_where_raises_error(self) -> None:
        """Test that empty where clause raises validation error."""
        from unittest.mock import MagicMock

        from arc_client.management.delete import DeleteClient

        http = MagicMock()
        config = MagicMock()

        client = DeleteClient(http, config)

        with pytest.raises(ArcValidationError, match="where clause is required"):
            client.delete("default", "cpu", "")

        with pytest.raises(ArcValidationError, match="where clause is required"):
            client.delete("default", "cpu", "   ")


class TestAsyncDeleteClientValidation:
    """Tests for AsyncDeleteClient input validation."""

    @pytest.mark.asyncio
    async def test_empty_database_raises_error(self) -> None:
        """Test that empty database raises validation error."""
        from unittest.mock import MagicMock

        from arc_client.management.async_delete import AsyncDeleteClient

        http = MagicMock()
        config = MagicMock()

        client = AsyncDeleteClient(http, config)

        with pytest.raises(ArcValidationError, match="database is required"):
            await client.delete("", "cpu", "time < '2024-01-01'")

    @pytest.mark.asyncio
    async def test_empty_measurement_raises_error(self) -> None:
        """Test that empty measurement raises validation error."""
        from unittest.mock import MagicMock

        from arc_client.management.async_delete import AsyncDeleteClient

        http = MagicMock()
        config = MagicMock()

        client = AsyncDeleteClient(http, config)

        with pytest.raises(ArcValidationError, match="measurement is required"):
            await client.delete("default", "", "time < '2024-01-01'")

    @pytest.mark.asyncio
    async def test_empty_where_raises_error(self) -> None:
        """Test that empty where clause raises validation error."""
        from unittest.mock import MagicMock

        from arc_client.management.async_delete import AsyncDeleteClient

        http = MagicMock()
        config = MagicMock()

        client = AsyncDeleteClient(http, config)

        with pytest.raises(ArcValidationError, match="where clause is required"):
            await client.delete("default", "cpu", "")
