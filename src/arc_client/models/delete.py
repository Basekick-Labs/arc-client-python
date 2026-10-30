"""Delete operation models."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


class DeleteResponse(BaseModel):
    """Response from a delete operation.

    Attributes:
        success: Whether the operation succeeded.
        deleted_count: Number of rows deleted.
        affected_files: Number of files containing matching rows.
        rewritten_files: Number of files actually rewritten.
        execution_time_ms: Execution time in milliseconds.
        dry_run: Whether this was a dry run preview.
        files_processed: List of processed file names.
        failed_files: Files whose deletion could not be completed.
        unreachable_files: Manifest files unavailable on this node.
        unflushed_nodes: Nodes whose buffered rows could not be flushed.
        cold_deleted_files: Whole cold files deleted or selected by a dry run.
        cold_files: Partially matching cold files left unchanged.
        cold_rows_not_deleted: Matching rows left in cold storage.
        cold_not_examined: Reason cold storage could not be examined.
        error: Error message if operation failed.
    """

    success: bool
    deleted_count: int = 0
    affected_files: int = 0
    rewritten_files: int = 0
    execution_time_ms: float = 0.0
    dry_run: bool = False
    files_processed: list[str] = Field(default_factory=list)
    failed_files: list[str] = Field(default_factory=list)
    unreachable_files: list[str] = Field(default_factory=list)
    unflushed_nodes: list[str] = Field(default_factory=list)
    cold_deleted_files: list[str] = Field(default_factory=list)
    cold_files: list[str] = Field(default_factory=list)
    cold_rows_not_deleted: int = 0
    cold_not_examined: str = ""
    error: Optional[str] = None

    @field_validator(
        "files_processed",
        "failed_files",
        "unreachable_files",
        "unflushed_nodes",
        "cold_deleted_files",
        "cold_files",
        mode="before",
    )
    @classmethod
    def _normalize_null_arrays(cls, value: Any) -> Any:
        """Go nil slices encode as null; expose them as empty lists."""
        return [] if value is None else value


class DeleteConfigResponse(BaseModel):
    """Response containing delete configuration.

    Attributes:
        enabled: Whether delete operations are enabled.
        confirmation_threshold: Row count requiring confirmation.
        max_rows_per_delete: Maximum rows per single delete operation.
        implementation: Delete implementation strategy.
        performance_impact: Performance impact description.
    """

    enabled: bool
    confirmation_threshold: int = 10000
    max_rows_per_delete: int = 1000000
    implementation: str = "rewrite-based"
    performance_impact: dict[str, str] = Field(default_factory=dict)
