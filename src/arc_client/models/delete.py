"""Delete operation models."""

from __future__ import annotations

from pydantic import BaseModel, Field


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
        error: Error message if operation failed.
    """

    success: bool
    deleted_count: int = 0
    affected_files: int = 0
    rewritten_files: int = 0
    execution_time_ms: float = 0.0
    dry_run: bool = False
    files_processed: list[str] = Field(default_factory=list)
    error: str | None = None


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
