"""Models for the reconciliation API (enterprise clustering)."""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class ReconciliationRun(BaseModel):
    """One reconciliation run, as Arc reports it.

    Mirrors ``reconciliation.Run`` in Arc. Unknown fields are kept (``extra``)
    so newer servers' counters are not lost. ``finished_at`` is Go's zero time
    (year 1) for a run that has not finished.
    """

    model_config = ConfigDict(extra="allow")

    id: str
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    dry_run: bool = True
    backend_kind: Optional[str] = None
    role: Optional[str] = None

    manifest_file_count: int = 0
    storage_file_count: int = 0
    orphan_manifest_count: int = 0
    orphan_storage_count: int = 0
    manifest_deletes: int = 0
    storage_deletes: int = 0
    skipped_grace: int = 0
    skipped_recheck: int = 0
    skipped_pending: int = 0
    skipped_held: int = 0
    skipped_pending_remote: int = 0
    skipped_adoptable: int = 0
    skipped_transient: int = 0
    skipped_invalid_path: int = 0

    orphan_manifest_sample: List[str] = Field(default_factory=list)
    orphan_storage_sample: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)

    aborted: bool = False
    abort_reason: Optional[str] = None
    abort_message: Optional[str] = None
    manifest_sweep_held: bool = False
    storage_sweep_held: bool = False
    storage_sweep_held_reason: Optional[str] = None
    cap_hit: bool = False
    walk_partial: bool = False
