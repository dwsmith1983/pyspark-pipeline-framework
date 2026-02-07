"""Checkpoint and resume system for pipeline fault recovery.

Provides component-level checkpointing so that a failed pipeline can be
resumed from the last successfully completed component.  The framework only
tracks *which components completed* — partition-level recovery is the
component's own responsibility.
"""

from __future__ import annotations

import hashlib
import json
import logging
import tempfile
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.pipeline import PipelineConfig
from pyspark_pipeline_framework.core.resilience.circuit_breaker import CircuitState
from pyspark_pipeline_framework.runner.result import PipelineResult, PipelineResultStatus

logger = logging.getLogger(__name__)

_VALID_STATUSES = frozenset({"in_progress", "completed", "failed"})


# ------------------------------------------------------------------
# Exception
# ------------------------------------------------------------------


class PipelineConfigChangedError(Exception):
    """Raised when a checkpoint's fingerprint doesn't match the current pipeline."""

    def __init__(self, run_id: str, pipeline_name: str) -> None:
        self.run_id = run_id
        self.pipeline_name = pipeline_name
        super().__init__(
            f"Pipeline config changed since checkpoint was saved (run_id={run_id!r}, pipeline={pipeline_name!r})"
        )


# ------------------------------------------------------------------
# State model
# ------------------------------------------------------------------


@dataclass
class CheckpointState:
    """Serialisable snapshot of pipeline checkpoint progress."""

    run_id: str
    pipeline_name: str
    pipeline_fingerprint: str
    completed_components: list[str] = field(default_factory=list)
    failed_component: str | None = None
    status: str = "in_progress"
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if not self.pipeline_name:
            raise ValueError("pipeline_name must not be empty")
        if not self.pipeline_fingerprint:
            raise ValueError("pipeline_fingerprint must not be empty")
        if self.status not in _VALID_STATUSES:
            raise ValueError(f"Invalid status {self.status!r}; must be one of {sorted(_VALID_STATUSES)}")


# ------------------------------------------------------------------
# Storage protocol
# ------------------------------------------------------------------


class CheckpointStore(Protocol):
    """Abstract checkpoint persistence layer."""

    def save(self, state: CheckpointState) -> None:
        """Persist *state* (upsert semantics)."""
        ...

    def load(self, run_id: str, pipeline_name: str) -> CheckpointState | None:
        """Load a checkpoint, or return ``None`` if it doesn't exist."""
        ...

    def delete(self, run_id: str, pipeline_name: str) -> None:
        """Delete a checkpoint.  No-op if it doesn't exist."""
        ...

    def exists(self, run_id: str, pipeline_name: str) -> bool:
        """Return whether a checkpoint exists."""
        ...


# ------------------------------------------------------------------
# Local filesystem implementation
# ------------------------------------------------------------------


class LocalCheckpointStore:
    """File-backed checkpoint store using atomic write-then-rename.

    Layout::

        {base_dir}/{pipeline_name}/{run_id}.json
    """

    def __init__(self, base_dir: Path) -> None:
        self._base_dir = base_dir

    def _path_for(self, run_id: str, pipeline_name: str) -> Path:
        return self._base_dir / pipeline_name / f"{run_id}.json"

    def save(self, state: CheckpointState) -> None:
        target = self._path_for(state.run_id, state.pipeline_name)
        target.parent.mkdir(parents=True, exist_ok=True)
        data = json.dumps(asdict(state), indent=2)
        # Atomic write: write to tmp in same dir, then rename
        fd, tmp_path_str = tempfile.mkstemp(dir=str(target.parent), suffix=".tmp")
        tmp_path = Path(tmp_path_str)
        try:
            tmp_path.write_text(data)
            tmp_path.replace(target)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
        finally:
            # fd was opened by mkstemp; close it
            import os

            os.close(fd)

    def load(self, run_id: str, pipeline_name: str) -> CheckpointState | None:
        target = self._path_for(run_id, pipeline_name)
        if not target.exists():
            return None
        data = json.loads(target.read_text())
        return CheckpointState(**data)

    def delete(self, run_id: str, pipeline_name: str) -> None:
        target = self._path_for(run_id, pipeline_name)
        target.unlink(missing_ok=True)

    def exists(self, run_id: str, pipeline_name: str) -> bool:
        return self._path_for(run_id, pipeline_name).exists()


# ------------------------------------------------------------------
# Pipeline fingerprint
# ------------------------------------------------------------------


def compute_pipeline_fingerprint(config: PipelineConfig) -> str:
    """Compute a SHA-256 fingerprint of a pipeline's structural identity.

    Includes each component's ``name``, ``class_path``, and sorted
    ``depends_on``.  Intentionally ignores ``config`` dicts and
    ``enabled`` flags so that parameter tuning between retries doesn't
    invalidate checkpoints.
    """
    hasher = hashlib.sha256()
    for comp in config.components:
        hasher.update(comp.name.encode())
        hasher.update(comp.class_path.encode())
        for dep in sorted(comp.depends_on):
            hasher.update(dep.encode())
    return hasher.hexdigest()


# ------------------------------------------------------------------
# Hooks
# ------------------------------------------------------------------


class CheckpointHooks:
    """Pipeline hooks that persist checkpoint state on lifecycle events.

    Designed to be composed via ``CompositeHooks`` alongside other hooks.
    """

    def __init__(
        self,
        store: CheckpointStore,
        run_id: str,
        pipeline_fingerprint: str,
        now_func: Callable[[], datetime] | None = None,
    ) -> None:
        self._store = store
        self._run_id = run_id
        self._pipeline_fingerprint = pipeline_fingerprint
        self._now = now_func or (lambda: datetime.now(timezone.utc))
        self._state: CheckpointState | None = None

    def before_pipeline(self, config: PipelineConfig) -> None:
        now = self._now().isoformat()
        self._state = CheckpointState(
            run_id=self._run_id,
            pipeline_name=config.name,
            pipeline_fingerprint=self._pipeline_fingerprint,
            created_at=now,
            updated_at=now,
        )
        self._store.save(self._state)

    def after_pipeline(self, config: PipelineConfig, result: Any) -> None:
        if self._state is None:
            return
        if isinstance(result, PipelineResult):
            if result.status == PipelineResultStatus.SUCCESS:
                self._state.status = "completed"
            else:
                self._state.status = "failed"
        self._state.updated_at = self._now().isoformat()
        self._store.save(self._state)

    def before_component(self, config: ComponentConfig, index: int, total: int) -> None:
        pass

    def after_component(self, config: ComponentConfig, index: int, total: int, duration_ms: int) -> None:
        if self._state is None:
            return
        self._state.completed_components.append(config.name)
        self._state.updated_at = self._now().isoformat()
        self._store.save(self._state)

    def on_component_failure(self, config: ComponentConfig, index: int, error: Exception) -> None:
        if self._state is None:
            return
        self._state.failed_component = config.name
        self._state.updated_at = self._now().isoformat()
        self._store.save(self._state)

    def on_retry_attempt(
        self,
        config: ComponentConfig,
        attempt: int,
        max_attempts: int,
        delay_ms: int,
        error: Exception,
    ) -> None:
        pass

    def on_circuit_breaker_state_change(
        self,
        component_name: str,
        old_state: CircuitState,
        new_state: CircuitState,
    ) -> None:
        pass


# ------------------------------------------------------------------
# Resume helper
# ------------------------------------------------------------------


def load_checkpoint_for_resume(
    store: CheckpointStore,
    run_id: str,
    config: PipelineConfig,
) -> set[str]:
    """Load a checkpoint and return the set of completed component names.

    Args:
        store: Checkpoint storage backend.
        run_id: Run identifier to resume.
        config: Current pipeline configuration (used for fingerprint
            validation).

    Returns:
        Set of component names that completed in the prior run.

    Raises:
        ValueError: If no checkpoint exists for *run_id*.
        PipelineConfigChangedError: If the pipeline fingerprint has changed
            since the checkpoint was saved.
    """
    state = store.load(run_id, config.name)
    if state is None:
        raise ValueError(f"No checkpoint found for run_id={run_id!r}, pipeline={config.name!r}")

    current_fingerprint = compute_pipeline_fingerprint(config)
    if state.pipeline_fingerprint != current_fingerprint:
        raise PipelineConfigChangedError(run_id, config.name)

    return set(state.completed_components)
