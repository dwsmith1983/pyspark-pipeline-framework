"""Tests for the checkpoint and resume system."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from pyspark_pipeline_framework.core.config.base import ComponentType
from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.pipeline import PipelineConfig
from pyspark_pipeline_framework.core.config.spark import SparkConfig
from pyspark_pipeline_framework.runner.checkpoint import (
    CheckpointHooks,
    CheckpointState,
    LocalCheckpointStore,
    PipelineConfigChangedError,
    compute_pipeline_fingerprint,
    load_checkpoint_for_resume,
)
from pyspark_pipeline_framework.runner.result import (
    PipelineResult,
    PipelineResultStatus,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SPARK = SparkConfig(app_name="test")


def _comp(
    name: str,
    class_path: str = "some.Module",
    depends_on: list[str] | None = None,
    enabled: bool = True,
    config: dict[str, Any] | None = None,
) -> ComponentConfig:
    return ComponentConfig(
        name=name,
        component_type=ComponentType.TRANSFORMATION,
        class_path=class_path,
        depends_on=depends_on or [],
        enabled=enabled,
        config=config or {},
    )


def _pipeline(
    components: list[ComponentConfig],
    name: str = "test-pipe",
) -> PipelineConfig:
    return PipelineConfig(
        name=name, version="1.0", spark=_SPARK, components=components
    )


# ===================================================================
# TestCheckpointState
# ===================================================================


class TestCheckpointState:
    def test_valid_construction(self) -> None:
        state = CheckpointState(
            run_id="r1",
            pipeline_name="p1",
            pipeline_fingerprint="abc123",
        )
        assert state.run_id == "r1"
        assert state.status == "in_progress"
        assert state.completed_components == []
        assert state.failed_component is None

    def test_empty_run_id_raises(self) -> None:
        with pytest.raises(ValueError, match="run_id"):
            CheckpointState(
                run_id="", pipeline_name="p", pipeline_fingerprint="fp"
            )

    def test_empty_pipeline_name_raises(self) -> None:
        with pytest.raises(ValueError, match="pipeline_name"):
            CheckpointState(
                run_id="r", pipeline_name="", pipeline_fingerprint="fp"
            )

    def test_empty_fingerprint_raises(self) -> None:
        with pytest.raises(ValueError, match="pipeline_fingerprint"):
            CheckpointState(
                run_id="r", pipeline_name="p", pipeline_fingerprint=""
            )

    def test_invalid_status_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid status"):
            CheckpointState(
                run_id="r",
                pipeline_name="p",
                pipeline_fingerprint="fp",
                status="running",
            )


# ===================================================================
# TestComputePipelineFingerprint
# ===================================================================


class TestComputePipelineFingerprint:
    def test_deterministic(self) -> None:
        cfg = _pipeline([_comp("a"), _comp("b")])
        assert compute_pipeline_fingerprint(cfg) == compute_pipeline_fingerprint(cfg)

    def test_different_name_changes_hash(self) -> None:
        cfg1 = _pipeline([_comp("a")])
        cfg2 = _pipeline([_comp("b")])
        assert compute_pipeline_fingerprint(cfg1) != compute_pipeline_fingerprint(cfg2)

    def test_different_class_path_changes_hash(self) -> None:
        cfg1 = _pipeline([_comp("a", class_path="x.A")])
        cfg2 = _pipeline([_comp("a", class_path="x.B")])
        assert compute_pipeline_fingerprint(cfg1) != compute_pipeline_fingerprint(cfg2)

    def test_different_depends_on_changes_hash(self) -> None:
        cfg1 = _pipeline([_comp("a"), _comp("b", depends_on=["a"])])
        cfg2 = _pipeline([_comp("a"), _comp("b")])
        assert compute_pipeline_fingerprint(cfg1) != compute_pipeline_fingerprint(cfg2)

    def test_config_dict_does_not_change_hash(self) -> None:
        cfg1 = _pipeline([_comp("a", config={"x": 1})])
        cfg2 = _pipeline([_comp("a", config={"x": 999})])
        assert compute_pipeline_fingerprint(cfg1) == compute_pipeline_fingerprint(cfg2)

    def test_enabled_flag_does_not_change_hash(self) -> None:
        cfg1 = _pipeline([_comp("a", enabled=True)])
        cfg2 = _pipeline([_comp("a", enabled=False)])
        assert compute_pipeline_fingerprint(cfg1) == compute_pipeline_fingerprint(cfg2)


# ===================================================================
# TestLocalCheckpointStore
# ===================================================================


class TestLocalCheckpointStore:
    def test_save_load_roundtrip(self, tmp_path: Path) -> None:
        store = LocalCheckpointStore(tmp_path)
        state = CheckpointState(
            run_id="r1",
            pipeline_name="p1",
            pipeline_fingerprint="fp",
            completed_components=["a", "b"],
        )
        store.save(state)
        loaded = store.load("r1", "p1")
        assert loaded is not None
        assert loaded.run_id == "r1"
        assert loaded.completed_components == ["a", "b"]

    def test_load_nonexistent_returns_none(self, tmp_path: Path) -> None:
        store = LocalCheckpointStore(tmp_path)
        assert store.load("missing", "nope") is None

    def test_exists_before_save(self, tmp_path: Path) -> None:
        store = LocalCheckpointStore(tmp_path)
        assert not store.exists("r1", "p1")

    def test_exists_after_save(self, tmp_path: Path) -> None:
        store = LocalCheckpointStore(tmp_path)
        state = CheckpointState(
            run_id="r1", pipeline_name="p1", pipeline_fingerprint="fp"
        )
        store.save(state)
        assert store.exists("r1", "p1")

    def test_delete_removes(self, tmp_path: Path) -> None:
        store = LocalCheckpointStore(tmp_path)
        state = CheckpointState(
            run_id="r1", pipeline_name="p1", pipeline_fingerprint="fp"
        )
        store.save(state)
        store.delete("r1", "p1")
        assert not store.exists("r1", "p1")

    def test_delete_nonexistent_is_noop(self, tmp_path: Path) -> None:
        store = LocalCheckpointStore(tmp_path)
        store.delete("r1", "p1")  # should not raise

    def test_save_creates_directories(self, tmp_path: Path) -> None:
        store = LocalCheckpointStore(tmp_path / "deep" / "nested")
        state = CheckpointState(
            run_id="r1", pipeline_name="p1", pipeline_fingerprint="fp"
        )
        store.save(state)
        assert store.exists("r1", "p1")


# ===================================================================
# TestCheckpointHooks
# ===================================================================


class TestCheckpointHooks:
    def _make_store(self, tmp_path: Path) -> LocalCheckpointStore:
        return LocalCheckpointStore(tmp_path)

    def _make_config(self) -> PipelineConfig:
        return _pipeline([_comp("a"), _comp("b")])

    def _fixed_now(self) -> datetime:
        return datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

    def test_before_pipeline_saves_initial_state(self, tmp_path: Path) -> None:
        store = self._make_store(tmp_path)
        cfg = self._make_config()
        fp = compute_pipeline_fingerprint(cfg)
        hooks = CheckpointHooks(store, "r1", fp, now_func=self._fixed_now)

        hooks.before_pipeline(cfg)

        loaded = store.load("r1", "test-pipe")
        assert loaded is not None
        assert loaded.status == "in_progress"
        assert loaded.completed_components == []

    def test_after_component_appends_completed(self, tmp_path: Path) -> None:
        store = self._make_store(tmp_path)
        cfg = self._make_config()
        fp = compute_pipeline_fingerprint(cfg)
        hooks = CheckpointHooks(store, "r1", fp, now_func=self._fixed_now)

        hooks.before_pipeline(cfg)
        hooks.after_component(cfg.components[0], 0, 2, 100)

        loaded = store.load("r1", "test-pipe")
        assert loaded is not None
        assert loaded.completed_components == ["a"]

    def test_multiple_components_tracked(self, tmp_path: Path) -> None:
        store = self._make_store(tmp_path)
        cfg = self._make_config()
        fp = compute_pipeline_fingerprint(cfg)
        hooks = CheckpointHooks(store, "r1", fp, now_func=self._fixed_now)

        hooks.before_pipeline(cfg)
        hooks.after_component(cfg.components[0], 0, 2, 100)
        hooks.after_component(cfg.components[1], 1, 2, 200)

        loaded = store.load("r1", "test-pipe")
        assert loaded is not None
        assert loaded.completed_components == ["a", "b"]

    def test_on_component_failure_records_name(self, tmp_path: Path) -> None:
        store = self._make_store(tmp_path)
        cfg = self._make_config()
        fp = compute_pipeline_fingerprint(cfg)
        hooks = CheckpointHooks(store, "r1", fp, now_func=self._fixed_now)

        hooks.before_pipeline(cfg)
        hooks.on_component_failure(cfg.components[0], 0, RuntimeError("boom"))

        loaded = store.load("r1", "test-pipe")
        assert loaded is not None
        assert loaded.failed_component == "a"

    def test_after_pipeline_marks_completed(self, tmp_path: Path) -> None:
        store = self._make_store(tmp_path)
        cfg = self._make_config()
        fp = compute_pipeline_fingerprint(cfg)
        hooks = CheckpointHooks(store, "r1", fp, now_func=self._fixed_now)

        hooks.before_pipeline(cfg)
        result = PipelineResult(
            status=PipelineResultStatus.SUCCESS,
            pipeline_name="test-pipe",
        )
        hooks.after_pipeline(cfg, result)

        loaded = store.load("r1", "test-pipe")
        assert loaded is not None
        assert loaded.status == "completed"

    def test_after_pipeline_marks_failed(self, tmp_path: Path) -> None:
        store = self._make_store(tmp_path)
        cfg = self._make_config()
        fp = compute_pipeline_fingerprint(cfg)
        hooks = CheckpointHooks(store, "r1", fp, now_func=self._fixed_now)

        hooks.before_pipeline(cfg)
        result = PipelineResult(
            status=PipelineResultStatus.FAILURE,
            pipeline_name="test-pipe",
        )
        hooks.after_pipeline(cfg, result)

        loaded = store.load("r1", "test-pipe")
        assert loaded is not None
        assert loaded.status == "failed"

    def test_injectable_now_func(self, tmp_path: Path) -> None:
        store = self._make_store(tmp_path)
        cfg = self._make_config()
        fp = compute_pipeline_fingerprint(cfg)
        ts = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        hooks = CheckpointHooks(store, "r1", fp, now_func=lambda: ts)

        hooks.before_pipeline(cfg)
        loaded = store.load("r1", "test-pipe")
        assert loaded is not None
        assert loaded.created_at == ts.isoformat()

    def test_safe_when_state_is_none(self, tmp_path: Path) -> None:
        store = self._make_store(tmp_path)
        cfg = self._make_config()
        fp = compute_pipeline_fingerprint(cfg)
        hooks = CheckpointHooks(store, "r1", fp)

        # These should not raise even without before_pipeline
        hooks.after_component(cfg.components[0], 0, 2, 100)
        hooks.on_component_failure(cfg.components[0], 0, RuntimeError("x"))
        result = PipelineResult(
            status=PipelineResultStatus.SUCCESS, pipeline_name="test-pipe"
        )
        hooks.after_pipeline(cfg, result)


# ===================================================================
# TestLoadCheckpointForResume
# ===================================================================


class TestLoadCheckpointForResume:
    def test_returns_completed_set(self, tmp_path: Path) -> None:
        store = LocalCheckpointStore(tmp_path)
        cfg = _pipeline([_comp("a"), _comp("b")])
        fp = compute_pipeline_fingerprint(cfg)
        state = CheckpointState(
            run_id="r1",
            pipeline_name="test-pipe",
            pipeline_fingerprint=fp,
            completed_components=["a"],
        )
        store.save(state)

        result = load_checkpoint_for_resume(store, "r1", cfg)
        assert result == {"a"}

    def test_raises_on_missing(self, tmp_path: Path) -> None:
        store = LocalCheckpointStore(tmp_path)
        cfg = _pipeline([_comp("a")])

        with pytest.raises(ValueError, match="No checkpoint found"):
            load_checkpoint_for_resume(store, "missing", cfg)

    def test_raises_on_fingerprint_mismatch(self, tmp_path: Path) -> None:
        store = LocalCheckpointStore(tmp_path)
        cfg = _pipeline([_comp("a")])
        state = CheckpointState(
            run_id="r1",
            pipeline_name="test-pipe",
            pipeline_fingerprint="stale-fingerprint",
            completed_components=["a"],
        )
        store.save(state)

        with pytest.raises(PipelineConfigChangedError):
            load_checkpoint_for_resume(store, "r1", cfg)


# ===================================================================
# TestPipelineConfigChangedError
# ===================================================================


class TestPipelineConfigChangedError:
    def test_message_contains_ids(self) -> None:
        err = PipelineConfigChangedError("run-42", "my-pipeline")
        assert "run-42" in str(err)
        assert "my-pipeline" in str(err)
        assert err.run_id == "run-42"
        assert err.pipeline_name == "my-pipeline"
