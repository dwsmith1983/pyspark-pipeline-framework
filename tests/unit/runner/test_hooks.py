"""Tests for pipeline hooks protocol and infrastructure."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, call

import pytest

from pyspark_pipeline_framework.core.config.base import ComponentType
from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.resilience.circuit_breaker import CircuitState
from pyspark_pipeline_framework.runner.hooks import (
    CompositeHooks,
    NoOpHooks,
    PipelineHooks,
)


def _make_component_config(name: str = "test-comp") -> ComponentConfig:
    return ComponentConfig(
        name=name,
        component_type=ComponentType.TRANSFORMATION,
        class_path="fake.Module",
    )


class TestNoOpHooks:
    """Tests for NoOpHooks."""

    def test_all_methods_callable(self) -> None:
        """Every lifecycle method can be called without error."""
        hooks = NoOpHooks()
        pipeline_cfg = MagicMock()
        comp_cfg = _make_component_config()

        hooks.before_pipeline(pipeline_cfg)
        hooks.after_pipeline(pipeline_cfg, None)
        hooks.before_component(comp_cfg, 0, 1)
        hooks.after_component(comp_cfg, 0, 1, 100)
        hooks.on_component_failure(comp_cfg, 0, RuntimeError("fail"))
        hooks.on_retry_attempt(comp_cfg, 1, 3, 500, RuntimeError("retry"))
        hooks.on_circuit_breaker_state_change(
            "comp", CircuitState.CLOSED, CircuitState.OPEN
        )

    def test_method_signatures_match_protocol(self) -> None:
        """NoOpHooks implements all methods declared in PipelineHooks."""
        protocol_methods = {
            name
            for name in dir(PipelineHooks)
            if not name.startswith("_")
        }
        noop_methods = {
            name
            for name in dir(NoOpHooks)
            if not name.startswith("_") and callable(getattr(NoOpHooks, name))
        }
        assert protocol_methods.issubset(noop_methods)


class TestCompositeHooks:
    """Tests for CompositeHooks."""

    def _mock_hooks(self) -> MagicMock:
        """Create a mock that implements all hook methods."""
        mock = MagicMock()
        for method in [
            "before_pipeline",
            "after_pipeline",
            "before_component",
            "after_component",
            "on_component_failure",
            "on_retry_attempt",
            "on_circuit_breaker_state_change",
        ]:
            getattr(mock, method).return_value = None
        return mock

    def test_calls_all_hooks_in_order(self) -> None:
        """CompositeHooks delegates to each hook in registration order."""
        h1 = self._mock_hooks()
        h2 = self._mock_hooks()
        composite = CompositeHooks(h1, h2)

        pipeline_cfg = MagicMock()
        composite.before_pipeline(pipeline_cfg)

        h1.before_pipeline.assert_called_once_with(pipeline_cfg)
        h2.before_pipeline.assert_called_once_with(pipeline_cfg)

    def test_swallows_hook_exceptions(self) -> None:
        """A failing hook does not prevent other hooks from running."""
        failing = self._mock_hooks()
        failing.before_pipeline.side_effect = RuntimeError("boom")
        healthy = self._mock_hooks()

        composite = CompositeHooks(failing, healthy)
        pipeline_cfg = MagicMock()

        # Should not raise
        composite.before_pipeline(pipeline_cfg)
        healthy.before_pipeline.assert_called_once_with(pipeline_cfg)

    def test_empty_composite(self) -> None:
        """CompositeHooks with no hooks does nothing."""
        composite = CompositeHooks()
        pipeline_cfg = MagicMock()
        composite.before_pipeline(pipeline_cfg)  # Should not raise

    def test_single_hook_delegation(self) -> None:
        """CompositeHooks with a single hook delegates correctly."""
        hook = self._mock_hooks()
        composite = CompositeHooks(hook)

        comp_cfg = _make_component_config()
        composite.after_component(comp_cfg, 0, 3, 150)
        hook.after_component.assert_called_once_with(comp_cfg, 0, 3, 150)

    def test_before_after_pipeline(self) -> None:
        """before_pipeline and after_pipeline are delegated."""
        hook = self._mock_hooks()
        composite = CompositeHooks(hook)
        pipeline_cfg = MagicMock()

        composite.before_pipeline(pipeline_cfg)
        composite.after_pipeline(pipeline_cfg, "result")

        hook.before_pipeline.assert_called_once_with(pipeline_cfg)
        hook.after_pipeline.assert_called_once_with(pipeline_cfg, "result")

    def test_component_lifecycle(self) -> None:
        """Component lifecycle events are delegated."""
        hook = self._mock_hooks()
        composite = CompositeHooks(hook)
        comp_cfg = _make_component_config()

        composite.before_component(comp_cfg, 0, 2)
        composite.after_component(comp_cfg, 0, 2, 500)
        composite.on_component_failure(comp_cfg, 0, ValueError("err"))

        hook.before_component.assert_called_once()
        hook.after_component.assert_called_once()
        hook.on_component_failure.assert_called_once()

    def test_retry_callback(self) -> None:
        """on_retry_attempt is delegated."""
        hook = self._mock_hooks()
        composite = CompositeHooks(hook)
        comp_cfg = _make_component_config()
        err = RuntimeError("retry")

        composite.on_retry_attempt(comp_cfg, 2, 5, 1000, err)
        hook.on_retry_attempt.assert_called_once_with(
            comp_cfg, 2, 5, 1000, err
        )

    def test_circuit_breaker_callback(self) -> None:
        """on_circuit_breaker_state_change is delegated."""
        hook = self._mock_hooks()
        composite = CompositeHooks(hook)

        composite.on_circuit_breaker_state_change(
            "breaker", CircuitState.CLOSED, CircuitState.OPEN
        )
        hook.on_circuit_breaker_state_change.assert_called_once_with(
            "breaker", CircuitState.CLOSED, CircuitState.OPEN
        )
