"""Tests for built-in pipeline hooks (logging and metrics)."""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

from pyspark_pipeline_framework.core.metrics.registry import InMemoryRegistry
from pyspark_pipeline_framework.core.resilience.circuit_breaker import CircuitState
from pyspark_pipeline_framework.runner.hooks_builtin import (
    LoggingHooks,
    MetricsHooks,
)
from tests.factories import make_component_config, make_pipeline_config


def make_pipeline_config() -> MagicMock:
    cfg = MagicMock()
    cfg.name = "test-pipeline"
    cfg.version = "1.0.0"
    cfg.components = [make_component_config()]
    return cfg


class TestLoggingHooks:
    """Tests for LoggingHooks."""

    def test_default_logger_name(self) -> None:
        """Default logger is named 'ppf.pipeline'."""
        hooks = LoggingHooks()
        assert hooks.logger.name == "ppf.pipeline"

    def test_custom_logger(self) -> None:
        """A custom logger can be injected."""
        custom = logging.getLogger("custom.test")
        hooks = LoggingHooks(logger=custom)
        assert hooks.logger is custom

    def test_before_pipeline_logs(self) -> None:
        """before_pipeline emits an info log."""
        mock_logger = MagicMock(spec=logging.Logger)
        hooks = LoggingHooks(logger=mock_logger)
        cfg = make_pipeline_config()

        hooks.before_pipeline(cfg)
        mock_logger.info.assert_called_once()
        args = mock_logger.info.call_args
        assert "test-pipeline" in str(args)

    def test_after_pipeline_logs(self) -> None:
        """after_pipeline emits an info log."""
        mock_logger = MagicMock(spec=logging.Logger)
        hooks = LoggingHooks(logger=mock_logger)
        cfg = make_pipeline_config()

        hooks.after_pipeline(cfg, None)
        mock_logger.info.assert_called_once()

    def test_component_lifecycle_logging(self) -> None:
        """before/after/failure component events log at appropriate levels."""
        mock_logger = MagicMock(spec=logging.Logger)
        hooks = LoggingHooks(logger=mock_logger)
        comp_cfg = make_component_config()

        hooks.before_component(comp_cfg, 0, 3)
        assert mock_logger.info.call_count == 1

        hooks.after_component(comp_cfg, 0, 3, 250)
        assert mock_logger.info.call_count == 2

        hooks.on_component_failure(comp_cfg, 0, RuntimeError("fail"))
        mock_logger.error.assert_called_once()

    def test_retry_and_circuit_breaker_logging(self) -> None:
        """Retry and circuit breaker events log at warning level."""
        mock_logger = MagicMock(spec=logging.Logger)
        hooks = LoggingHooks(logger=mock_logger)
        comp_cfg = make_component_config()

        hooks.on_retry_attempt(comp_cfg, 1, 3, 500, RuntimeError("err"))
        assert mock_logger.warning.call_count == 1

        hooks.on_circuit_breaker_state_change(
            "comp", CircuitState.CLOSED, CircuitState.OPEN
        )
        assert mock_logger.warning.call_count == 2


class TestMetricsHooks:
    """Tests for MetricsHooks."""

    def test_tracks_component_durations(self) -> None:
        """after_component records duration per component."""
        metrics = MetricsHooks()
        cfg = make_pipeline_config()
        comp = make_component_config("transform-a")

        metrics.before_pipeline(cfg)
        metrics.after_component(comp, 0, 1, 350)

        assert metrics.component_durations == {"transform-a": 350}

    def test_tracks_retries(self) -> None:
        """on_retry_attempt counts retries per component."""
        metrics = MetricsHooks()
        cfg = make_pipeline_config()
        comp = make_component_config("flaky")

        metrics.before_pipeline(cfg)
        metrics.on_retry_attempt(comp, 1, 3, 100, RuntimeError("err"))
        metrics.on_retry_attempt(comp, 2, 3, 200, RuntimeError("err"))

        assert metrics.component_retries == {"flaky": 2}

    def test_computes_total_duration(self) -> None:
        """total_duration_ms is computed from clock in after_pipeline."""
        clock_values = iter([10.0, 10.5])  # 0.5s = 500ms
        metrics = MetricsHooks(clock=lambda: next(clock_values))

        cfg = make_pipeline_config()
        metrics.before_pipeline(cfg)
        metrics.after_pipeline(cfg, None)

        assert metrics.total_duration_ms == 500

    def test_reset_on_new_pipeline_run(self) -> None:
        """before_pipeline resets all accumulated metrics."""
        clock_values = iter([0.0, 1.0, 2.0, 2.25])
        metrics = MetricsHooks(clock=lambda: next(clock_values))
        cfg = make_pipeline_config()
        comp = make_component_config("comp-a")

        # First run
        metrics.before_pipeline(cfg)
        metrics.after_component(comp, 0, 1, 100)
        metrics.on_retry_attempt(comp, 1, 3, 50, RuntimeError("err"))
        metrics.after_pipeline(cfg, None)

        assert metrics.total_duration_ms == 1000
        assert metrics.component_durations == {"comp-a": 100}
        assert metrics.component_retries == {"comp-a": 1}

        # Second run — should reset
        metrics.before_pipeline(cfg)
        assert metrics.component_durations == {}
        assert metrics.component_retries == {}
        assert metrics.total_duration_ms == 0

        metrics.after_pipeline(cfg, None)
        assert metrics.total_duration_ms == 250


class TestMetricsHooksWithRegistry:
    """Tests for MetricsHooks with an InMemoryRegistry."""

    def test_registry_property(self) -> None:
        reg = InMemoryRegistry()
        hooks = MetricsHooks(registry=reg)
        assert hooks.registry is reg

    def test_no_registry_by_default(self) -> None:
        hooks = MetricsHooks()
        assert hooks.registry is None

    def test_records_pipeline_duration(self) -> None:
        reg = InMemoryRegistry()
        clock_values = iter([10.0, 10.5])
        hooks = MetricsHooks(clock=lambda: next(clock_values), registry=reg)
        cfg = make_pipeline_config()

        hooks.before_pipeline(cfg)
        hooks.after_pipeline(cfg, None)

        assert reg.get_timer_total("ppf.pipeline.duration", tags={"pipeline": "test-pipeline"}) == 500.0

    def test_records_component_duration(self) -> None:
        reg = InMemoryRegistry()
        hooks = MetricsHooks(registry=reg)
        comp = make_component_config("transform-a")

        hooks.after_component(comp, 0, 1, 350)

        assert reg.get_timer_total("ppf.component.duration", tags={"component": "transform-a"}) == 350.0

    def test_records_component_count_on_before_pipeline(self) -> None:
        reg = InMemoryRegistry()
        hooks = MetricsHooks(registry=reg)
        cfg = make_pipeline_config()

        hooks.before_pipeline(cfg)

        assert reg.get_gauge("ppf.pipeline.components", tags={"pipeline": "test-pipeline"}) == 1.0

    def test_records_retries(self) -> None:
        reg = InMemoryRegistry()
        hooks = MetricsHooks(registry=reg)
        comp = make_component_config("flaky")

        hooks.on_retry_attempt(comp, 1, 3, 100, RuntimeError("err"))
        hooks.on_retry_attempt(comp, 2, 3, 200, RuntimeError("err"))

        assert reg.get_counter("ppf.component.retries", tags={"component": "flaky"}) == 2.0

    def test_records_failures(self) -> None:
        reg = InMemoryRegistry()
        hooks = MetricsHooks(registry=reg)
        comp = make_component_config("broken")

        hooks.on_component_failure(comp, 0, RuntimeError("fail"))

        assert reg.get_counter("ppf.component.failures", tags={"component": "broken"}) == 1.0

    def test_records_circuit_breaker_state_changes(self) -> None:
        reg = InMemoryRegistry()
        hooks = MetricsHooks(registry=reg)

        hooks.on_circuit_breaker_state_change("comp1", CircuitState.CLOSED, CircuitState.OPEN)

        assert reg.get_counter(
            "ppf.circuit_breaker.state_changes",
            tags={"component": "comp1", "to_state": "open"},
        ) == 1.0
