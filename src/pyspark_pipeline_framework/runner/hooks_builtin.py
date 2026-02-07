"""Built-in pipeline hooks: logging and metrics collection."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.pipeline import PipelineConfig
from pyspark_pipeline_framework.core.metrics.registry import MeterRegistry
from pyspark_pipeline_framework.core.resilience.circuit_breaker import CircuitState


class LoggingHooks:
    """Hooks that log pipeline lifecycle events.

    Uses ``%s`` formatting for lazy evaluation.

    Args:
        logger: Custom logger instance. Defaults to ``logging.getLogger("ppf.pipeline")``.
    """

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger("ppf.pipeline")

    @property
    def logger(self) -> logging.Logger:
        """Return the logger used by this hooks instance."""
        return self._logger

    def before_pipeline(self, config: PipelineConfig) -> None:
        self._logger.info(
            "Pipeline '%s' v%s starting with %d components",
            config.name,
            config.version,
            len(config.components),
        )

    def after_pipeline(self, config: PipelineConfig, result: Any) -> None:
        self._logger.info(
            "Pipeline '%s' v%s completed",
            config.name,
            config.version,
        )

    def before_component(self, config: ComponentConfig, index: int, total: int) -> None:
        self._logger.info(
            "Component '%s' [%d/%d] starting",
            config.name,
            index + 1,
            total,
        )

    def after_component(self, config: ComponentConfig, index: int, total: int, duration_ms: int) -> None:
        self._logger.info(
            "Component '%s' [%d/%d] completed in %dms",
            config.name,
            index + 1,
            total,
            duration_ms,
        )

    def on_component_failure(self, config: ComponentConfig, index: int, error: Exception) -> None:
        self._logger.error(
            "Component '%s' [%d] failed: %s",
            config.name,
            index + 1,
            error,
        )

    def on_retry_attempt(
        self,
        config: ComponentConfig,
        attempt: int,
        max_attempts: int,
        delay_ms: int,
        error: Exception,
    ) -> None:
        self._logger.warning(
            "Component '%s' retry %d/%d after %dms: %s",
            config.name,
            attempt,
            max_attempts,
            delay_ms,
            error,
        )

    def on_circuit_breaker_state_change(
        self,
        component_name: str,
        old_state: CircuitState,
        new_state: CircuitState,
    ) -> None:
        self._logger.warning(
            "Circuit breaker '%s': %s -> %s",
            component_name,
            old_state.value,
            new_state.value,
        )


class MetricsHooks:
    """Hooks that collect execution timing and retry metrics.

    When a :class:`~pyspark_pipeline_framework.core.metrics.registry.MeterRegistry`
    is provided, metrics are also recorded there for export to external
    observability backends (Prometheus, OpenTelemetry, etc.).

    Args:
        clock: Injectable monotonic clock for testing.
            Defaults to ``time.monotonic``.
        registry: Optional meter registry for structured metrics export.
    """

    def __init__(
        self,
        clock: Callable[[], float] | None = None,
        registry: MeterRegistry | None = None,
    ) -> None:
        self._clock = clock or time.monotonic
        self._registry = registry
        self.component_durations: dict[str, int] = {}
        self.component_retries: dict[str, int] = {}
        self.total_duration_ms: int = 0
        self._pipeline_start: float = 0.0

    @property
    def registry(self) -> MeterRegistry | None:
        """Return the meter registry, if configured."""
        return self._registry

    def before_pipeline(self, config: PipelineConfig) -> None:
        self.component_durations = {}
        self.component_retries = {}
        self.total_duration_ms = 0
        self._pipeline_start = self._clock()
        if self._registry is not None:
            self._registry.gauge(
                "ppf.pipeline.components",
                float(len(config.components)),
                tags={"pipeline": config.name},
            )

    def after_pipeline(self, config: PipelineConfig, result: Any) -> None:
        elapsed = self._clock() - self._pipeline_start
        self.total_duration_ms = int(elapsed * 1000)
        if self._registry is not None:
            self._registry.timer(
                "ppf.pipeline.duration",
                float(self.total_duration_ms),
                tags={"pipeline": config.name},
            )

    def before_component(self, config: ComponentConfig, index: int, total: int) -> None:
        pass

    def after_component(self, config: ComponentConfig, index: int, total: int, duration_ms: int) -> None:
        self.component_durations[config.name] = duration_ms
        if self._registry is not None:
            self._registry.timer(
                "ppf.component.duration",
                float(duration_ms),
                tags={"component": config.name},
            )

    def on_component_failure(self, config: ComponentConfig, index: int, error: Exception) -> None:
        if self._registry is not None:
            self._registry.counter(
                "ppf.component.failures",
                tags={"component": config.name},
            )

    def on_retry_attempt(
        self,
        config: ComponentConfig,
        attempt: int,
        max_attempts: int,
        delay_ms: int,
        error: Exception,
    ) -> None:
        self.component_retries[config.name] = self.component_retries.get(config.name, 0) + 1
        if self._registry is not None:
            self._registry.counter(
                "ppf.component.retries",
                tags={"component": config.name},
            )

    def on_circuit_breaker_state_change(
        self,
        component_name: str,
        old_state: CircuitState,
        new_state: CircuitState,
    ) -> None:
        if self._registry is not None:
            self._registry.counter(
                "ppf.circuit_breaker.state_changes",
                tags={"component": component_name, "to_state": new_state.value},
            )
