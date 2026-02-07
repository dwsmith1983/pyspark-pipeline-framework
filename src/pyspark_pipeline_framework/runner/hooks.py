"""Pipeline lifecycle hooks protocol and infrastructure."""

from __future__ import annotations

import logging
from typing import Any, Protocol

from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.pipeline import PipelineConfig
from pyspark_pipeline_framework.core.resilience.circuit_breaker import CircuitState

logger = logging.getLogger(__name__)


class PipelineHooks(Protocol):
    """Protocol defining lifecycle callbacks for pipeline execution.

    Implementations receive notifications at key points during pipeline
    execution. This protocol is NOT ``@runtime_checkable`` — use
    structural typing or ``hasattr`` checks.
    """

    def before_pipeline(self, config: PipelineConfig) -> None:
        """Called before the pipeline starts executing."""
        ...

    def after_pipeline(self, config: PipelineConfig, result: Any) -> None:
        """Called after the pipeline finishes (success or failure)."""
        ...

    def before_component(self, config: ComponentConfig, index: int, total: int) -> None:
        """Called before each component executes."""
        ...

    def after_component(self, config: ComponentConfig, index: int, total: int, duration_ms: int) -> None:
        """Called after each component completes successfully."""
        ...

    def on_component_failure(self, config: ComponentConfig, index: int, error: Exception) -> None:
        """Called when a component raises an exception."""
        ...

    def on_retry_attempt(
        self,
        config: ComponentConfig,
        attempt: int,
        max_attempts: int,
        delay_ms: int,
        error: Exception,
    ) -> None:
        """Called before a retry attempt."""
        ...

    def on_circuit_breaker_state_change(
        self,
        component_name: str,
        old_state: CircuitState,
        new_state: CircuitState,
    ) -> None:
        """Called when a circuit breaker changes state."""
        ...


class NoOpHooks:
    """Hooks implementation that does nothing.

    Useful as a default or placeholder.
    """

    def before_pipeline(self, config: PipelineConfig) -> None:
        pass

    def after_pipeline(self, config: PipelineConfig, result: Any) -> None:
        pass

    def before_component(self, config: ComponentConfig, index: int, total: int) -> None:
        pass

    def after_component(self, config: ComponentConfig, index: int, total: int, duration_ms: int) -> None:
        pass

    def on_component_failure(self, config: ComponentConfig, index: int, error: Exception) -> None:
        pass

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


class CompositeHooks:
    """Broadcasts lifecycle events to multiple hooks implementations.

    Exceptions raised by individual hooks are caught and logged so that
    one misbehaving hook does not break the pipeline.
    """

    def __init__(self, *hooks: PipelineHooks) -> None:
        self._hooks: tuple[PipelineHooks, ...] = hooks

    def _call_all(self, method: str, *args: Any, **kwargs: Any) -> None:
        """Invoke *method* on every registered hook, swallowing errors."""
        for hook in self._hooks:
            try:
                getattr(hook, method)(*args, **kwargs)
            except Exception:
                logger.warning(
                    "Hook %s.%s raised an exception",
                    type(hook).__name__,
                    method,
                    exc_info=True,
                )

    def before_pipeline(self, config: PipelineConfig) -> None:
        self._call_all("before_pipeline", config)

    def after_pipeline(self, config: PipelineConfig, result: Any) -> None:
        self._call_all("after_pipeline", config, result)

    def before_component(self, config: ComponentConfig, index: int, total: int) -> None:
        self._call_all("before_component", config, index, total)

    def after_component(self, config: ComponentConfig, index: int, total: int, duration_ms: int) -> None:
        self._call_all("after_component", config, index, total, duration_ms)

    def on_component_failure(self, config: ComponentConfig, index: int, error: Exception) -> None:
        self._call_all("on_component_failure", config, index, error)

    def on_retry_attempt(
        self,
        config: ComponentConfig,
        attempt: int,
        max_attempts: int,
        delay_ms: int,
        error: Exception,
    ) -> None:
        self._call_all("on_retry_attempt", config, attempt, max_attempts, delay_ms, error)

    def on_circuit_breaker_state_change(
        self,
        component_name: str,
        old_state: CircuitState,
        new_state: CircuitState,
    ) -> None:
        self._call_all(
            "on_circuit_breaker_state_change",
            component_name,
            old_state,
            new_state,
        )
