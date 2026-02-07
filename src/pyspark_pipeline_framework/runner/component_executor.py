"""Single-component execution with resilience wrappers."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from pyspark_pipeline_framework.core.component.protocols import Resource
from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
)
from pyspark_pipeline_framework.core.resilience.retry import RetryExecutor
from pyspark_pipeline_framework.core.utils import safe_call
from pyspark_pipeline_framework.runner.hooks import PipelineHooks
from pyspark_pipeline_framework.runner.result import ComponentResult
from pyspark_pipeline_framework.runtime.dataflow.base import DataFlow
from pyspark_pipeline_framework.runtime.loader import instantiate_component

logger = logging.getLogger(__name__)


class ComponentExecutor:
    """Executes a single pipeline component with retry, circuit breaker,
    Resource lifecycle, and hook callbacks.

    Extracted from ``SimplePipelineRunner._execute_component`` to improve
    testability and reduce runner complexity.

    Args:
        hooks: Lifecycle hooks instance.
        circuit_breakers: Shared mutable dict of circuit breakers keyed by
            component name.
        clock: Monotonic clock function.
        sleep_func: Sleep function for retry delays (``None`` uses default).
    """

    def __init__(
        self,
        hooks: PipelineHooks,
        circuit_breakers: dict[str, CircuitBreaker],
        clock: Callable[[], float] | None = None,
        sleep_func: Callable[[float], None] | None = None,
    ) -> None:
        self._hooks = hooks
        self._circuit_breakers = circuit_breakers
        self._clock = clock or time.monotonic
        self._sleep_func = sleep_func

    def execute(
        self,
        comp_config: ComponentConfig,
        spark_session: Any,
        index: int,
        total: int,
    ) -> ComponentResult:
        """Run a single component with resilience wrappers.

        Args:
            comp_config: Component configuration.
            spark_session: Active SparkSession to inject into DataFlow components.
            index: Zero-based position in the execution order.
            total: Total number of components to execute.

        Returns:
            A ``ComponentResult`` indicating success or failure.
        """
        start = self._clock()
        self._call_hook("before_component", comp_config, index, total)
        retries_used = 0

        try:
            # Circuit breaker guard
            cb = self._get_or_create_circuit_breaker(comp_config)
            if cb is not None:
                state = cb.state
                if state is CircuitState.OPEN:
                    raise CircuitBreakerOpenError(comp_config.name, cb.time_until_reset)

            # Instantiate
            component = instantiate_component(comp_config)

            # Inject spark if DataFlow
            if isinstance(component, DataFlow):
                component.set_spark_session(spark_session)

            # Resource lifecycle — open before run, close in finally
            if isinstance(component, Resource):
                component.open()

            try:
                # Execute with optional retry
                if comp_config.retry is not None:
                    executor = RetryExecutor(
                        comp_config.retry,
                        jitter_factor=0.0,
                        sleep_func=self._sleep_func,
                    )
                    attempts_before = [0]

                    def on_retry(attempt: int, error: Exception, delay: float) -> None:
                        attempts_before[0] = attempt
                        delay_ms = int(delay * 1000)
                        self._call_hook(
                            "on_retry_attempt",
                            comp_config,
                            attempt,
                            comp_config.retry.max_attempts,  # type: ignore[union-attr]
                            delay_ms,
                            error,
                        )

                    executor.execute(component.run, on_retry=on_retry)
                    retries_used = attempts_before[0]
                else:
                    component.run()
            finally:
                if isinstance(component, Resource):
                    component.close()

            # Record success on CB
            if cb is not None:
                cb.record_success()

            duration_ms = int((self._clock() - start) * 1000)
            self._call_hook("after_component", comp_config, index, total, duration_ms)
            return ComponentResult(
                component_name=comp_config.name,
                success=True,
                duration_ms=duration_ms,
                retries=retries_used,
            )

        except Exception as exc:
            # Record failure on CB
            cb = self._circuit_breakers.get(comp_config.name)
            if cb is not None and not isinstance(exc, CircuitBreakerOpenError):
                cb.record_failure()

            duration_ms = int((self._clock() - start) * 1000)
            self._call_hook("on_component_failure", comp_config, index, exc)
            return ComponentResult(
                component_name=comp_config.name,
                success=False,
                duration_ms=duration_ms,
                error=exc,
                retries=retries_used,
            )

    def _get_or_create_circuit_breaker(self, comp_config: ComponentConfig) -> CircuitBreaker | None:
        """Return the circuit breaker for a component, or ``None``."""
        if comp_config.circuit_breaker is None:
            return None

        if comp_config.name not in self._circuit_breakers:

            def on_state_change(old: CircuitState, new: CircuitState) -> None:
                self._call_hook(
                    "on_circuit_breaker_state_change",
                    comp_config.name,
                    old,
                    new,
                )

            self._circuit_breakers[comp_config.name] = CircuitBreaker(
                config=comp_config.circuit_breaker,
                name=comp_config.name,
                on_state_change=on_state_change,
                clock=self._clock,
            )

        return self._circuit_breakers[comp_config.name]

    def _call_hook(self, method: str, *args: Any) -> None:
        """Invoke a hook method defensively — errors are logged, not raised."""
        safe_call(
            lambda: getattr(self._hooks, method)(*args),
            logger,
            "Hook %s.%s raised an exception",
            type(self._hooks).__name__,
            method,
        )
