"""Simple pipeline runner with resilience and hooks."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.loader import load_from_file
from pyspark_pipeline_framework.core.config.pipeline import PipelineConfig
from pyspark_pipeline_framework.core.config.validator import validate_pipeline
from pyspark_pipeline_framework.core.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
)
from pyspark_pipeline_framework.core.resilience.retry import RetryExecutor
from pyspark_pipeline_framework.core.utils import safe_call
from pyspark_pipeline_framework.runner.hooks import NoOpHooks, PipelineHooks
from pyspark_pipeline_framework.runner.result import ComponentResult, PipelineResult, PipelineResultStatus
from pyspark_pipeline_framework.runtime.dataflow.base import DataFlow
from pyspark_pipeline_framework.runtime.loader import instantiate_component, validate_component_class
from pyspark_pipeline_framework.runtime.session.wrapper import SparkSessionWrapper

logger = logging.getLogger(__name__)


class SimplePipelineRunner:
    """Executes a pipeline by running components in topological order.

    Supports retry, circuit breaker, hooks, and Spark session injection.
    The runner does **not** manage SparkSession lifecycle — the caller
    owns start/stop. If no ``spark_wrapper`` is provided, one is created
    from ``config.spark`` but never stopped automatically.

    Args:
        config: Pipeline configuration.
        spark_wrapper: Optional pre-built session wrapper.
        hooks: Lifecycle hooks (default: ``NoOpHooks``).
        fail_fast: Stop on first component failure (default: ``True``).
        clock: Injectable monotonic clock for testing.
        sleep_func: Injectable sleep for testing retry delays.
        validate_before_run: Run static config validation before execution
            (default: ``True``).  Set to ``False`` to skip pre-flight checks.
    """

    def __init__(
        self,
        config: PipelineConfig,
        spark_wrapper: SparkSessionWrapper | None = None,
        hooks: PipelineHooks | None = None,
        fail_fast: bool = True,
        clock: Callable[[], float] | None = None,
        sleep_func: Callable[[float], None] | None = None,
        validate_before_run: bool = True,
    ) -> None:
        self._config = config
        self._spark_wrapper = spark_wrapper or SparkSessionWrapper(config.spark)
        self._hooks: PipelineHooks = hooks or NoOpHooks()
        self._fail_fast = fail_fast
        self._clock = clock or time.monotonic
        self._sleep_func = sleep_func
        self._validate_before_run = validate_before_run
        self._circuit_breakers: dict[str, CircuitBreaker] = {}

    @classmethod
    def from_file(cls, path: str | Path, **kwargs: Any) -> SimplePipelineRunner:
        """Create a runner from a HOCON configuration file.

        Args:
            path: Path to the HOCON file.
            **kwargs: Forwarded to the constructor.

        Returns:
            Configured ``SimplePipelineRunner``.
        """
        config = load_from_file(str(path), PipelineConfig)
        return cls(config, **kwargs)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, completed_components: set[str] | None = None) -> PipelineResult:
        """Execute the pipeline.

        Args:
            completed_components: Optional set of component names to skip
                (already completed in a prior run).  Pass ``None`` to run
                all components.

        Returns:
            ``PipelineResult`` with per-component outcomes and overall status.
        """
        start = self._clock()

        # Pre-flight config validation
        if self._validate_before_run:
            validation = validate_pipeline(self._config)
            for w in validation.warnings:
                logger.warning("Validation warning: %s", w)
            if not validation.is_valid:
                errors_msg = "; ".join(e.message for e in validation.errors)
                logger.error("Pipeline validation failed: %s", errors_msg)
                return PipelineResult(
                    status=PipelineResultStatus.FAILURE,
                    pipeline_name=self._config.name,
                    total_duration_ms=int((self._clock() - start) * 1000),
                )

        self._call_hook("before_pipeline", self._config)

        execution_order = self._config.get_execution_order()
        # Filter to enabled components only
        enabled_order: list[str] = []
        for name in execution_order:
            comp_config = self._config.get_component(name)
            if comp_config is not None and comp_config.enabled:
                enabled_order.append(name)

        total = len(enabled_order)
        results: list[ComponentResult] = []
        had_failure = False

        for index, name in enumerate(enabled_order):
            if completed_components and name in completed_components:
                logger.debug("Skipping already-completed component '%s'", name)
                continue

            comp_config = self._config.get_component(name)
            assert comp_config is not None  # guaranteed by filter above

            result = self._execute_component(comp_config, index, total)
            results.append(result)

            if not result.success:
                had_failure = True
                if self._fail_fast:
                    break

        total_duration_ms = int((self._clock() - start) * 1000)

        if not results or not had_failure:
            status = PipelineResultStatus.SUCCESS
        elif all(not r.success for r in results):
            status = PipelineResultStatus.FAILURE
        else:
            status = PipelineResultStatus.PARTIAL_SUCCESS

        pipeline_result = PipelineResult(
            status=status,
            pipeline_name=self._config.name,
            component_results=results,
            total_duration_ms=total_duration_ms,
        )

        self._call_hook("after_pipeline", self._config, pipeline_result)
        return pipeline_result

    def dry_run(self) -> list[str]:
        """Validate component classes without executing them.

        Returns:
            A list of warning strings. Empty means all classes are valid.
        """
        warnings: list[str] = []
        for comp_config in self._config.components:
            if comp_config.enabled:
                try:
                    warnings.extend(validate_component_class(comp_config.class_path))
                except Exception as exc:
                    warnings.append(f"Cannot load '{comp_config.class_path}': {exc}")
        return warnings

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _execute_component(
        self,
        comp_config: ComponentConfig,
        index: int,
        total: int,
    ) -> ComponentResult:
        """Run a single component with resilience wrappers."""
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
                component.set_spark_session(self._spark_wrapper.spark)

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
