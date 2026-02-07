"""Data quality hooks for pipeline lifecycle integration."""

from __future__ import annotations

import logging
from typing import Any

from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.pipeline import PipelineConfig
from pyspark_pipeline_framework.core.quality.types import (
    CheckResult,
    CheckTiming,
    DataQualityCheck,
    FailureMode,
)
from pyspark_pipeline_framework.core.resilience.circuit_breaker import CircuitState
from pyspark_pipeline_framework.runtime.session.wrapper import SparkSessionWrapper

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when a data quality check fails in ``FAIL_ON_ERROR`` mode."""

    def __init__(self, check_name: str, result: CheckResult) -> None:
        self.check_name = check_name
        self.result = result
        super().__init__(
            f"Data quality check '{check_name}' failed: {result.message}"
        )


class DataQualityHooks:
    """Pipeline hooks that run data quality checks at lifecycle events.

    Register checks via :pymeth:`register` or :pymeth:`register_all`,
    then pass this hooks instance to the runner (typically via
    ``CompositeHooks``).

    .. note::

        The pipeline runner catches hook exceptions to prevent hooks from
        crashing the pipeline.  If a ``FAIL_ON_ERROR`` check fails, the
        ``DataQualityError`` is logged but the pipeline continues.
        Inspect :pyattr:`results` after the run to detect failures
        programmatically.

    Args:
        spark_wrapper: Provides the SparkSession for checks.
    """

    def __init__(self, spark_wrapper: SparkSessionWrapper) -> None:
        self._spark_wrapper = spark_wrapper
        self._checks: list[DataQualityCheck] = []
        self._failure_counts: dict[str, int] = {}
        self._results: list[CheckResult] = []

    def register(self, check: DataQualityCheck) -> None:
        """Register a single data quality check."""
        self._checks.append(check)

    def register_all(self, checks: list[DataQualityCheck]) -> None:
        """Register multiple data quality checks."""
        self._checks.extend(checks)

    @property
    def results(self) -> list[CheckResult]:
        """All check results from the current run."""
        return self._results.copy()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run_checks(
        self, timing: CheckTiming, component_name: str | None = None
    ) -> None:
        """Run all checks matching *timing* (and optional *component_name*)."""
        for check in self._checks:
            if check.timing != timing:
                continue
            if (
                timing == CheckTiming.AFTER_COMPONENT
                and check.component_name != component_name
            ):
                continue

            logger.info("Running DQ check: %s", check.name)

            try:
                result = check.check_fn(self._spark_wrapper.spark)
            except Exception as exc:
                result = CheckResult(
                    check_name=check.name,
                    passed=False,
                    message=f"Check raised exception: {exc}",
                )

            self._results.append(result)

            if not result.passed:
                self._handle_failure(check, result)

    def _handle_failure(
        self, check: DataQualityCheck, result: CheckResult
    ) -> None:
        """Handle a failed check according to its failure mode."""
        if check.failure_mode == FailureMode.WARN_ONLY:
            logger.warning(
                "DQ check '%s' failed (warn only): %s",
                check.name,
                result.message,
            )
            return

        if check.failure_mode == FailureMode.THRESHOLD:
            count = self._failure_counts.get(check.name, 0) + 1
            self._failure_counts[check.name] = count
            if count <= check.max_failures:
                logger.warning(
                    "DQ check '%s' failed (%d/%d): %s",
                    check.name,
                    count,
                    check.max_failures,
                    result.message,
                )
                return

        # FAIL_ON_ERROR or threshold exceeded
        raise DataQualityError(check.name, result)

    # ------------------------------------------------------------------
    # PipelineHooks protocol
    # ------------------------------------------------------------------

    def before_pipeline(self, config: PipelineConfig) -> None:
        self._results.clear()
        self._failure_counts.clear()
        self._run_checks(CheckTiming.BEFORE_PIPELINE)

    def after_pipeline(self, config: PipelineConfig, result: Any) -> None:
        self._run_checks(CheckTiming.AFTER_PIPELINE)

    def before_component(
        self, config: ComponentConfig, index: int, total: int
    ) -> None:
        pass

    def after_component(
        self, config: ComponentConfig, index: int, total: int, duration_ms: int
    ) -> None:
        self._run_checks(CheckTiming.AFTER_COMPONENT, config.name)

    def on_component_failure(
        self, config: ComponentConfig, index: int, error: Exception
    ) -> None:
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
