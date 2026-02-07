"""Data quality check types and models."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class CheckTiming(str, Enum):
    """When to run a data quality check."""

    BEFORE_PIPELINE = "before_pipeline"
    AFTER_PIPELINE = "after_pipeline"
    AFTER_COMPONENT = "after_component"


class FailureMode(str, Enum):
    """How to handle check failures."""

    FAIL_ON_ERROR = "fail_on_error"
    WARN_ONLY = "warn_only"
    THRESHOLD = "threshold"


@dataclass
class CheckResult:
    """Result of a data quality check."""

    check_name: str
    passed: bool
    message: str
    details: dict[str, Any] | None = None


@dataclass
class DataQualityCheck:
    """A single data quality check.

    The ``check_fn`` receives a ``SparkSession`` and returns a
    ``CheckResult``.

    Args:
        name: Unique check name.
        timing: When the check should run.
        check_fn: Callable that performs the check.
        component_name: For ``AFTER_COMPONENT`` timing, the component
            to attach the check to.
        failure_mode: How to handle failures.
        max_failures: For ``THRESHOLD`` mode, number of failures
            allowed before raising.
    """

    name: str
    timing: CheckTiming
    check_fn: Callable[[SparkSession], CheckResult]
    component_name: str | None = None
    failure_mode: FailureMode = FailureMode.FAIL_ON_ERROR
    max_failures: int = 0
