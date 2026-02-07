"""DataFlow base class for Spark-aware pipeline components."""

from __future__ import annotations

import logging
from abc import abstractmethod
from typing import TYPE_CHECKING

from pyspark_pipeline_framework.core.component.base import PipelineComponent

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class DataFlow(PipelineComponent):
    """Base class for Spark-aware pipeline components.

    Extends PipelineComponent with SparkSession access. The runner
    injects a session via ``set_spark_session()`` before calling ``run()``.

    DataFlow does NOT create or manage sessions. Session lifecycle is
    handled by ``SparkSessionWrapper`` in the runner layer.

    Example:
        >>> class MyTransform(DataFlow):
        ...     @property
        ...     def name(self) -> str:
        ...         return "my-transform"
        ...
        ...     def run(self) -> None:
        ...         df = self.spark.read.parquet("input.parquet")
        ...         df.write.parquet("output.parquet")
    """

    def __init__(self) -> None:
        self._spark_session: SparkSession | None = None
        self._logger: logging.Logger | None = None

    @property
    def spark(self) -> SparkSession:
        """Access the injected SparkSession.

        Returns:
            The SparkSession injected by the runner.

        Raises:
            RuntimeError: If no session has been injected yet.
        """
        if self._spark_session is None:
            raise RuntimeError(
                f"SparkSession not available for component '{self.name}'. "
                "Ensure the runner calls set_spark_session() before run()."
            )
        return self._spark_session

    @property
    def logger(self) -> logging.Logger:
        """Component-scoped logger.

        Returns a logger named ``ppf.component.{name}``, created lazily
        and cached for the lifetime of the instance.
        """
        if self._logger is None:
            self._logger = logging.getLogger(f"ppf.component.{self.name}")
        return self._logger

    def set_spark_session(self, spark: SparkSession) -> None:
        """Inject a SparkSession for this component.

        Called by the pipeline runner before ``run()``.

        Args:
            spark: Active SparkSession instance.
        """
        self._spark_session = spark

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable component name for logging."""
        ...

    @abstractmethod
    def run(self) -> None:
        """Execute the component's main logic."""
        ...
