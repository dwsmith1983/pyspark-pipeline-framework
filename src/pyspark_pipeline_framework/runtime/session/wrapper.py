"""Spark session lifecycle management."""

from __future__ import annotations

import logging
import threading
import warnings
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

from pyspark_pipeline_framework.core.config import SparkConfig

logger = logging.getLogger(__name__)


class SparkSessionWrapper:
    """Manages SparkSession lifecycle for pipeline execution.

    Supports:
    - Local mode (master="local[*]")
    - Cluster mode (master="yarn", "spark://...")
    - Spark Connect (connect_string="sc://...")

    Example:
        >>> wrapper = SparkSessionWrapper.get_or_create(config)
        >>> df = wrapper.spark.read.parquet("data.parquet")
        >>> wrapper.stop()

        # Or as context manager:
        >>> with SparkSessionWrapper(config) as wrapper:
        ...     df = wrapper.spark.read.parquet("data.parquet")
    """

    _instance: SparkSessionWrapper | None = None
    _lock = threading.Lock()

    def __init__(self, config: SparkConfig | None = None) -> None:
        """Initialize wrapper with optional config.

        Args:
            config: Spark configuration. If None, uses defaults with
                app_name="pyspark-pipeline".
        """
        if config is None:
            config = SparkConfig(app_name="pyspark-pipeline")
        self._config = config
        self._spark: SparkSession | None = None
        self._owns_session = False
        self._session_lock = threading.Lock()

    @classmethod
    def get_or_create(cls, config: SparkConfig | None = None) -> SparkSessionWrapper:
        """Get singleton instance or create new one.

        Thread-safe singleton access. First call creates the instance,
        subsequent calls return the same instance (ignoring config).

        Args:
            config: Spark configuration for first initialization.

        Returns:
            The singleton SparkSessionWrapper instance.
        """
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls(config)
            return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset singleton instance.

        Stops any owned session and clears the singleton.
        Primarily used for testing.
        """
        with cls._lock:
            if cls._instance is not None:
                cls._instance.stop()
                cls._instance = None

    @property
    def spark(self) -> SparkSession:
        """Get or create SparkSession.

        Lazily creates the session on first access. Thread-safe.

        Returns:
            Active SparkSession instance.
        """
        with self._session_lock:
            if self._spark is None:
                self._spark = self._create_session()
                self._owns_session = True
            return self._spark

    @property
    def spark_context(self) -> Any:
        """Get SparkContext from session.

        Note: Not available when using Spark Connect mode.

        Returns:
            SparkContext instance.

        Raises:
            RuntimeError: If using Spark Connect (no SparkContext available).
        """
        if self._config.connect_string:
            raise RuntimeError(
                "SparkContext is not available in Spark Connect mode. "
                "Use spark session directly for DataFrame operations."
            )
        return self.spark.sparkContext

    @property
    def sql_context(self) -> Any:
        """Get SQLContext from session.

        .. deprecated::
            SQLContext is deprecated since Spark 2.0. Use SparkSession directly.
            This property may be removed in future versions.

        Returns:
            SQLContext instance.

        Raises:
            RuntimeError: If using Spark Connect or PySpark 4.0+.
        """
        warnings.warn(
            "sql_context is deprecated. Use spark session directly.",
            DeprecationWarning,
            stacklevel=2,
        )
        if self._config.connect_string:
            raise RuntimeError("SQLContext is not available in Spark Connect mode.")
        try:
            from pyspark.sql import SQLContext

            return SQLContext(self.spark_context)
        except ImportError:
            raise RuntimeError("SQLContext is not available in PySpark 4.0+. Use SparkSession directly.") from None

    @property
    def is_connect_mode(self) -> bool:
        """Check if using Spark Connect mode."""
        return self._config.connect_string is not None

    def set_spark_session(self, spark: SparkSession) -> None:
        """Inject an existing SparkSession.

        Use when running in an existing Spark environment
        (e.g., Databricks, EMR notebooks).

        Args:
            spark: Existing SparkSession to use.
        """
        with self._session_lock:
            if self._spark is not None and self._owns_session:
                logger.warning("Replacing owned SparkSession with injected one")
                self._spark.stop()
            self._spark = spark
            self._owns_session = False

    def _create_session(self) -> SparkSession:
        """Create new SparkSession from config."""
        from pyspark.sql import SparkSession

        # Spark Connect mode
        if self._config.connect_string:
            logger.info("Connecting via Spark Connect: %s", self._config.connect_string)
            return SparkSession.builder.remote(self._config.connect_string).getOrCreate()

        # Standard mode - use to_spark_conf_dict()
        builder = SparkSession.builder

        for key, value in self._config.to_spark_conf_dict().items():
            builder = builder.config(key, value)

        logger.info(
            "Creating SparkSession: app=%s, master=%s",
            self._config.app_name,
            self._config.master,
        )
        return builder.getOrCreate()

    def stop(self) -> None:
        """Stop SparkSession if we own it."""
        with self._session_lock:
            if self._spark is not None and self._owns_session:
                logger.info("Stopping SparkSession")
                self._spark.stop()
            self._spark = None
            self._owns_session = False

    def __enter__(self) -> SparkSessionWrapper:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Exit context manager, stopping session if owned."""
        self.stop()
