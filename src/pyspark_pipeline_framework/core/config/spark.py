"""Spark configuration models."""

from dataclasses import dataclass, field

from .base import SparkDeployMode


@dataclass
class SparkConfig:
    """Configuration for Apache Spark runtime."""

    app_name: str
    """Spark application name (required)"""

    master: str = "local[*]"
    """Spark master URL - local, yarn, k8s URL (default: local[*])"""

    deploy_mode: SparkDeployMode = SparkDeployMode.CLIENT
    """Spark deployment mode (default: client)"""

    driver_memory: str = "2g"
    """Driver memory allocation (default: 2g)"""

    driver_cores: int = 1
    """Number of cores for driver (default: 1)"""

    executor_memory: str = "4g"
    """Executor memory allocation (default: 4g)"""

    executor_cores: int = 2
    """Number of cores per executor (default: 2)"""

    num_executors: int = 2
    """Number of executors (default: 2)"""

    dynamic_allocation: bool = False
    """Enable dynamic allocation of executors (default: False)"""

    spark_conf: dict[str, str] = field(default_factory=dict)
    """Additional Spark configuration properties (default: {})"""

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if not self.app_name:
            raise ValueError("app_name is required")

        if self.driver_cores < 1:
            raise ValueError("driver_cores must be at least 1")

        if self.executor_cores < 1:
            raise ValueError("executor_cores must be at least 1")

        if self.num_executors < 1 and not self.dynamic_allocation:
            raise ValueError(
                "num_executors must be at least 1 when dynamic_allocation is False"
            )

    def to_spark_conf_dict(self) -> dict[str, str]:
        """Convert to Spark configuration dictionary.

        Returns:
            Dictionary of Spark configuration properties.
        """
        config = {
            "spark.app.name": self.app_name,
            "spark.master": self.master,
            "spark.submit.deployMode": self.deploy_mode.value,
            "spark.driver.memory": self.driver_memory,
            "spark.driver.cores": str(self.driver_cores),
            "spark.executor.memory": self.executor_memory,
            "spark.executor.cores": str(self.executor_cores),
        }

        if not self.dynamic_allocation:
            config["spark.executor.instances"] = str(self.num_executors)
        else:
            config["spark.dynamicAllocation.enabled"] = "true"

        # Merge additional spark_conf
        config.update(self.spark_conf)

        return config
