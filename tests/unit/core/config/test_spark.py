"""Tests for Spark configuration models."""

import pytest

from pyspark_pipeline_framework.core.config.base import SparkDeployMode
from pyspark_pipeline_framework.core.config.spark import SparkConfig


class TestSparkConfig:
    """Tests for SparkConfig."""

    def test_minimal_config(self) -> None:
        """Test minimal required configuration."""
        config = SparkConfig(app_name="test-app")
        assert config.app_name == "test-app"
        assert config.master == "local[*]"
        assert config.deploy_mode == SparkDeployMode.CLIENT
        assert config.driver_memory == "2g"
        assert config.driver_cores == 1
        assert config.executor_memory == "4g"
        assert config.executor_cores == 2
        assert config.num_executors == 2
        assert config.dynamic_allocation is False
        assert config.spark_conf == {}

    def test_custom_config(self) -> None:
        """Test custom configuration values."""
        config = SparkConfig(
            app_name="custom-app",
            master="yarn",
            deploy_mode=SparkDeployMode.CLUSTER,
            driver_memory="4g",
            driver_cores=2,
            executor_memory="8g",
            executor_cores=4,
            num_executors=10,
            dynamic_allocation=False,
            spark_conf={"spark.sql.shuffle.partitions": "200"},
        )
        assert config.app_name == "custom-app"
        assert config.master == "yarn"
        assert config.deploy_mode == SparkDeployMode.CLUSTER
        assert config.driver_memory == "4g"
        assert config.driver_cores == 2
        assert config.executor_memory == "8g"
        assert config.executor_cores == 4
        assert config.num_executors == 10
        assert config.dynamic_allocation is False
        assert config.spark_conf == {"spark.sql.shuffle.partitions": "200"}

    def test_dynamic_allocation(self) -> None:
        """Test dynamic allocation configuration."""
        config = SparkConfig(app_name="test-app", dynamic_allocation=True)
        assert config.dynamic_allocation is True

    def test_validation_app_name(self) -> None:
        """Test validation for app_name."""
        with pytest.raises(ValueError, match="app_name is required"):
            SparkConfig(app_name="")

    def test_validation_driver_cores(self) -> None:
        """Test validation for driver_cores."""
        with pytest.raises(ValueError, match="driver_cores must be at least 1"):
            SparkConfig(app_name="test-app", driver_cores=0)

    def test_validation_executor_cores(self) -> None:
        """Test validation for executor_cores."""
        with pytest.raises(ValueError, match="executor_cores must be at least 1"):
            SparkConfig(app_name="test-app", executor_cores=0)

    def test_validation_num_executors(self) -> None:
        """Test validation for num_executors without dynamic allocation."""
        with pytest.raises(
            ValueError,
            match="num_executors must be at least 1 when dynamic_allocation is False",
        ):
            SparkConfig(app_name="test-app", num_executors=0, dynamic_allocation=False)

    def test_validation_num_executors_with_dynamic_allocation(self) -> None:
        """Test that num_executors validation is skipped with dynamic allocation."""
        # Should not raise an error
        config = SparkConfig(app_name="test-app", num_executors=0, dynamic_allocation=True)
        assert config.num_executors == 0
        assert config.dynamic_allocation is True

    def test_to_spark_conf_dict_basic(self) -> None:
        """Test conversion to Spark configuration dictionary."""
        config = SparkConfig(app_name="test-app")
        spark_conf = config.to_spark_conf_dict()

        assert spark_conf["spark.app.name"] == "test-app"
        assert spark_conf["spark.master"] == "local[*]"
        assert spark_conf["spark.submit.deployMode"] == "client"
        assert spark_conf["spark.driver.memory"] == "2g"
        assert spark_conf["spark.driver.cores"] == "1"
        assert spark_conf["spark.executor.memory"] == "4g"
        assert spark_conf["spark.executor.cores"] == "2"
        assert spark_conf["spark.executor.instances"] == "2"
        assert "spark.dynamicAllocation.enabled" not in spark_conf

    def test_to_spark_conf_dict_dynamic_allocation(self) -> None:
        """Test Spark config dict with dynamic allocation enabled."""
        config = SparkConfig(app_name="test-app", dynamic_allocation=True)
        spark_conf = config.to_spark_conf_dict()

        assert spark_conf["spark.dynamicAllocation.enabled"] == "true"
        assert "spark.executor.instances" not in spark_conf

    def test_to_spark_conf_dict_with_custom_conf(self) -> None:
        """Test Spark config dict with custom configuration."""
        config = SparkConfig(
            app_name="test-app",
            spark_conf={
                "spark.sql.shuffle.partitions": "200",
                "spark.executor.memoryOverhead": "1g",
            },
        )
        spark_conf = config.to_spark_conf_dict()

        assert spark_conf["spark.sql.shuffle.partitions"] == "200"
        assert spark_conf["spark.executor.memoryOverhead"] == "1g"
        assert spark_conf["spark.app.name"] == "test-app"
