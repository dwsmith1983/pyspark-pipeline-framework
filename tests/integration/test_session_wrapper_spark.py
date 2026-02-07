"""Integration tests for SparkSessionWrapper with a real SparkSession."""

from __future__ import annotations

import pytest

from pyspark_pipeline_framework.core.config.spark import SparkConfig
from pyspark_pipeline_framework.runtime.session.wrapper import SparkSessionWrapper

pytestmark = [pytest.mark.spark, pytest.mark.integration]


class TestSparkSessionWrapper:
    def setup_method(self) -> None:
        SparkSessionWrapper.reset()

    def teardown_method(self) -> None:
        SparkSessionWrapper.reset()

    def test_creates_real_session(self) -> None:
        config = SparkConfig(
            app_name="wrapper-test",
            master="local[1]",
            spark_conf={"spark.ui.enabled": "false"},
        )
        wrapper = SparkSessionWrapper(config)
        spark = wrapper.spark

        assert spark is not None
        assert spark.conf.get("spark.app.name") == "wrapper-test"

        wrapper.stop()

    def test_context_manager(self) -> None:
        config = SparkConfig(
            app_name="ctx-test",
            master="local[1]",
            spark_conf={"spark.ui.enabled": "false"},
        )
        with SparkSessionWrapper(config) as wrapper:
            df = wrapper.spark.createDataFrame([(1,), (2,)], schema=["x"])
            assert df.count() == 2

    def test_singleton_returns_same_instance(self) -> None:
        config = SparkConfig(
            app_name="singleton-test",
            master="local[1]",
            spark_conf={"spark.ui.enabled": "false"},
        )
        w1 = SparkSessionWrapper.get_or_create(config)
        w2 = SparkSessionWrapper.get_or_create(config)

        assert w1 is w2

        w1.stop()

    def test_inject_existing_session(self) -> None:
        from pyspark.sql import SparkSession

        external = (
            SparkSession.builder
            .master("local[1]")
            .appName("injected")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        try:
            wrapper = SparkSessionWrapper()
            wrapper.set_spark_session(external)

            df = wrapper.spark.createDataFrame([(42,)], schema=["val"])
            assert df.collect()[0]["val"] == 42
        finally:
            external.stop()
