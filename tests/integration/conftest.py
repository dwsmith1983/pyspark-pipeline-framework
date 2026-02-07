"""Shared fixtures for integration tests that require a real SparkSession."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark() -> Generator[SparkSession]:
    """Provide a module-scoped local SparkSession.

    Using ``local[1]`` keeps overhead low while still exercising real
    Spark code paths.  The session is reused across all tests within a
    single module and stopped at the end.
    """
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("ppf-integration-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", "/tmp/ppf-test-warehouse")
        .getOrCreate()
    )
    yield session
    session.stop()
