"""Tests for SparkSessionWrapper."""

import warnings
from unittest.mock import MagicMock, patch

import pytest

from pyspark_pipeline_framework.core.config import SparkConfig
from pyspark_pipeline_framework.runtime.session import SparkSessionWrapper


class TestSparkSessionWrapper:
    """Tests for SparkSessionWrapper."""

    def setup_method(self) -> None:
        """Reset singleton before each test."""
        SparkSessionWrapper.reset()

    def teardown_method(self) -> None:
        """Reset singleton after each test."""
        SparkSessionWrapper.reset()

    def test_default_config(self) -> None:
        """Test wrapper with default config."""
        wrapper = SparkSessionWrapper()
        assert wrapper._config.app_name == "pyspark-pipeline"

    def test_custom_config(self) -> None:
        """Test wrapper with custom config."""
        config = SparkConfig(app_name="test-app", master="local[2]")
        wrapper = SparkSessionWrapper(config)
        assert wrapper._config.app_name == "test-app"
        assert wrapper._config.master == "local[2]"

    def test_singleton_get_or_create(self) -> None:
        """Test singleton pattern."""
        wrapper1 = SparkSessionWrapper.get_or_create()
        wrapper2 = SparkSessionWrapper.get_or_create()
        assert wrapper1 is wrapper2

    def test_singleton_ignores_subsequent_config(self) -> None:
        """Test singleton ignores config after first creation."""
        config1 = SparkConfig(app_name="first-app")
        config2 = SparkConfig(app_name="second-app")
        wrapper1 = SparkSessionWrapper.get_or_create(config1)
        wrapper2 = SparkSessionWrapper.get_or_create(config2)
        assert wrapper1 is wrapper2
        assert wrapper1._config.app_name == "first-app"

    def test_reset_clears_singleton(self) -> None:
        """Test reset clears singleton."""
        wrapper1 = SparkSessionWrapper.get_or_create()
        SparkSessionWrapper.reset()
        wrapper2 = SparkSessionWrapper.get_or_create()
        assert wrapper1 is not wrapper2

    def test_is_connect_mode_true(self) -> None:
        """Test connect mode detection when connect_string is set."""
        config = SparkConfig(app_name="test", connect_string="sc://localhost:15002")
        wrapper = SparkSessionWrapper(config)
        assert wrapper.is_connect_mode is True

    def test_is_connect_mode_false(self) -> None:
        """Test connect mode detection when connect_string is not set."""
        config = SparkConfig(app_name="test")
        wrapper = SparkSessionWrapper(config)
        assert wrapper.is_connect_mode is False

    def test_spark_context_fails_in_connect_mode(self) -> None:
        """Test spark_context raises in Connect mode."""
        config = SparkConfig(app_name="test", connect_string="sc://localhost")
        wrapper = SparkSessionWrapper(config)
        with pytest.raises(RuntimeError, match="Spark Connect mode"):
            _ = wrapper.spark_context

    def test_sql_context_deprecation_warning(self) -> None:
        """Test sql_context emits deprecation warning."""
        config = SparkConfig(app_name="test", connect_string="sc://localhost")
        wrapper = SparkSessionWrapper(config)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with pytest.raises(RuntimeError):
                _ = wrapper.sql_context
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "deprecated" in str(w[0].message).lower()

    def test_sql_context_fails_in_connect_mode(self) -> None:
        """Test sql_context raises in Connect mode."""
        config = SparkConfig(app_name="test", connect_string="sc://localhost")
        wrapper = SparkSessionWrapper(config)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            with pytest.raises(RuntimeError, match="Spark Connect mode"):
                _ = wrapper.sql_context

    def test_session_injection(self) -> None:
        """Test session injection."""
        mock_session = MagicMock()
        wrapper = SparkSessionWrapper()
        wrapper.set_spark_session(mock_session)

        assert wrapper._spark is mock_session
        assert wrapper._owns_session is False

    def test_session_injection_does_not_stop_injected(self) -> None:
        """Test that stop() does not stop injected sessions."""
        mock_session = MagicMock()
        wrapper = SparkSessionWrapper()
        wrapper.set_spark_session(mock_session)
        wrapper.stop()

        mock_session.stop.assert_not_called()
        assert wrapper._spark is None

    @patch("pyspark.sql.SparkSession")
    def test_stop_stops_owned_session(self, mock_spark_class: MagicMock) -> None:
        """Test that stop() stops owned sessions."""
        mock_session = MagicMock()
        mock_spark_class.builder.config.return_value = mock_spark_class.builder
        mock_spark_class.builder.getOrCreate.return_value = mock_session

        wrapper = SparkSessionWrapper()
        _ = wrapper.spark  # Trigger session creation
        wrapper.stop()

        mock_session.stop.assert_called_once()

    def test_context_manager_returns_self(self) -> None:
        """Test context manager returns self on enter."""
        wrapper = SparkSessionWrapper()
        with wrapper as w:
            assert w is wrapper

    def test_context_manager_clears_session(self) -> None:
        """Test context manager clears session on exit."""
        mock_session = MagicMock()
        wrapper = SparkSessionWrapper()
        wrapper.set_spark_session(mock_session)

        with wrapper:
            assert wrapper._spark is mock_session

        assert wrapper._spark is None

    def test_context_manager_does_not_suppress_exceptions(self) -> None:
        """Test context manager does not suppress exceptions."""
        wrapper = SparkSessionWrapper()
        with pytest.raises(ValueError, match="test error"), wrapper:
            raise ValueError("test error")

    def test_owns_session_false_after_injection(self) -> None:
        """Test _owns_session is False after injection."""
        wrapper = SparkSessionWrapper()
        mock_session = MagicMock()
        wrapper.set_spark_session(mock_session)
        assert wrapper._owns_session is False

    @patch("pyspark.sql.SparkSession")
    def test_owns_session_true_after_creation(self, mock_spark_class: MagicMock) -> None:
        """Test _owns_session is True after lazy creation."""
        mock_session = MagicMock()
        mock_spark_class.builder.config.return_value = mock_spark_class.builder
        mock_spark_class.builder.getOrCreate.return_value = mock_session

        wrapper = SparkSessionWrapper()
        _ = wrapper.spark

        assert wrapper._owns_session is True
