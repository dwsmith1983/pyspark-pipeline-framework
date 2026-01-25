"""Tests for retry configuration models."""

import pytest

from pyspark_pipeline_framework.core.config.retry import (
    CircuitBreakerConfig,
    RetryConfig,
)


class TestRetryConfig:
    """Tests for RetryConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.initial_delay_seconds == 1.0
        assert config.max_delay_seconds == 60.0
        assert config.backoff_multiplier == 2.0
        assert config.retry_on_exceptions == ["Exception"]

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = RetryConfig(
            max_attempts=5,
            initial_delay_seconds=2.0,
            max_delay_seconds=120.0,
            backoff_multiplier=3.0,
            retry_on_exceptions=["IOError", "TimeoutError"],
        )
        assert config.max_attempts == 5
        assert config.initial_delay_seconds == 2.0
        assert config.max_delay_seconds == 120.0
        assert config.backoff_multiplier == 3.0
        assert config.retry_on_exceptions == ["IOError", "TimeoutError"]

    def test_validation_max_attempts(self) -> None:
        """Test validation for max_attempts."""
        with pytest.raises(ValueError, match="max_attempts must be at least 1"):
            RetryConfig(max_attempts=0)

    def test_validation_initial_delay(self) -> None:
        """Test validation for initial_delay_seconds."""
        with pytest.raises(ValueError, match="initial_delay_seconds must be positive"):
            RetryConfig(initial_delay_seconds=0)

        with pytest.raises(ValueError, match="initial_delay_seconds must be positive"):
            RetryConfig(initial_delay_seconds=-1.0)

    def test_validation_max_delay(self) -> None:
        """Test validation for max_delay_seconds."""
        with pytest.raises(ValueError, match="max_delay_seconds must be >= initial_delay_seconds"):
            RetryConfig(initial_delay_seconds=10.0, max_delay_seconds=5.0)

    def test_validation_backoff_multiplier(self) -> None:
        """Test validation for backoff_multiplier."""
        with pytest.raises(ValueError, match=r"backoff_multiplier must be >= 1.0"):
            RetryConfig(backoff_multiplier=0.5)


class TestCircuitBreakerConfig:
    """Tests for CircuitBreakerConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = CircuitBreakerConfig()
        assert config.failure_threshold == 5
        assert config.success_threshold == 2
        assert config.timeout_seconds == 60.0
        assert config.half_open_max_calls == 1

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = CircuitBreakerConfig(
            failure_threshold=10,
            success_threshold=3,
            timeout_seconds=120.0,
            half_open_max_calls=2,
        )
        assert config.failure_threshold == 10
        assert config.success_threshold == 3
        assert config.timeout_seconds == 120.0
        assert config.half_open_max_calls == 2

    def test_validation_failure_threshold(self) -> None:
        """Test validation for failure_threshold."""
        with pytest.raises(ValueError, match="failure_threshold must be at least 1"):
            CircuitBreakerConfig(failure_threshold=0)

    def test_validation_success_threshold(self) -> None:
        """Test validation for success_threshold."""
        with pytest.raises(ValueError, match="success_threshold must be at least 1"):
            CircuitBreakerConfig(success_threshold=0)

    def test_validation_timeout(self) -> None:
        """Test validation for timeout_seconds."""
        with pytest.raises(ValueError, match="timeout_seconds must be positive"):
            CircuitBreakerConfig(timeout_seconds=0)

        with pytest.raises(ValueError, match="timeout_seconds must be positive"):
            CircuitBreakerConfig(timeout_seconds=-1.0)

    def test_validation_half_open_max_calls(self) -> None:
        """Test validation for half_open_max_calls."""
        with pytest.raises(ValueError, match="half_open_max_calls must be at least 1"):
            CircuitBreakerConfig(half_open_max_calls=0)
