"""Tests for pre-built configuration policies."""

from pyspark_pipeline_framework.core.config import (
    CircuitBreakerConfigs,
    RetryPolicies,
)


class TestRetryPolicies:
    """Tests for RetryPolicies presets."""

    def test_no_retry_is_single_attempt(self) -> None:
        """Test NO_RETRY policy has exactly 1 attempt (no retries)."""
        assert RetryPolicies.NO_RETRY.max_attempts == 1

    def test_default_values(self) -> None:
        """Test DEFAULT policy uses expected values."""
        assert RetryPolicies.DEFAULT.max_attempts == 3
        assert RetryPolicies.DEFAULT.initial_delay_seconds == 1.0
        assert RetryPolicies.DEFAULT.backoff_multiplier == 2.0

    def test_aggressive_has_more_attempts(self) -> None:
        """Test AGGRESSIVE policy has more attempts and faster retry."""
        assert RetryPolicies.AGGRESSIVE.max_attempts == 5
        assert RetryPolicies.AGGRESSIVE.initial_delay_seconds == 0.5
        assert RetryPolicies.AGGRESSIVE.backoff_multiplier == 1.5

    def test_conservative_has_longer_delays(self) -> None:
        """Test CONSERVATIVE policy has fewer attempts and longer delays."""
        assert RetryPolicies.CONSERVATIVE.max_attempts == 2
        assert RetryPolicies.CONSERVATIVE.initial_delay_seconds == 5.0
        assert RetryPolicies.CONSERVATIVE.max_delay_seconds == 30.0


class TestCircuitBreakerConfigs:
    """Tests for CircuitBreakerConfigs presets."""

    def test_default_values(self) -> None:
        """Test DEFAULT config uses expected values."""
        assert CircuitBreakerConfigs.DEFAULT.failure_threshold == 5
        assert CircuitBreakerConfigs.DEFAULT.timeout_seconds == 60.0

    def test_sensitive_trips_faster(self) -> None:
        """Test SENSITIVE config has lower threshold and longer timeout."""
        assert CircuitBreakerConfigs.SENSITIVE.failure_threshold == 3
        assert CircuitBreakerConfigs.SENSITIVE.timeout_seconds == 120.0

    def test_resilient_tolerates_more(self) -> None:
        """Test RESILIENT config has higher threshold and shorter timeout."""
        assert CircuitBreakerConfigs.RESILIENT.failure_threshold == 10
        assert CircuitBreakerConfigs.RESILIENT.timeout_seconds == 30.0
