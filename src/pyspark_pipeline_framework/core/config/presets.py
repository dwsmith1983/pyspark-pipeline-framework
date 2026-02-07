"""Pre-built configuration policies matching Scala equivalents.

This module provides common retry and circuit breaker configurations
as class-level constants. These match the pre-built policies from
the Scala version of this framework.

Note: These presets are instances, not factories. If you need to
modify a preset, create a new instance instead of mutating these.
"""

from pyspark_pipeline_framework.core.config.retry import CircuitBreakerConfig, RetryConfig


class RetryPolicies:
    """Pre-built retry policies for common use cases.

    Example:
        >>> from pyspark_pipeline_framework.core.config import RetryPolicies
        >>> config = RetryPolicies.AGGRESSIVE
        >>> print(config.max_attempts)  # 5
    """

    # Single attempt, no retries.
    # Note: Scala equivalent is max_retries=0; our API uses max_attempts=1
    # which has the same behavior (one try, zero retries).
    NO_RETRY: RetryConfig = RetryConfig(max_attempts=1)

    # Default retry policy: 3 attempts, 1s initial delay, 2x backoff.
    DEFAULT: RetryConfig = RetryConfig()

    # Aggressive retry: more attempts, faster initial retry.
    AGGRESSIVE: RetryConfig = RetryConfig(
        max_attempts=5,
        initial_delay_seconds=0.5,
        backoff_multiplier=1.5,
    )

    # Conservative retry: fewer attempts, longer delays.
    CONSERVATIVE: RetryConfig = RetryConfig(
        max_attempts=2,
        initial_delay_seconds=5.0,
        max_delay_seconds=30.0,
    )


class CircuitBreakerConfigs:
    """Pre-built circuit breaker configurations for common use cases.

    Example:
        >>> from pyspark_pipeline_framework.core.config import CircuitBreakerConfigs
        >>> config = CircuitBreakerConfigs.SENSITIVE
        >>> print(config.failure_threshold)  # 3
    """

    # Default circuit breaker: 5 failures, 60s timeout.
    DEFAULT: CircuitBreakerConfig = CircuitBreakerConfig()

    # Sensitive: trips faster, longer recovery time.
    SENSITIVE: CircuitBreakerConfig = CircuitBreakerConfig(
        failure_threshold=3,
        timeout_seconds=120.0,
    )

    # Resilient: tolerates more failures, recovers faster.
    RESILIENT: CircuitBreakerConfig = CircuitBreakerConfig(
        failure_threshold=10,
        timeout_seconds=30.0,
    )
