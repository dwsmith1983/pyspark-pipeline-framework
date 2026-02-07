"""Retry and fault tolerance configuration models."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class RetryConfig:
    """Configuration for retry behavior.

    Implements exponential backoff with configurable parameters.
    """

    max_attempts: int = 3
    """Maximum number of retry attempts (default: 3)"""

    initial_delay_seconds: float = 1.0
    """Initial delay between retries in seconds (default: 1.0)"""

    max_delay_seconds: float = 60.0
    """Maximum delay between retries in seconds (default: 60.0)"""

    backoff_multiplier: float = 2.0
    """Multiplier for exponential backoff (default: 2.0)"""

    retry_on_exceptions: list[str] = field(default_factory=lambda: ["Exception"])
    """List of exception class names to retry on (default: ['Exception'])"""

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.initial_delay_seconds <= 0:
            raise ValueError("initial_delay_seconds must be positive")
        if self.max_delay_seconds < self.initial_delay_seconds:
            raise ValueError("max_delay_seconds must be >= initial_delay_seconds")
        if self.backoff_multiplier < 1.0:
            raise ValueError("backoff_multiplier must be >= 1.0")


@dataclass(frozen=True)
class CircuitBreakerConfig:
    """Configuration for circuit breaker pattern.

    Prevents cascading failures by failing fast when error threshold is exceeded.
    """

    failure_threshold: int = 5
    """Number of failures before opening the circuit (default: 5)"""

    success_threshold: int = 2
    """Number of successes needed to close the circuit (default: 2)"""

    timeout_seconds: float = 60.0
    """Time to wait before attempting to close the circuit (default: 60.0)"""

    half_open_max_calls: int = 1
    """Maximum calls allowed in half-open state (default: 1)"""

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")
        if self.success_threshold < 1:
            raise ValueError("success_threshold must be at least 1")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.half_open_max_calls < 1:
            raise ValueError("half_open_max_calls must be at least 1")
