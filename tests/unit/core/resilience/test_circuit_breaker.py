"""Tests for CircuitBreaker, CircuitState, and CircuitBreakerOpenError."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pyspark_pipeline_framework.core.config.retry import CircuitBreakerConfig
from pyspark_pipeline_framework.core.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
)


class TestCircuitState:
    """Tests for initial state."""

    def test_initial_state_closed(self) -> None:
        """New circuit breaker starts in CLOSED state."""
        config = CircuitBreakerConfig()
        cb = CircuitBreaker(config)
        assert cb.state is CircuitState.CLOSED


class TestStateTransitions:
    """Tests for circuit breaker state transitions."""

    def test_closed_to_open_on_failures(self) -> None:
        """CLOSED -> OPEN after failure_threshold consecutive failures."""
        config = CircuitBreakerConfig(failure_threshold=3)
        cb = CircuitBreaker(config)

        for _ in range(3):
            cb.record_failure()

        assert cb.state is CircuitState.OPEN

    def test_open_to_half_open_on_timeout(self) -> None:
        """OPEN -> HALF_OPEN after timeout_seconds elapsed."""
        current_time = 0.0

        def clock() -> float:
            return current_time

        config = CircuitBreakerConfig(failure_threshold=1, timeout_seconds=10.0)
        cb = CircuitBreaker(config, clock=clock)

        cb.record_failure()
        assert cb.state is CircuitState.OPEN

        # Not enough time
        current_time = 9.0
        assert cb.state is CircuitState.OPEN

        # Enough time
        current_time = 10.0
        assert cb.state is CircuitState.HALF_OPEN

    def test_half_open_to_closed_on_successes(self) -> None:
        """HALF_OPEN -> CLOSED after success_threshold successes."""
        current_time = 0.0

        def clock() -> float:
            return current_time

        config = CircuitBreakerConfig(
            failure_threshold=1,
            success_threshold=2,
            timeout_seconds=5.0,
            half_open_max_calls=5,
        )
        cb = CircuitBreaker(config, clock=clock)

        # Trip to OPEN
        cb.record_failure()
        assert cb.state is CircuitState.OPEN

        # Wait for timeout -> HALF_OPEN
        current_time = 5.0
        assert cb.state is CircuitState.HALF_OPEN

        # Record successes
        cb.record_success()
        assert cb.state is CircuitState.HALF_OPEN
        cb.record_success()
        assert cb.state is CircuitState.CLOSED

    def test_half_open_to_open_on_failure(self) -> None:
        """HALF_OPEN -> OPEN on any single failure."""
        current_time = 0.0

        def clock() -> float:
            return current_time

        config = CircuitBreakerConfig(
            failure_threshold=1,
            timeout_seconds=5.0,
        )
        cb = CircuitBreaker(config, clock=clock)

        cb.record_failure()
        current_time = 5.0
        assert cb.state is CircuitState.HALF_OPEN

        cb.record_failure()
        assert cb.state is CircuitState.OPEN


class TestCall:
    """Tests for CircuitBreaker.call."""

    def test_call_succeeds_when_closed(self) -> None:
        """Calls pass through when circuit is CLOSED."""
        config = CircuitBreakerConfig()
        cb = CircuitBreaker(config)

        result = cb.call(lambda: 42)
        assert result == 42

    def test_call_raises_when_open(self) -> None:
        """Calls raise CircuitBreakerOpenError when circuit is OPEN."""
        config = CircuitBreakerConfig(failure_threshold=1, timeout_seconds=30.0)
        current_time = 0.0

        def clock() -> float:
            return current_time

        cb = CircuitBreaker(config, name="test-svc", clock=clock)

        # Trip to OPEN
        with pytest.raises(ValueError):
            cb.call(lambda: (_ for _ in ()).throw(ValueError("fail")))

        # Now OPEN - should reject
        with pytest.raises(CircuitBreakerOpenError, match="test-svc"):
            cb.call(lambda: 1)

    def test_call_records_success(self) -> None:
        """Successful calls are auto-recorded."""
        config = CircuitBreakerConfig(failure_threshold=3)
        cb = CircuitBreaker(config)

        # Record 2 failures then a success
        cb.record_failure()
        cb.record_failure()
        assert cb.failure_count == 2

        cb.call(lambda: "ok")
        # Success resets failure count in CLOSED state
        assert cb.failure_count == 0

    def test_call_records_failure_and_reraises(self) -> None:
        """Failed calls are recorded and the exception is re-raised."""
        config = CircuitBreakerConfig(failure_threshold=5)
        cb = CircuitBreaker(config)

        with pytest.raises(RuntimeError, match="boom"):
            cb.call(lambda: (_ for _ in ()).throw(RuntimeError("boom")))

        assert cb.failure_count == 1

    def test_half_open_max_calls(self) -> None:
        """Excess calls in HALF_OPEN are rejected."""
        current_time = 0.0

        def clock() -> float:
            return current_time

        config = CircuitBreakerConfig(
            failure_threshold=1,
            timeout_seconds=5.0,
            half_open_max_calls=1,
            success_threshold=2,
        )
        cb = CircuitBreaker(config, name="throttled", clock=clock)

        # Trip to OPEN
        with pytest.raises(ValueError):
            cb.call(lambda: (_ for _ in ()).throw(ValueError("trip")))

        # Wait for timeout -> HALF_OPEN
        current_time = 5.0

        # First call allowed
        cb.call(lambda: "probe")

        # Second call rejected (max_calls=1 already used)
        with pytest.raises(CircuitBreakerOpenError, match="throttled"):
            cb.call(lambda: "too many")


class TestCallbacks:
    """Tests for on_state_change callback."""

    def test_on_state_change_callback(self) -> None:
        """Callback invoked with (old, new) on state transitions."""
        callback = MagicMock()
        config = CircuitBreakerConfig(failure_threshold=2)
        cb = CircuitBreaker(config, on_state_change=callback)

        cb.record_failure()
        cb.record_failure()

        callback.assert_called_once_with(CircuitState.CLOSED, CircuitState.OPEN)

    def test_on_state_change_callback_error_swallowed(self) -> None:
        """A failing callback does not break the circuit breaker."""

        def bad_callback(old: CircuitState, new: CircuitState) -> None:
            raise RuntimeError("callback exploded")

        config = CircuitBreakerConfig(failure_threshold=1)
        cb = CircuitBreaker(config, on_state_change=bad_callback)

        # Should not raise despite callback failure
        cb.record_failure()
        assert cb.state is CircuitState.OPEN


class TestReset:
    """Tests for manual reset."""

    def test_reset(self) -> None:
        """reset() returns breaker to CLOSED with cleared counters."""
        config = CircuitBreakerConfig(failure_threshold=1)
        cb = CircuitBreaker(config)

        cb.record_failure()
        assert cb.state is CircuitState.OPEN

        cb.reset()
        assert cb.state is CircuitState.CLOSED
        assert cb.failure_count == 0
        assert cb.success_count == 0


class TestTimeUntilReset:
    """Tests for the time_until_reset property."""

    def test_returns_zero_when_closed(self) -> None:
        """CLOSED state has no time until reset."""
        cb = CircuitBreaker(CircuitBreakerConfig())
        assert cb.time_until_reset == 0.0

    def test_returns_remaining_time_when_open(self) -> None:
        """OPEN state returns seconds remaining until HALF_OPEN transition."""
        current_time = 0.0

        def clock() -> float:
            return current_time

        config = CircuitBreakerConfig(failure_threshold=1, timeout_seconds=30.0)
        cb = CircuitBreaker(config, clock=clock)

        cb.record_failure()
        assert cb.state is CircuitState.OPEN

        current_time = 10.0
        assert cb.time_until_reset == pytest.approx(20.0)

        current_time = 29.5
        assert cb.time_until_reset == pytest.approx(0.5)

    def test_returns_zero_when_timeout_exceeded(self) -> None:
        """Returns 0.0 when timeout has already elapsed (about to transition)."""
        current_time = 0.0

        def clock() -> float:
            return current_time

        config = CircuitBreakerConfig(failure_threshold=1, timeout_seconds=10.0)
        cb = CircuitBreaker(config, clock=clock)

        cb.record_failure()
        current_time = 15.0
        # state property would transition to HALF_OPEN, but time_until_reset
        # checks raw state under lock without triggering transition
        assert cb.time_until_reset == 0.0

    def test_returns_zero_when_half_open(self) -> None:
        """HALF_OPEN state has no time until reset."""
        current_time = 0.0

        def clock() -> float:
            return current_time

        config = CircuitBreakerConfig(failure_threshold=1, timeout_seconds=5.0)
        cb = CircuitBreaker(config, clock=clock)

        cb.record_failure()
        current_time = 5.0
        _ = cb.state  # trigger transition to HALF_OPEN
        assert cb.time_until_reset == 0.0


class TestCircuitBreakerOpenError:
    """Tests for the error class itself."""

    def test_error_attrs(self) -> None:
        """Error exposes component_name and time_until_reset."""
        err = CircuitBreakerOpenError("my-service", 12.5)

        assert err.component_name == "my-service"
        assert err.time_until_reset == 12.5
        assert "my-service" in str(err)
        assert "12.5" in str(err)
