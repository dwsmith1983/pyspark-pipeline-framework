"""Tests for RetryExecutor and with_retry decorator."""

from __future__ import annotations

import random
from typing import Any
from unittest.mock import MagicMock

import pytest

from pyspark_pipeline_framework.core.config.retry import RetryConfig
from pyspark_pipeline_framework.core.resilience.retry import RetryExecutor, with_retry


# ---------------------------------------------------------------------------
# Parameterized: exponential backoff delay table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("initial", "multiplier", "attempt", "expected"),
    [
        (1.0, 2.0, 0, 1.0),
        (1.0, 2.0, 1, 2.0),
        (1.0, 2.0, 2, 4.0),
        (1.0, 2.0, 3, 8.0),
        (0.5, 3.0, 0, 0.5),
        (0.5, 3.0, 1, 1.5),
        (0.5, 3.0, 2, 4.5),
    ],
    ids=[
        "1x2^0=1",
        "1x2^1=2",
        "1x2^2=4",
        "1x2^3=8",
        "0.5x3^0=0.5",
        "0.5x3^1=1.5",
        "0.5x3^2=4.5",
    ],
)
def test_exponential_backoff_parametrized(
    initial: float, multiplier: float, attempt: int, expected: float
) -> None:
    config = RetryConfig(
        initial_delay_seconds=initial,
        backoff_multiplier=multiplier,
        max_delay_seconds=100.0,
    )
    executor = RetryExecutor(config, jitter_factor=0.0)
    assert executor.calculate_delay(attempt) == expected


class TestCalculateDelay:
    """Tests for RetryExecutor.calculate_delay."""

    def test_exponential_backoff(self) -> None:
        """Delay increases exponentially with attempt number."""
        config = RetryConfig(
            initial_delay_seconds=1.0,
            backoff_multiplier=2.0,
            max_delay_seconds=60.0,
        )
        executor = RetryExecutor(config, jitter_factor=0.0)

        assert executor.calculate_delay(0) == 1.0
        assert executor.calculate_delay(1) == 2.0
        assert executor.calculate_delay(2) == 4.0
        assert executor.calculate_delay(3) == 8.0

    def test_capped_at_max_delay(self) -> None:
        """Delay never exceeds max_delay_seconds."""
        config = RetryConfig(
            initial_delay_seconds=1.0,
            backoff_multiplier=10.0,
            max_delay_seconds=5.0,
        )
        executor = RetryExecutor(config, jitter_factor=0.0)

        # 1.0 * 10^3 = 1000, but capped at 5
        assert executor.calculate_delay(3) == 5.0

    def test_with_jitter(self) -> None:
        """Jitter adds a random component to the delay."""
        config = RetryConfig(
            initial_delay_seconds=1.0,
            backoff_multiplier=2.0,
            max_delay_seconds=60.0,
        )
        executor = RetryExecutor(config, jitter_factor=0.25)

        # Seed for determinism
        random.seed(42)
        delay = executor.calculate_delay(0)
        # base=1.0, jitter = 1.0 * 0.25 * random() -> delay > 1.0
        assert delay >= 1.0
        assert delay <= 1.25

    def test_zero_jitter_gives_exact_values(self) -> None:
        """jitter_factor=0 produces exact exponential values."""
        config = RetryConfig(
            initial_delay_seconds=0.5,
            backoff_multiplier=3.0,
            max_delay_seconds=100.0,
        )
        executor = RetryExecutor(config, jitter_factor=0.0)

        assert executor.calculate_delay(0) == 0.5
        assert executor.calculate_delay(1) == 1.5
        assert executor.calculate_delay(2) == 4.5


class TestIsRetryable:
    """Tests for RetryExecutor.is_retryable."""

    def test_default_matches_any_exception(self) -> None:
        """Default config ['Exception'] matches any exception via MRO."""
        config = RetryConfig()  # retry_on_exceptions=["Exception"]
        executor = RetryExecutor(config)

        assert executor.is_retryable(ValueError("oops"))
        assert executor.is_retryable(RuntimeError("fail"))
        assert executor.is_retryable(IOError("io"))

    def test_specific_simple_name(self) -> None:
        """Simple name (no dot) matches class __name__."""
        config = RetryConfig(retry_on_exceptions=["ValueError"])
        executor = RetryExecutor(config)

        assert executor.is_retryable(ValueError("x"))
        assert not executor.is_retryable(TypeError("x"))

    def test_qualified_name(self) -> None:
        """Fully-qualified name matches module.class."""
        config = RetryConfig(retry_on_exceptions=["builtins.ValueError"])
        executor = RetryExecutor(config)

        assert executor.is_retryable(ValueError("x"))
        assert not executor.is_retryable(TypeError("x"))

    def test_rejects_non_matching(self) -> None:
        """Returns False for unlisted exception types."""
        config = RetryConfig(retry_on_exceptions=["KeyError"])
        executor = RetryExecutor(config)

        assert not executor.is_retryable(ValueError("nope"))
        assert not executor.is_retryable(RuntimeError("nope"))


class TestExecute:
    """Tests for RetryExecutor.execute."""

    def test_succeeds_first_try(self) -> None:
        """No retries when the function succeeds immediately."""
        config = RetryConfig(max_attempts=3)
        sleep = MagicMock()
        executor = RetryExecutor(config, sleep_func=sleep)

        result = executor.execute(lambda: 42)

        assert result == 42
        sleep.assert_not_called()

    def test_retries_on_failure_then_succeeds(self) -> None:
        """Retries transient failures and returns on success."""
        config = RetryConfig(max_attempts=3, initial_delay_seconds=0.1)
        sleep = MagicMock()
        executor = RetryExecutor(config, jitter_factor=0.0, sleep_func=sleep)

        call_count = 0

        def flaky() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("transient")
            return "ok"

        result = executor.execute(flaky)

        assert result == "ok"
        assert call_count == 3
        assert sleep.call_count == 2

    def test_exhausts_retries(self) -> None:
        """Raises last exception when all attempts are exhausted."""
        config = RetryConfig(max_attempts=2, initial_delay_seconds=0.1)
        sleep = MagicMock()
        executor = RetryExecutor(config, jitter_factor=0.0, sleep_func=sleep)

        with pytest.raises(ValueError, match="always fails"):
            executor.execute(lambda: (_ for _ in ()).throw(ValueError("always fails")))

    def test_non_retryable_raises_immediately(self) -> None:
        """Non-retryable exceptions are raised without retry."""
        config = RetryConfig(
            max_attempts=5,
            retry_on_exceptions=["IOError"],
        )
        sleep = MagicMock()
        executor = RetryExecutor(config, sleep_func=sleep)

        with pytest.raises(ValueError, match="not retryable"):
            executor.execute(
                lambda: (_ for _ in ()).throw(ValueError("not retryable"))
            )

        sleep.assert_not_called()

    def test_on_retry_callback(self) -> None:
        """on_retry callback receives (attempt, exception, delay)."""
        config = RetryConfig(max_attempts=3, initial_delay_seconds=1.0)
        sleep = MagicMock()
        on_retry = MagicMock()
        executor = RetryExecutor(config, jitter_factor=0.0, sleep_func=sleep)

        call_count = 0

        def fail_twice() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("err")
            return "done"

        executor.execute(fail_twice, on_retry=on_retry)

        assert on_retry.call_count == 2
        # First callback: attempt=1, ValueError, delay=1.0
        args1 = on_retry.call_args_list[0][0]
        assert args1[0] == 1
        assert isinstance(args1[1], ValueError)
        assert args1[2] == 1.0
        # Second callback: attempt=2, ValueError, delay=2.0
        args2 = on_retry.call_args_list[1][0]
        assert args2[0] == 2
        assert args2[2] == 2.0


class TestWithRetryDecorator:
    """Tests for the with_retry decorator."""

    def test_decorator_wraps_function(self) -> None:
        """Decorated function retries on failure."""
        config = RetryConfig(max_attempts=3, initial_delay_seconds=0.01)
        sleep = MagicMock()

        call_count = 0

        @with_retry(config, jitter_factor=0.0, sleep_func=sleep)
        def flaky_func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("boom")
            return x * 2

        result = flaky_func(5)
        assert result == 10
        assert call_count == 2

    def test_decorator_preserves_function_name(self) -> None:
        """Decorated function preserves __name__ via functools.wraps."""
        config = RetryConfig()

        @with_retry(config)
        def my_function() -> None:
            pass

        assert my_function.__name__ == "my_function"
