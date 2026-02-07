"""Retry execution with exponential backoff and jitter."""

from __future__ import annotations

import functools
import logging
import random
import time
from collections.abc import Callable
from typing import TypeVar

from pyspark_pipeline_framework.core.config.retry import RetryConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RetryExecutor:
    """Executes callables with configurable retry logic.

    Uses exponential backoff with jitter based on a ``RetryConfig``.

    Args:
        config: Retry configuration specifying attempts, delays, and retryable exceptions.
        jitter_factor: Random jitter multiplier applied to each delay (0 disables jitter).
        sleep_func: Injectable sleep function for testing. Defaults to ``time.sleep``.
    """

    def __init__(
        self,
        config: RetryConfig,
        jitter_factor: float = 0.25,
        sleep_func: Callable[[float], None] | None = None,
    ) -> None:
        self._config = config
        self._jitter_factor = jitter_factor
        self._sleep = sleep_func or time.sleep

    @property
    def config(self) -> RetryConfig:
        """Return the retry configuration."""
        return self._config

    def calculate_delay(self, attempt: int) -> float:
        """Calculate the delay in seconds for a given attempt number.

        Uses exponential backoff: ``min(initial * multiplier^attempt, max) * (1 + jitter)``.

        Args:
            attempt: Zero-based attempt index (0 = first retry).

        Returns:
            Delay in seconds.
        """
        base = self._config.initial_delay_seconds * (
            self._config.backoff_multiplier ** attempt
        )
        base = min(base, self._config.max_delay_seconds)

        if self._jitter_factor > 0:
            jitter = base * self._jitter_factor * random.random()
            base += jitter

        return base

    def is_retryable(self, error: Exception) -> bool:
        """Check whether an exception should be retried.

        Matches against ``retry_on_exceptions`` from config. A name without a dot
        is matched against the class ``__name__``; a name with a dot is matched
        against the fully-qualified ``module.class`` path.

        Args:
            error: The exception to check.

        Returns:
            True if the exception is retryable.
        """
        error_type = type(error)
        simple_name = error_type.__name__
        qualified_name = f"{error_type.__module__}.{simple_name}"

        for exc_name in self._config.retry_on_exceptions:
            if "." in exc_name:
                if exc_name == qualified_name:
                    return True
            else:
                if exc_name == simple_name:
                    return True
                # Also match if the configured name is a parent class name
                for cls in error_type.__mro__:
                    if cls.__name__ == exc_name:
                        return True

        return False

    def execute(
        self,
        func: Callable[[], T],
        on_retry: Callable[[int, Exception, float], None] | None = None,
    ) -> T:
        """Execute a callable with retry logic.

        Args:
            func: Zero-argument callable to execute.
            on_retry: Optional callback invoked before each retry with
                ``(attempt, exception, delay)`` where attempt is 1-based.

        Returns:
            The return value of *func*.

        Raises:
            Exception: The last exception if all attempts are exhausted,
                or immediately if the exception is not retryable.
        """
        last_error: Exception | None = None

        for attempt in range(self._config.max_attempts):
            try:
                return func()
            except Exception as exc:
                last_error = exc

                is_last = attempt == self._config.max_attempts - 1
                if is_last or not self.is_retryable(exc):
                    raise

                delay = self.calculate_delay(attempt)
                logger.debug(
                    "Attempt %d/%d failed (%s), retrying in %.3fs",
                    attempt + 1,
                    self._config.max_attempts,
                    type(exc).__name__,
                    delay,
                )

                if on_retry is not None:
                    on_retry(attempt + 1, exc, delay)

                self._sleep(delay)

        # Should never reach here, but satisfies type checker
        assert last_error is not None
        raise last_error


def with_retry(
    config: RetryConfig,
    jitter_factor: float = 0.25,
    sleep_func: Callable[[float], None] | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator that wraps a function with retry logic.

    Args:
        config: Retry configuration.
        jitter_factor: Jitter multiplier (0 disables).
        sleep_func: Injectable sleep for testing.

    Returns:
        A decorator that adds retry behavior.
    """
    executor = RetryExecutor(config, jitter_factor=jitter_factor, sleep_func=sleep_func)

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: object, **kwargs: object) -> T:
            return executor.execute(lambda: func(*args, **kwargs))

        return wrapper  # type: ignore[return-value]

    return decorator
