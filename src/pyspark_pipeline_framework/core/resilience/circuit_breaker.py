"""Circuit breaker pattern for fault tolerance."""

from __future__ import annotations

import enum
import logging
import threading
import time
from collections.abc import Callable
from typing import TypeVar

from pyspark_pipeline_framework.core.config.retry import CircuitBreakerConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(enum.Enum):
    """Possible states of a circuit breaker."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpenError(Exception):
    """Raised when a call is attempted on an open circuit breaker."""

    def __init__(self, component_name: str, time_until_reset: float) -> None:
        self.component_name = component_name
        self.time_until_reset = time_until_reset
        super().__init__(f"Circuit breaker '{component_name}' is open; " f"retry after {time_until_reset:.1f}s")


class CircuitBreaker:
    """Thread-safe circuit breaker preventing cascading failures.

    State machine:
        CLOSED  --[failures >= threshold]--> OPEN
        OPEN    --[timeout elapsed]--------> HALF_OPEN
        HALF_OPEN --[successes >= threshold]--> CLOSED
        HALF_OPEN --[any failure]-----------> OPEN

    Args:
        config: Circuit breaker configuration.
        name: Identifier for this breaker instance (used in errors/logs).
        on_state_change: Optional callback ``(old_state, new_state)`` invoked on transitions.
        clock: Injectable monotonic clock for testing. Defaults to ``time.monotonic``.
    """

    def __init__(
        self,
        config: CircuitBreakerConfig,
        name: str = "default",
        on_state_change: Callable[[CircuitState, CircuitState], None] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._config = config
        self._name = name
        self._on_state_change = on_state_change
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_calls = 0
        self._opened_at: float = 0.0

    @property
    def name(self) -> str:
        """Return the breaker name."""
        return self._name

    @property
    def config(self) -> CircuitBreakerConfig:
        """Return the circuit breaker configuration."""
        return self._config

    @property
    def state(self) -> CircuitState:
        """Return the current state, checking for timeout transitions."""
        with self._lock:
            self._maybe_transition_to_half_open()
            return self._state

    @property
    def failure_count(self) -> int:
        """Return the current failure count."""
        return self._failure_count

    @property
    def success_count(self) -> int:
        """Return the current success count (relevant in HALF_OPEN)."""
        return self._success_count

    @property
    def time_until_reset(self) -> float:
        """Return seconds remaining before an OPEN breaker transitions to HALF_OPEN.

        Returns ``0.0`` when the breaker is not in the OPEN state.
        """
        with self._lock:
            if self._state is not CircuitState.OPEN:
                return 0.0
            elapsed = self._clock() - self._opened_at
            return max(0.0, self._config.timeout_seconds - elapsed)

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    def _maybe_transition_to_half_open(self) -> None:
        """Transition OPEN -> HALF_OPEN if timeout has elapsed. Caller holds lock."""
        if self._state is CircuitState.OPEN:
            elapsed = self._clock() - self._opened_at
            if elapsed >= self._config.timeout_seconds:
                self._set_state(CircuitState.HALF_OPEN)

    def _set_state(self, new_state: CircuitState) -> None:
        """Set state and fire callback. Caller holds lock."""
        old_state = self._state
        if old_state is new_state:
            return

        self._state = new_state

        if new_state is CircuitState.OPEN:
            self._opened_at = self._clock()

        if new_state is CircuitState.HALF_OPEN:
            self._success_count = 0
            self._half_open_calls = 0

        if new_state is CircuitState.CLOSED:
            self._failure_count = 0
            self._success_count = 0
            self._half_open_calls = 0

        logger.debug(
            "Circuit breaker '%s': %s -> %s",
            self._name,
            old_state.value,
            new_state.value,
        )

        if self._on_state_change is not None:
            try:
                self._on_state_change(old_state, new_state)
            except Exception:
                logger.warning(
                    "on_state_change callback raised an exception",
                    exc_info=True,
                )

    # ------------------------------------------------------------------
    # Recording outcomes
    # ------------------------------------------------------------------

    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            if self._state is CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self._config.success_threshold:
                    self._set_state(CircuitState.CLOSED)
            elif self._state is CircuitState.CLOSED:
                # Reset consecutive failure count on success
                self._failure_count = 0

    def record_failure(self) -> None:
        """Record a failed call."""
        with self._lock:
            if self._state is CircuitState.HALF_OPEN:
                self._set_state(CircuitState.OPEN)
            elif self._state is CircuitState.CLOSED:
                self._failure_count += 1
                if self._failure_count >= self._config.failure_threshold:
                    self._set_state(CircuitState.OPEN)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def call(self, func: Callable[[], T]) -> T:
        """Execute *func* through the circuit breaker.

        Args:
            func: Zero-argument callable.

        Returns:
            The return value of *func*.

        Raises:
            CircuitBreakerOpenError: If the circuit is OPEN or HALF_OPEN call limit reached.
        """
        with self._lock:
            self._maybe_transition_to_half_open()

            if self._state is CircuitState.OPEN:
                remaining = self._config.timeout_seconds - (self._clock() - self._opened_at)
                raise CircuitBreakerOpenError(self._name, max(remaining, 0.0))

            if self._state is CircuitState.HALF_OPEN:
                if self._half_open_calls >= self._config.half_open_max_calls:
                    remaining = self._config.timeout_seconds - (self._clock() - self._opened_at)
                    raise CircuitBreakerOpenError(self._name, max(remaining, 0.0))
                self._half_open_calls += 1

        # Execute outside the lock so we don't hold it during the call
        try:
            result = func()
        except Exception:
            self.record_failure()
            raise
        else:
            self.record_success()
            return result

    def reset(self) -> None:
        """Manually reset the circuit breaker to CLOSED."""
        with self._lock:
            self._set_state(CircuitState.CLOSED)
