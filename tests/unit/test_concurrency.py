"""Concurrency tests for thread-safe components.

Validates that CircuitBreaker, SecretsCache, and InMemoryRegistry
behave correctly under high contention from multiple threads.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

from pyspark_pipeline_framework.core.config.retry import CircuitBreakerConfig
from pyspark_pipeline_framework.core.metrics.registry import InMemoryRegistry
from pyspark_pipeline_framework.core.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitState,
)
from pyspark_pipeline_framework.core.secrets.base import (
    SecretResolutionResult,
    SecretResolutionStatus,
    SecretsProvider,
    SecretsReference,
)
from pyspark_pipeline_framework.core.secrets.resolver import (
    SecretsCache,
    SecretsResolver,
)

THREADS = 8
ITERATIONS = 500


class TestCircuitBreakerConcurrency:
    """Concurrent access to CircuitBreaker under high contention."""

    def test_concurrent_record_failure_reaches_open(self) -> None:
        """Multiple threads recording failures should trip the breaker exactly once."""
        config = CircuitBreakerConfig(failure_threshold=THREADS * ITERATIONS)
        cb = CircuitBreaker(config)
        barrier = threading.Barrier(THREADS)

        def record_failures() -> None:
            barrier.wait()
            for _ in range(ITERATIONS):
                cb.record_failure()

        threads = [threading.Thread(target=record_failures) for _ in range(THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert cb.failure_count == THREADS * ITERATIONS
        assert cb.state is CircuitState.OPEN

    def test_concurrent_record_success_resets_failures(self) -> None:
        """Concurrent successes in CLOSED state reset failure count atomically."""
        config = CircuitBreakerConfig(failure_threshold=100)
        cb = CircuitBreaker(config)
        # Pre-load some failures
        for _ in range(50):
            cb.record_failure()
        assert cb.failure_count == 50

        barrier = threading.Barrier(THREADS)

        def record_successes() -> None:
            barrier.wait()
            for _ in range(ITERATIONS):
                cb.record_success()

        threads = [threading.Thread(target=record_successes) for _ in range(THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # After any success in CLOSED, failure_count resets to 0
        assert cb.failure_count == 0
        assert cb.state is CircuitState.CLOSED

    def test_concurrent_state_reads(self) -> None:
        """Reading state from multiple threads does not deadlock."""
        current_time = 0.0
        lock = threading.Lock()

        def clock() -> float:
            with lock:
                return current_time

        config = CircuitBreakerConfig(failure_threshold=1, timeout_seconds=5.0)
        cb = CircuitBreaker(config, clock=clock)
        cb.record_failure()

        barrier = threading.Barrier(THREADS)
        states: list[CircuitState] = []
        states_lock = threading.Lock()

        def read_state() -> None:
            barrier.wait()
            for _ in range(ITERATIONS):
                s = cb.state
                with states_lock:
                    states.append(s)

        threads = [threading.Thread(target=read_state) for _ in range(THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(states) == THREADS * ITERATIONS
        # All reads should see OPEN (time hasn't advanced)
        assert all(s is CircuitState.OPEN for s in states)

    def test_concurrent_call_method(self) -> None:
        """call() under contention records successes correctly."""
        config = CircuitBreakerConfig(failure_threshold=100)
        cb = CircuitBreaker(config)
        counter = {"value": 0}
        counter_lock = threading.Lock()
        barrier = threading.Barrier(THREADS)

        def do_calls() -> None:
            barrier.wait()
            for _ in range(ITERATIONS):
                def fn() -> int:
                    with counter_lock:
                        counter["value"] += 1
                    return 1
                cb.call(fn)

        threads = [threading.Thread(target=do_calls) for _ in range(THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert counter["value"] == THREADS * ITERATIONS
        assert cb.state is CircuitState.CLOSED


class TestSecretsCacheConcurrency:
    """Concurrent access to SecretsCache under high contention."""

    def _mock_resolver(self) -> SecretsResolver:
        provider = MagicMock(spec=SecretsProvider)
        provider.provider_name = "env"
        call_count = {"n": 0}

        def resolve_side(ref: SecretsReference) -> SecretResolutionResult:
            call_count["n"] += 1
            return SecretResolutionResult(
                reference=ref,
                status=SecretResolutionStatus.SUCCESS,
                value=f"val-{call_count['n']}",
            )

        provider.resolve.side_effect = resolve_side
        resolver = SecretsResolver()
        resolver.register(provider)
        return resolver

    def test_concurrent_resolve_same_key(self) -> None:
        """Multiple threads resolving the same key should share cached value."""
        resolver = self._mock_resolver()
        t = 100.0
        cache = SecretsCache(resolver, ttl_seconds=300, clock=lambda: t)
        ref = SecretsReference(provider="env", key="SHARED")
        barrier = threading.Barrier(THREADS)
        results: list[str | None] = []
        results_lock = threading.Lock()

        def resolve_many() -> None:
            barrier.wait()
            for _ in range(ITERATIONS):
                result = cache.resolve(ref)
                with results_lock:
                    results.append(result.value)

        threads = [threading.Thread(target=resolve_many) for _ in range(THREADS)]
        for t_thread in threads:
            t_thread.start()
        for t_thread in threads:
            t_thread.join()

        assert len(results) == THREADS * ITERATIONS
        # All results should be the same cached value (first resolution)
        unique_values = set(results)
        assert len(unique_values) == 1

    def test_concurrent_resolve_different_keys(self) -> None:
        """Different keys are cached independently under contention."""
        resolver = self._mock_resolver()
        t = 100.0
        cache = SecretsCache(resolver, ttl_seconds=300, clock=lambda: t)
        barrier = threading.Barrier(THREADS)

        def resolve_unique(thread_id: int) -> None:
            barrier.wait()
            ref = SecretsReference(provider="env", key=f"KEY_{thread_id}")
            for _ in range(ITERATIONS):
                result = cache.resolve(ref)
                assert result.status == SecretResolutionStatus.SUCCESS

        threads = [
            threading.Thread(target=resolve_unique, args=(i,))
            for i in range(THREADS)
        ]
        for t_thread in threads:
            t_thread.start()
        for t_thread in threads:
            t_thread.join()

    def test_concurrent_resolve_and_clear(self) -> None:
        """clear() during concurrent resolves does not deadlock or corrupt state."""
        resolver = self._mock_resolver()
        t = 100.0
        cache = SecretsCache(resolver, ttl_seconds=300, clock=lambda: t)
        ref = SecretsReference(provider="env", key="CLEARABLE")
        barrier = threading.Barrier(THREADS + 1)  # +1 for clearer thread

        def resolve_loop() -> None:
            barrier.wait()
            for _ in range(ITERATIONS):
                result = cache.resolve(ref)
                assert result.status == SecretResolutionStatus.SUCCESS

        def clear_loop() -> None:
            barrier.wait()
            for _ in range(ITERATIONS // 10):
                cache.clear()

        threads = [threading.Thread(target=resolve_loop) for _ in range(THREADS)]
        threads.append(threading.Thread(target=clear_loop))
        for t_thread in threads:
            t_thread.start()
        for t_thread in threads:
            t_thread.join()


class TestInMemoryRegistryConcurrency:
    """Concurrent access to InMemoryRegistry under high contention."""

    def test_concurrent_counter_increments_are_exact(self) -> None:
        """Counter value is exact after concurrent increments."""
        reg = InMemoryRegistry()
        barrier = threading.Barrier(THREADS)

        def increment() -> None:
            barrier.wait()
            for _ in range(ITERATIONS):
                reg.counter("requests")

        threads = [threading.Thread(target=increment) for _ in range(THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert reg.get_counter("requests") == THREADS * ITERATIONS

    def test_concurrent_gauge_sets(self) -> None:
        """Gauge ends up with a valid value after concurrent sets."""
        reg = InMemoryRegistry()
        barrier = threading.Barrier(THREADS)

        def set_gauge(thread_id: int) -> None:
            barrier.wait()
            for i in range(ITERATIONS):
                reg.gauge("temperature", float(thread_id * 1000 + i))

        threads = [
            threading.Thread(target=set_gauge, args=(tid,))
            for tid in range(THREADS)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should have some valid float value (last writer wins)
        val = reg.get_gauge("temperature")
        assert val is not None
        assert isinstance(val, float)

    def test_concurrent_timer_records(self) -> None:
        """Timer total and count are exact after concurrent records."""
        reg = InMemoryRegistry()
        barrier = threading.Barrier(THREADS)

        def record_timer() -> None:
            barrier.wait()
            for _ in range(ITERATIONS):
                reg.timer("latency", 1.0)

        threads = [threading.Thread(target=record_timer) for _ in range(THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert reg.get_timer_total("latency") == float(THREADS * ITERATIONS)
        assert reg.get_timer_count("latency") == THREADS * ITERATIONS

    def test_concurrent_tagged_counters(self) -> None:
        """Tagged counters are isolated under contention."""
        reg = InMemoryRegistry()
        barrier = threading.Barrier(THREADS)

        def increment_tagged(tag_val: str) -> None:
            barrier.wait()
            for _ in range(ITERATIONS):
                reg.counter("requests", tags={"method": tag_val})

        threads = [
            threading.Thread(target=increment_tagged, args=(f"method-{i}",))
            for i in range(THREADS)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        for i in range(THREADS):
            assert reg.get_counter("requests", tags={"method": f"method-{i}"}) == ITERATIONS

    def test_concurrent_reset_during_writes(self) -> None:
        """reset() during concurrent writes does not deadlock."""
        reg = InMemoryRegistry()
        barrier = threading.Barrier(THREADS + 1)

        def write_loop() -> None:
            barrier.wait()
            for _ in range(ITERATIONS):
                reg.counter("c")
                reg.gauge("g", 1.0)
                reg.timer("t", 1.0)

        def reset_loop() -> None:
            barrier.wait()
            for _ in range(ITERATIONS // 10):
                reg.reset()

        threads = [threading.Thread(target=write_loop) for _ in range(THREADS)]
        threads.append(threading.Thread(target=reset_loop))
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # No assertion on values (non-deterministic), just no deadlock/crash
