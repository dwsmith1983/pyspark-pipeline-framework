"""Tests for core.metrics.registry."""

from __future__ import annotations

import threading

from pyspark_pipeline_framework.core.metrics.registry import (
    InMemoryRegistry,
    MeterRegistry,
)


class TestMeterRegistryProtocol:
    """Tests for MeterRegistry protocol."""

    def test_in_memory_registry_satisfies_protocol(self) -> None:
        registry = InMemoryRegistry()
        assert isinstance(registry, MeterRegistry)


class TestInMemoryRegistryCounter:
    """Tests for InMemoryRegistry.counter()."""

    def test_increment_default(self) -> None:
        reg = InMemoryRegistry()
        reg.counter("requests")
        assert reg.get_counter("requests") == 1.0

    def test_increment_custom_value(self) -> None:
        reg = InMemoryRegistry()
        reg.counter("bytes", value=1024.0)
        assert reg.get_counter("bytes") == 1024.0

    def test_accumulates(self) -> None:
        reg = InMemoryRegistry()
        reg.counter("requests")
        reg.counter("requests")
        reg.counter("requests", value=3.0)
        assert reg.get_counter("requests") == 5.0

    def test_tags_separate_counters(self) -> None:
        reg = InMemoryRegistry()
        reg.counter("requests", tags={"method": "GET"})
        reg.counter("requests", tags={"method": "POST"})
        reg.counter("requests", tags={"method": "GET"})
        assert reg.get_counter("requests", tags={"method": "GET"}) == 2.0
        assert reg.get_counter("requests", tags={"method": "POST"}) == 1.0

    def test_missing_counter_returns_zero(self) -> None:
        reg = InMemoryRegistry()
        assert reg.get_counter("nonexistent") == 0.0


class TestInMemoryRegistryGauge:
    """Tests for InMemoryRegistry.gauge()."""

    def test_set_value(self) -> None:
        reg = InMemoryRegistry()
        reg.gauge("temperature", 72.5)
        assert reg.get_gauge("temperature") == 72.5

    def test_overwrite_value(self) -> None:
        reg = InMemoryRegistry()
        reg.gauge("temperature", 72.5)
        reg.gauge("temperature", 80.0)
        assert reg.get_gauge("temperature") == 80.0

    def test_tags_separate_gauges(self) -> None:
        reg = InMemoryRegistry()
        reg.gauge("cpu", 0.5, tags={"core": "0"})
        reg.gauge("cpu", 0.8, tags={"core": "1"})
        assert reg.get_gauge("cpu", tags={"core": "0"}) == 0.5
        assert reg.get_gauge("cpu", tags={"core": "1"}) == 0.8

    def test_missing_gauge_returns_none(self) -> None:
        reg = InMemoryRegistry()
        assert reg.get_gauge("nonexistent") is None


class TestInMemoryRegistryTimer:
    """Tests for InMemoryRegistry.timer()."""

    def test_record_duration(self) -> None:
        reg = InMemoryRegistry()
        reg.timer("latency", 150.0)
        assert reg.get_timer_total("latency") == 150.0
        assert reg.get_timer_count("latency") == 1

    def test_accumulates_duration_and_count(self) -> None:
        reg = InMemoryRegistry()
        reg.timer("latency", 100.0)
        reg.timer("latency", 200.0)
        assert reg.get_timer_total("latency") == 300.0
        assert reg.get_timer_count("latency") == 2

    def test_tags_separate_timers(self) -> None:
        reg = InMemoryRegistry()
        reg.timer("latency", 100.0, tags={"endpoint": "/api"})
        reg.timer("latency", 200.0, tags={"endpoint": "/health"})
        assert reg.get_timer_total("latency", tags={"endpoint": "/api"}) == 100.0
        assert reg.get_timer_total("latency", tags={"endpoint": "/health"}) == 200.0

    def test_missing_timer_returns_zero(self) -> None:
        reg = InMemoryRegistry()
        assert reg.get_timer_total("nonexistent") == 0.0
        assert reg.get_timer_count("nonexistent") == 0


class TestInMemoryRegistryGetMetrics:
    """Tests for InMemoryRegistry.get_metrics()."""

    def test_empty_snapshot(self) -> None:
        reg = InMemoryRegistry()
        metrics = reg.get_metrics()
        assert metrics == {"counters": {}, "gauges": {}, "timers": {}}

    def test_populated_snapshot(self) -> None:
        reg = InMemoryRegistry()
        reg.counter("requests")
        reg.gauge("cpu", 0.5)
        reg.timer("latency", 100.0)

        metrics = reg.get_metrics()
        assert "requests" in metrics["counters"]
        assert "cpu" in metrics["gauges"]
        assert "latency" in metrics["timers"]


class TestInMemoryRegistryReset:
    """Tests for InMemoryRegistry.reset()."""

    def test_clears_all_metrics(self) -> None:
        reg = InMemoryRegistry()
        reg.counter("requests")
        reg.gauge("cpu", 0.5)
        reg.timer("latency", 100.0)
        reg.reset()
        assert reg.get_counter("requests") == 0.0
        assert reg.get_gauge("cpu") is None
        assert reg.get_timer_total("latency") == 0.0


class TestInMemoryRegistryThreadSafety:
    """Tests for thread safety of InMemoryRegistry."""

    def test_concurrent_counter_increments(self) -> None:
        reg = InMemoryRegistry()
        iterations = 1000

        def increment() -> None:
            for _ in range(iterations):
                reg.counter("concurrent")

        threads = [threading.Thread(target=increment) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert reg.get_counter("concurrent") == 4 * iterations
