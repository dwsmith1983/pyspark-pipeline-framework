"""Meter registry protocol and default implementation.

Provides a pluggable abstraction for recording counters, gauges, and
timers. Implementations can export metrics to Prometheus,
OpenTelemetry, or any other backend.

The :class:`InMemoryRegistry` stores all metrics in memory and is
suitable for testing or lightweight use.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class MeterRegistry(Protocol):
    """Protocol for recording application metrics.

    Implementations receive metric data from pipeline hooks and can
    export it to any observability backend.

    All methods must be safe to call from multiple threads.
    """

    def counter(
        self,
        name: str,
        value: float = 1.0,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Increment a counter metric.

        Args:
            name: Metric name (e.g. ``"ppf.component.retries"``).
            value: Amount to increment by.
            tags: Optional key-value tags for dimensionality.
        """
        ...

    def gauge(
        self,
        name: str,
        value: float,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Set a gauge metric to an absolute value.

        Args:
            name: Metric name (e.g. ``"ppf.pipeline.active_components"``).
            value: Current value.
            tags: Optional key-value tags for dimensionality.
        """
        ...

    def timer(
        self,
        name: str,
        duration_ms: float,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Record a timing measurement.

        Args:
            name: Metric name (e.g. ``"ppf.component.duration"``).
            duration_ms: Duration in milliseconds.
            tags: Optional key-value tags for dimensionality.
        """
        ...

    def get_metrics(self) -> dict[str, Any]:
        """Return a snapshot of all recorded metrics.

        Returns:
            A dictionary keyed by metric name. The value structure
            is implementation-defined.
        """
        ...


def _tag_key(tags: dict[str, str] | None) -> str:
    """Create a hashable key from tags for metric bucketing."""
    if not tags:
        return ""
    return ",".join(f"{k}={v}" for k, v in sorted(tags.items()))


@dataclass
class _MetricEntry:
    """A single metric data point."""

    name: str
    tags: dict[str, str] = field(default_factory=dict)
    value: float = 0.0
    count: int = 0


class InMemoryRegistry:
    """Thread-safe in-memory metrics registry.

    Stores counters, gauges, and timer totals/counts in memory.
    Useful for testing, local debugging, and as the default registry
    when no external backend is configured.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, dict[str, float]] = {}
        self._gauges: dict[str, dict[str, float]] = {}
        self._timers: dict[str, dict[str, _MetricEntry]] = {}

    def counter(
        self,
        name: str,
        value: float = 1.0,
        tags: dict[str, str] | None = None,
    ) -> None:
        key = _tag_key(tags)
        with self._lock:
            bucket = self._counters.setdefault(name, {})
            bucket[key] = bucket.get(key, 0.0) + value

    def gauge(
        self,
        name: str,
        value: float,
        tags: dict[str, str] | None = None,
    ) -> None:
        key = _tag_key(tags)
        with self._lock:
            bucket = self._gauges.setdefault(name, {})
            bucket[key] = value

    def timer(
        self,
        name: str,
        duration_ms: float,
        tags: dict[str, str] | None = None,
    ) -> None:
        key = _tag_key(tags)
        with self._lock:
            bucket = self._timers.setdefault(name, {})
            if key not in bucket:
                bucket[key] = _MetricEntry(name=name, tags=tags or {})
            entry = bucket[key]
            entry.value += duration_ms
            entry.count += 1

    def get_metrics(self) -> dict[str, Any]:
        """Return a snapshot of all recorded metrics.

        Returns:
            Dictionary with keys ``"counters"``, ``"gauges"``, ``"timers"``.
            Each contains a dict keyed by metric name with values
            representing the aggregated data.
        """
        with self._lock:
            return {
                "counters": {name: dict(buckets) for name, buckets in self._counters.items()},
                "gauges": {name: dict(buckets) for name, buckets in self._gauges.items()},
                "timers": {
                    name: {
                        key: {"total_ms": entry.value, "count": entry.count, "tags": dict(entry.tags)}
                        for key, entry in buckets.items()
                    }
                    for name, buckets in self._timers.items()
                },
            }

    def get_counter(self, name: str, tags: dict[str, str] | None = None) -> float:
        """Get current counter value for convenience.

        Args:
            name: Counter name.
            tags: Optional tags to match.

        Returns:
            Current counter value, or ``0.0`` if not found.
        """
        key = _tag_key(tags)
        with self._lock:
            return self._counters.get(name, {}).get(key, 0.0)

    def get_gauge(self, name: str, tags: dict[str, str] | None = None) -> float | None:
        """Get current gauge value for convenience.

        Args:
            name: Gauge name.
            tags: Optional tags to match.

        Returns:
            Current gauge value, or ``None`` if not set.
        """
        key = _tag_key(tags)
        with self._lock:
            return self._gauges.get(name, {}).get(key)

    def get_timer_total(self, name: str, tags: dict[str, str] | None = None) -> float:
        """Get total timer duration for convenience.

        Args:
            name: Timer name.
            tags: Optional tags to match.

        Returns:
            Total recorded duration in ms, or ``0.0`` if not found.
        """
        key = _tag_key(tags)
        with self._lock:
            entry = self._timers.get(name, {}).get(key)
            return entry.value if entry else 0.0

    def get_timer_count(self, name: str, tags: dict[str, str] | None = None) -> int:
        """Get number of timer recordings for convenience.

        Args:
            name: Timer name.
            tags: Optional tags to match.

        Returns:
            Number of recordings, or ``0`` if not found.
        """
        key = _tag_key(tags)
        with self._lock:
            entry = self._timers.get(name, {}).get(key)
            return entry.count if entry else 0

    def reset(self) -> None:
        """Clear all recorded metrics."""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._timers.clear()
