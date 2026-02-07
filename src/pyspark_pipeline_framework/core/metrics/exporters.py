"""Metric registry adapters for Prometheus and OpenTelemetry.

These adapters implement the :class:`MeterRegistry` protocol and delegate
to the respective observability libraries.  Each import is guarded so the
library is only required at runtime if the adapter is actually instantiated.

Install the optional extras to use them::

    pip install pyspark-pipeline-framework[metrics]
"""

from __future__ import annotations

import threading
from typing import Any


class PrometheusRegistry:
    """Adapter that forwards metrics to ``prometheus_client``.

    Counters map to Prometheus :class:`~prometheus_client.Counter`,
    gauges to :class:`~prometheus_client.Gauge`, and timers to
    :class:`~prometheus_client.Summary` (observed in milliseconds).

    Metrics are created lazily on first use and cached for subsequent
    calls.  Label names are derived from the tag keys of the first
    call for a given metric name.

    Raises:
        ImportError: If ``prometheus_client`` is not installed.
    """

    def __init__(self) -> None:
        try:
            import prometheus_client  # noqa: F401
        except ImportError:
            raise ImportError(
                "prometheus_client is required for PrometheusRegistry. Install it with: pip install prometheus-client"
            ) from None

        self._lock = threading.Lock()
        self._counters: dict[str, Any] = {}
        self._gauges: dict[str, Any] = {}
        self._summaries: dict[str, Any] = {}

    def counter(
        self,
        name: str,
        value: float = 1.0,
        tags: dict[str, str] | None = None,
    ) -> None:
        metric = self._get_or_create_counter(name, tags)
        if tags:
            metric.labels(**tags).inc(value)
        else:
            metric.inc(value)

    def gauge(
        self,
        name: str,
        value: float,
        tags: dict[str, str] | None = None,
    ) -> None:
        metric = self._get_or_create_gauge(name, tags)
        if tags:
            metric.labels(**tags).set(value)
        else:
            metric.set(value)

    def timer(
        self,
        name: str,
        duration_ms: float,
        tags: dict[str, str] | None = None,
    ) -> None:
        metric = self._get_or_create_summary(name, tags)
        if tags:
            metric.labels(**tags).observe(duration_ms)
        else:
            metric.observe(duration_ms)

    def get_metrics(self) -> dict[str, Any]:
        """Return registered metric names grouped by kind."""
        with self._lock:
            return {
                "counters": list(self._counters.keys()),
                "gauges": list(self._gauges.keys()),
                "timers": list(self._summaries.keys()),
            }

    # -- internal helpers ---------------------------------------------------

    def _get_or_create_counter(self, name: str, tags: dict[str, str] | None) -> Any:
        with self._lock:
            if name not in self._counters:
                from prometheus_client import Counter

                label_names = sorted(tags.keys()) if tags else []
                self._counters[name] = Counter(name, f"Counter {name}", label_names)
            return self._counters[name]

    def _get_or_create_gauge(self, name: str, tags: dict[str, str] | None) -> Any:
        with self._lock:
            if name not in self._gauges:
                from prometheus_client import Gauge

                label_names = sorted(tags.keys()) if tags else []
                self._gauges[name] = Gauge(name, f"Gauge {name}", label_names)
            return self._gauges[name]

    def _get_or_create_summary(self, name: str, tags: dict[str, str] | None) -> Any:
        with self._lock:
            if name not in self._summaries:
                from prometheus_client import Summary

                label_names = sorted(tags.keys()) if tags else []
                self._summaries[name] = Summary(name, f"Timer {name}", label_names)
            return self._summaries[name]


class OpenTelemetryRegistry:
    """Adapter that forwards metrics to OpenTelemetry.

    Counters map to OTel :class:`~opentelemetry.metrics.Counter`,
    gauges to :class:`~opentelemetry.metrics.UpDownCounter`, and timers
    to :class:`~opentelemetry.metrics.Histogram` (recorded in
    milliseconds).

    Metrics are created lazily on first use and cached for subsequent
    calls.

    Raises:
        ImportError: If ``opentelemetry-api`` is not installed.
    """

    def __init__(self, meter_name: str = "pyspark_pipeline_framework") -> None:
        try:
            from opentelemetry import metrics as otel_metrics
        except ImportError:
            raise ImportError(
                "opentelemetry-api is required for OpenTelemetryRegistry. "
                "Install it with: pip install opentelemetry-api"
            ) from None

        self._meter = otel_metrics.get_meter(meter_name)
        self._lock = threading.Lock()
        self._counters: dict[str, Any] = {}
        self._gauges: dict[str, Any] = {}
        self._histograms: dict[str, Any] = {}

    def counter(
        self,
        name: str,
        value: float = 1.0,
        tags: dict[str, str] | None = None,
    ) -> None:
        instrument = self._get_or_create_counter(name)
        instrument.add(value, attributes=tags or {})

    def gauge(
        self,
        name: str,
        value: float,
        tags: dict[str, str] | None = None,
    ) -> None:
        instrument = self._get_or_create_gauge(name)
        instrument.add(value, attributes=tags or {})

    def timer(
        self,
        name: str,
        duration_ms: float,
        tags: dict[str, str] | None = None,
    ) -> None:
        instrument = self._get_or_create_histogram(name)
        instrument.record(duration_ms, attributes=tags or {})

    def get_metrics(self) -> dict[str, Any]:
        """Return registered metric names grouped by kind."""
        with self._lock:
            return {
                "counters": list(self._counters.keys()),
                "gauges": list(self._gauges.keys()),
                "timers": list(self._histograms.keys()),
            }

    # -- internal helpers ---------------------------------------------------

    def _get_or_create_counter(self, name: str) -> Any:
        with self._lock:
            if name not in self._counters:
                self._counters[name] = self._meter.create_counter(name)
            return self._counters[name]

    def _get_or_create_gauge(self, name: str) -> Any:
        with self._lock:
            if name not in self._gauges:
                self._gauges[name] = self._meter.create_up_down_counter(name)
            return self._gauges[name]

    def _get_or_create_histogram(self, name: str) -> Any:
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = self._meter.create_histogram(name, unit="ms")
            return self._histograms[name]
