"""Tests for Prometheus and OpenTelemetry metric registry adapters."""

from __future__ import annotations

import pytest

from pyspark_pipeline_framework.core.metrics.exporters import (
    OpenTelemetryRegistry,
    PrometheusRegistry,
)
from pyspark_pipeline_framework.core.metrics.registry import MeterRegistry


# ---------------------------------------------------------------------------
# PrometheusRegistry
# ---------------------------------------------------------------------------


class TestPrometheusRegistryProtocol:
    """PrometheusRegistry satisfies the MeterRegistry protocol."""

    def test_implements_protocol(self) -> None:
        registry = PrometheusRegistry()
        assert isinstance(registry, MeterRegistry)


class TestPrometheusCounter:
    def test_counter_no_tags(self) -> None:
        reg = PrometheusRegistry()
        reg.counter("test_prom_counter_no_tags_total")
        reg.counter("test_prom_counter_no_tags_total", value=2.0)
        metrics = reg.get_metrics()
        assert "test_prom_counter_no_tags_total" in metrics["counters"]

    def test_counter_with_tags(self) -> None:
        reg = PrometheusRegistry()
        reg.counter(
            "test_prom_counter_tags_total",
            value=1.0,
            tags={"component": "extract"},
        )
        metrics = reg.get_metrics()
        assert "test_prom_counter_tags_total" in metrics["counters"]


class TestPrometheusGauge:
    def test_gauge_no_tags(self) -> None:
        reg = PrometheusRegistry()
        reg.gauge("test_prom_gauge_no_tags", value=42.0)
        metrics = reg.get_metrics()
        assert "test_prom_gauge_no_tags" in metrics["gauges"]

    def test_gauge_with_tags(self) -> None:
        reg = PrometheusRegistry()
        reg.gauge("test_prom_gauge_tags", value=10.0, tags={"env": "test"})
        metrics = reg.get_metrics()
        assert "test_prom_gauge_tags" in metrics["gauges"]


class TestPrometheusTimer:
    def test_timer_no_tags(self) -> None:
        reg = PrometheusRegistry()
        reg.timer("test_prom_timer_no_tags", duration_ms=123.4)
        metrics = reg.get_metrics()
        assert "test_prom_timer_no_tags" in metrics["timers"]

    def test_timer_with_tags(self) -> None:
        reg = PrometheusRegistry()
        reg.timer(
            "test_prom_timer_tags",
            duration_ms=50.0,
            tags={"component": "load"},
        )
        metrics = reg.get_metrics()
        assert "test_prom_timer_tags" in metrics["timers"]


class TestPrometheusGetMetrics:
    def test_empty_registry(self) -> None:
        reg = PrometheusRegistry()
        metrics = reg.get_metrics()
        assert metrics == {"counters": [], "gauges": [], "timers": []}

    def test_all_kinds(self) -> None:
        reg = PrometheusRegistry()
        reg.counter("test_prom_all_c_total")
        reg.gauge("test_prom_all_g", value=1.0)
        reg.timer("test_prom_all_t", duration_ms=1.0)
        metrics = reg.get_metrics()
        assert "test_prom_all_c_total" in metrics["counters"]
        assert "test_prom_all_g" in metrics["gauges"]
        assert "test_prom_all_t" in metrics["timers"]


# ---------------------------------------------------------------------------
# OpenTelemetryRegistry
# ---------------------------------------------------------------------------


class TestOpenTelemetryRegistryProtocol:
    """OpenTelemetryRegistry satisfies the MeterRegistry protocol."""

    def test_implements_protocol(self) -> None:
        registry = OpenTelemetryRegistry()
        assert isinstance(registry, MeterRegistry)


class TestOpenTelemetryCounter:
    def test_counter_no_tags(self) -> None:
        reg = OpenTelemetryRegistry(meter_name="test_otel_counter")
        reg.counter("otel_counter_no_tags")
        metrics = reg.get_metrics()
        assert "otel_counter_no_tags" in metrics["counters"]

    def test_counter_with_tags(self) -> None:
        reg = OpenTelemetryRegistry(meter_name="test_otel_counter_tags")
        reg.counter("otel_counter_tags", value=5.0, tags={"stage": "extract"})
        metrics = reg.get_metrics()
        assert "otel_counter_tags" in metrics["counters"]


class TestOpenTelemetryGauge:
    def test_gauge_no_tags(self) -> None:
        reg = OpenTelemetryRegistry(meter_name="test_otel_gauge")
        reg.gauge("otel_gauge_no_tags", value=99.0)
        metrics = reg.get_metrics()
        assert "otel_gauge_no_tags" in metrics["gauges"]

    def test_gauge_with_tags(self) -> None:
        reg = OpenTelemetryRegistry(meter_name="test_otel_gauge_tags")
        reg.gauge("otel_gauge_tags", value=7.0, tags={"env": "prod"})
        metrics = reg.get_metrics()
        assert "otel_gauge_tags" in metrics["gauges"]


class TestOpenTelemetryTimer:
    def test_timer_no_tags(self) -> None:
        reg = OpenTelemetryRegistry(meter_name="test_otel_timer")
        reg.timer("otel_timer_no_tags", duration_ms=200.0)
        metrics = reg.get_metrics()
        assert "otel_timer_no_tags" in metrics["timers"]

    def test_timer_with_tags(self) -> None:
        reg = OpenTelemetryRegistry(meter_name="test_otel_timer_tags")
        reg.timer("otel_timer_tags", duration_ms=15.0, tags={"op": "read"})
        metrics = reg.get_metrics()
        assert "otel_timer_tags" in metrics["timers"]


class TestOpenTelemetryGetMetrics:
    def test_empty_registry(self) -> None:
        reg = OpenTelemetryRegistry(meter_name="test_otel_empty")
        metrics = reg.get_metrics()
        assert metrics == {"counters": [], "gauges": [], "timers": []}

    def test_all_kinds(self) -> None:
        reg = OpenTelemetryRegistry(meter_name="test_otel_all")
        reg.counter("otel_all_c")
        reg.gauge("otel_all_g", value=1.0)
        reg.timer("otel_all_t", duration_ms=1.0)
        metrics = reg.get_metrics()
        assert "otel_all_c" in metrics["counters"]
        assert "otel_all_g" in metrics["gauges"]
        assert "otel_all_t" in metrics["timers"]


class TestOpenTelemetryCustomMeterName:
    def test_custom_meter_name(self) -> None:
        reg = OpenTelemetryRegistry(meter_name="my_app")
        reg.counter("custom_meter_counter")
        assert "custom_meter_counter" in reg.get_metrics()["counters"]


# ---------------------------------------------------------------------------
# Import guards
# ---------------------------------------------------------------------------


class TestImportGuards:
    """Verify helpful error messages when libraries are missing."""

    def test_prometheus_import_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import importlib
        import sys

        # Temporarily hide prometheus_client
        real_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__  # type: ignore[union-attr]

        def fake_import(name: str, *args: object, **kwargs: object) -> object:
            if name == "prometheus_client":
                raise ImportError("no prometheus_client")
            return real_import(name, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr("builtins.__import__", fake_import)

        # Reload the module to trigger the import guard
        if "pyspark_pipeline_framework.core.metrics.exporters" in sys.modules:
            del sys.modules["pyspark_pipeline_framework.core.metrics.exporters"]

        from pyspark_pipeline_framework.core.metrics.exporters import PrometheusRegistry as PR

        with pytest.raises(ImportError, match="prometheus_client"):
            PR()

    def test_opentelemetry_import_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import sys

        real_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__  # type: ignore[union-attr]

        def fake_import(name: str, *args: object, **kwargs: object) -> object:
            if name.startswith("opentelemetry"):
                raise ImportError("no opentelemetry")
            return real_import(name, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr("builtins.__import__", fake_import)

        if "pyspark_pipeline_framework.core.metrics.exporters" in sys.modules:
            del sys.modules["pyspark_pipeline_framework.core.metrics.exporters"]

        from pyspark_pipeline_framework.core.metrics.exporters import OpenTelemetryRegistry as OTR

        with pytest.raises(ImportError, match="opentelemetry"):
            OTR()
