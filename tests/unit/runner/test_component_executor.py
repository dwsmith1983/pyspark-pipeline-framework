"""Tests for ComponentExecutor."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pyspark_pipeline_framework.core.config.base import ComponentType
from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.retry import CircuitBreakerConfig, RetryConfig
from pyspark_pipeline_framework.core.resilience.circuit_breaker import CircuitBreaker
from pyspark_pipeline_framework.runner.component_executor import ComponentExecutor
from pyspark_pipeline_framework.runner.hooks import NoOpHooks

# Reuse test helpers from test_simple_runner
from tests.unit.runner.test_simple_runner import (
    _FailingResourceComponent,
    _FlakeyComponent,
    _ResourceComponent,
)

_MOD = "tests.unit.runner.test_simple_runner"


@pytest.fixture(autouse=True)
def _reset_state() -> None:
    _FlakeyComponent.failures_remaining = 0
    _ResourceComponent.log = []
    _FailingResourceComponent.log = []


def _comp(
    name: str = "test",
    cls_name: str = "_SuccessComponent",
    retry: RetryConfig | None = None,
    cb: CircuitBreakerConfig | None = None,
) -> ComponentConfig:
    return ComponentConfig(
        name=name,
        component_type=ComponentType.TRANSFORMATION,
        class_path=f"{_MOD}.{cls_name}",
        retry=retry,
        circuit_breaker=cb,
    )


def _make_executor(
    hooks: NoOpHooks | None = None,
) -> ComponentExecutor:
    return ComponentExecutor(
        hooks=hooks or NoOpHooks(),
        circuit_breakers={},
        clock=lambda: 0.0,
        sleep_func=lambda _: None,
    )


class TestComponentExecutorBasic:
    def test_success(self) -> None:
        executor = _make_executor()
        result = executor.execute(_comp(), MagicMock(), 0, 1)
        assert result.success is True
        assert result.component_name == "test"

    def test_failure(self) -> None:
        _FlakeyComponent.failures_remaining = 1
        executor = _make_executor()
        result = executor.execute(
            _comp(cls_name="_FlakeyComponent"), MagicMock(), 0, 1,
        )
        assert result.success is False
        assert result.error is not None


class TestComponentExecutorRetry:
    def test_retries_then_succeeds(self) -> None:
        _FlakeyComponent.failures_remaining = 2
        executor = _make_executor()
        config = _comp(
            cls_name="_FlakeyComponent",
            retry=RetryConfig(
                max_attempts=3,
                initial_delay_seconds=0.01,
                retry_on_exceptions=["RuntimeError"],
            ),
        )
        result = executor.execute(config, MagicMock(), 0, 1)
        assert result.success is True
        assert result.retries == 2


class TestComponentExecutorResource:
    def test_resource_lifecycle(self) -> None:
        executor = _make_executor()
        result = executor.execute(
            _comp(cls_name="_ResourceComponent"), MagicMock(), 0, 1,
        )
        assert result.success is True
        assert _ResourceComponent.log == ["open", "run", "close"]

    def test_resource_close_on_failure(self) -> None:
        executor = _make_executor()
        result = executor.execute(
            _comp(cls_name="_FailingResourceComponent"), MagicMock(), 0, 1,
        )
        assert result.success is False
        assert _FailingResourceComponent.log == ["open", "run", "close"]


class TestComponentExecutorCircuitBreaker:
    def test_circuit_breaker_created(self) -> None:
        cb_dict: dict[str, CircuitBreaker] = {}
        executor = ComponentExecutor(
            hooks=NoOpHooks(),
            circuit_breakers=cb_dict,
            clock=lambda: 0.0,
            sleep_func=lambda _: None,
        )
        config = _comp(cb=CircuitBreakerConfig(failure_threshold=3))
        executor.execute(config, MagicMock(), 0, 1)
        assert "test" in cb_dict


class TestComponentExecutorHooks:
    def test_hooks_called_on_success(self) -> None:
        hooks = MagicMock()
        executor = ComponentExecutor(
            hooks=hooks,
            circuit_breakers={},
            clock=lambda: 0.0,
            sleep_func=lambda _: None,
        )
        executor.execute(_comp(), MagicMock(), 0, 1)
        hooks.before_component.assert_called_once()
        hooks.after_component.assert_called_once()

    def test_hooks_called_on_failure(self) -> None:
        _FlakeyComponent.failures_remaining = 1
        hooks = MagicMock()
        executor = ComponentExecutor(
            hooks=hooks,
            circuit_breakers={},
            clock=lambda: 0.0,
            sleep_func=lambda _: None,
        )
        executor.execute(_comp(cls_name="_FlakeyComponent"), MagicMock(), 0, 1)
        hooks.before_component.assert_called_once()
        hooks.on_component_failure.assert_called_once()
