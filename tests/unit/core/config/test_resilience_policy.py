"""Tests for ResiliencePolicy and ResiliencePolicies presets."""

from __future__ import annotations

import pytest

from pyspark_pipeline_framework.core.config.base import ComponentType
from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.presets import (
    CircuitBreakerConfigs,
    ResiliencePolicies,
    RetryPolicies,
)
from pyspark_pipeline_framework.core.config.retry import (
    CircuitBreakerConfig,
    ResiliencePolicy,
    RetryConfig,
)

# ---------------------------------------------------------------------------
# ResiliencePolicy dataclass
# ---------------------------------------------------------------------------


class TestResiliencePolicy:
    def test_defaults_are_none(self) -> None:
        policy = ResiliencePolicy()
        assert policy.retry is None
        assert policy.circuit_breaker is None

    def test_retry_only(self) -> None:
        retry = RetryConfig(max_attempts=5)
        policy = ResiliencePolicy(retry=retry)
        assert policy.retry is retry
        assert policy.circuit_breaker is None

    def test_circuit_breaker_only(self) -> None:
        cb = CircuitBreakerConfig(failure_threshold=3)
        policy = ResiliencePolicy(circuit_breaker=cb)
        assert policy.retry is None
        assert policy.circuit_breaker is cb

    def test_both(self) -> None:
        retry = RetryConfig()
        cb = CircuitBreakerConfig()
        policy = ResiliencePolicy(retry=retry, circuit_breaker=cb)
        assert policy.retry is retry
        assert policy.circuit_breaker is cb

    def test_frozen(self) -> None:
        policy = ResiliencePolicy()
        with pytest.raises(AttributeError):
            policy.retry = RetryConfig()  # type: ignore[misc]


# ---------------------------------------------------------------------------
# ResiliencePolicies presets
# ---------------------------------------------------------------------------


class TestResiliencePolicies:
    def test_none_has_no_resilience(self) -> None:
        p = ResiliencePolicies.NONE
        assert p.retry is None
        assert p.circuit_breaker is None

    def test_default_has_both(self) -> None:
        p = ResiliencePolicies.DEFAULT
        assert p.retry is RetryPolicies.DEFAULT
        assert p.circuit_breaker is CircuitBreakerConfigs.DEFAULT

    def test_aggressive(self) -> None:
        p = ResiliencePolicies.AGGRESSIVE
        assert p.retry is RetryPolicies.AGGRESSIVE
        assert p.circuit_breaker is CircuitBreakerConfigs.SENSITIVE

    def test_conservative(self) -> None:
        p = ResiliencePolicies.CONSERVATIVE
        assert p.retry is RetryPolicies.CONSERVATIVE
        assert p.circuit_breaker is CircuitBreakerConfigs.RESILIENT

    def test_retry_only(self) -> None:
        p = ResiliencePolicies.RETRY_ONLY
        assert p.retry is RetryPolicies.DEFAULT
        assert p.circuit_breaker is None

    def test_circuit_breaker_only(self) -> None:
        p = ResiliencePolicies.CIRCUIT_BREAKER_ONLY
        assert p.retry is None
        assert p.circuit_breaker is CircuitBreakerConfigs.DEFAULT


# ---------------------------------------------------------------------------
# ComponentConfig integration
# ---------------------------------------------------------------------------


def _comp(
    *,
    retry: RetryConfig | None = None,
    circuit_breaker: CircuitBreakerConfig | None = None,
    resilience: ResiliencePolicy | None = None,
) -> ComponentConfig:
    return ComponentConfig(
        name="test",
        component_type=ComponentType.TRANSFORMATION,
        class_path="some.Module",
        retry=retry,
        circuit_breaker=circuit_breaker,
        resilience=resilience,
    )


class TestComponentConfigResilience:
    def test_resilience_populates_retry_and_cb(self) -> None:
        comp = _comp(resilience=ResiliencePolicies.DEFAULT)
        assert comp.retry is RetryPolicies.DEFAULT
        assert comp.circuit_breaker is CircuitBreakerConfigs.DEFAULT

    def test_resilience_none_is_noop(self) -> None:
        comp = _comp()
        assert comp.retry is None
        assert comp.circuit_breaker is None
        assert comp.resilience is None

    def test_resilience_retry_only_policy(self) -> None:
        comp = _comp(resilience=ResiliencePolicies.RETRY_ONLY)
        assert comp.retry is RetryPolicies.DEFAULT
        assert comp.circuit_breaker is None

    def test_resilience_cb_only_policy(self) -> None:
        comp = _comp(resilience=ResiliencePolicies.CIRCUIT_BREAKER_ONLY)
        assert comp.retry is None
        assert comp.circuit_breaker is CircuitBreakerConfigs.DEFAULT

    def test_resilience_with_retry_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot set both"):
            _comp(
                resilience=ResiliencePolicies.DEFAULT,
                retry=RetryConfig(),
            )

    def test_resilience_with_cb_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot set both"):
            _comp(
                resilience=ResiliencePolicies.DEFAULT,
                circuit_breaker=CircuitBreakerConfig(),
            )

    def test_resilience_with_both_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot set both"):
            _comp(
                resilience=ResiliencePolicies.DEFAULT,
                retry=RetryConfig(),
                circuit_breaker=CircuitBreakerConfig(),
            )

    def test_individual_fields_still_work(self) -> None:
        retry = RetryConfig(max_attempts=7)
        cb = CircuitBreakerConfig(failure_threshold=2)
        comp = _comp(retry=retry, circuit_breaker=cb)
        assert comp.retry is retry
        assert comp.circuit_breaker is cb
        assert comp.resilience is None

    def test_none_policy_keeps_fields_none(self) -> None:
        comp = _comp(resilience=ResiliencePolicies.NONE)
        assert comp.retry is None
        assert comp.circuit_breaker is None
