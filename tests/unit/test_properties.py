"""Property-based tests using Hypothesis.

Covers invariants for retry, circuit breaker, schema, config,
secret parsing, and topological sort.
"""

from __future__ import annotations

import math
import string

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from pyspark_pipeline_framework.core.config.base import ComponentType
from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.pipeline import PipelineConfig
from pyspark_pipeline_framework.core.config.presets import ResiliencePolicies
from pyspark_pipeline_framework.core.config.retry import (
    CircuitBreakerConfig,
    ResiliencePolicy,
    RetryConfig,
)
from pyspark_pipeline_framework.core.config.secret_resolver import parse_secret_reference
from pyspark_pipeline_framework.core.config.spark import SparkConfig
from pyspark_pipeline_framework.core.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitState,
)
from pyspark_pipeline_framework.core.resilience.retry import RetryExecutor
from pyspark_pipeline_framework.core.schema.definition import (
    DataType,
    SchemaDefinition,
    SchemaField,
)
from pyspark_pipeline_framework.core.schema.validator import SchemaValidator

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_valid_retry = st.builds(
    RetryConfig,
    max_attempts=st.integers(min_value=1, max_value=100),
    initial_delay_seconds=st.floats(min_value=0.001, max_value=10.0),
    max_delay_seconds=st.floats(min_value=10.0, max_value=300.0),
    backoff_multiplier=st.floats(min_value=1.0, max_value=5.0),
)

_valid_cb = st.builds(
    CircuitBreakerConfig,
    failure_threshold=st.integers(min_value=1, max_value=50),
    success_threshold=st.integers(min_value=1, max_value=20),
    timeout_seconds=st.floats(min_value=0.001, max_value=600.0),
    half_open_max_calls=st.integers(min_value=1, max_value=10),
)

_identifier = st.text(
    alphabet=string.ascii_lowercase + string.digits + "_",
    min_size=1,
    max_size=20,
)

_data_types = st.sampled_from(list(DataType))

_schema_field = st.builds(
    SchemaField,
    name=_identifier,
    data_type=_data_types,
    nullable=st.booleans(),
)


# ---------------------------------------------------------------------------
# RetryConfig validation
# ---------------------------------------------------------------------------


class TestRetryConfigProperties:
    @given(config=_valid_retry)
    def test_valid_configs_always_succeed(self, config: RetryConfig) -> None:
        assert config.max_attempts >= 1
        assert config.initial_delay_seconds > 0
        assert config.max_delay_seconds >= config.initial_delay_seconds

    @given(max_attempts=st.integers(max_value=0))
    def test_non_positive_attempts_always_raise(self, max_attempts: int) -> None:
        with pytest.raises(ValueError, match="max_attempts"):
            RetryConfig(max_attempts=max_attempts)

    @given(initial=st.floats(max_value=0.0))
    def test_non_positive_delay_always_raises(self, initial: float) -> None:
        assume(not math.isnan(initial))
        with pytest.raises(ValueError, match="initial_delay_seconds"):
            RetryConfig(initial_delay_seconds=initial)

    @given(multiplier=st.floats(max_value=0.99))
    def test_low_multiplier_always_raises(self, multiplier: float) -> None:
        assume(not math.isnan(multiplier))
        with pytest.raises(ValueError, match="backoff_multiplier"):
            RetryConfig(backoff_multiplier=multiplier)


# ---------------------------------------------------------------------------
# CircuitBreakerConfig validation
# ---------------------------------------------------------------------------


class TestCircuitBreakerConfigProperties:
    @given(config=_valid_cb)
    def test_valid_configs_always_succeed(self, config: CircuitBreakerConfig) -> None:
        assert config.failure_threshold >= 1
        assert config.success_threshold >= 1
        assert config.timeout_seconds > 0

    @given(threshold=st.integers(max_value=0))
    def test_non_positive_failure_threshold_raises(self, threshold: int) -> None:
        with pytest.raises(ValueError, match="failure_threshold"):
            CircuitBreakerConfig(failure_threshold=threshold)

    @given(timeout=st.floats(max_value=0.0))
    def test_non_positive_timeout_raises(self, timeout: float) -> None:
        assume(not math.isnan(timeout))
        with pytest.raises(ValueError, match="timeout_seconds"):
            CircuitBreakerConfig(timeout_seconds=timeout)


# ---------------------------------------------------------------------------
# RetryExecutor.calculate_delay
# ---------------------------------------------------------------------------


class TestRetryDelayProperties:
    @given(config=_valid_retry, attempt=st.integers(min_value=0, max_value=20))
    def test_delay_is_non_negative(self, config: RetryConfig, attempt: int) -> None:
        executor = RetryExecutor(config, jitter_factor=0.25)
        assert executor.calculate_delay(attempt) >= 0

    @given(config=_valid_retry, attempt=st.integers(min_value=0, max_value=20))
    def test_delay_bounded_by_max(self, config: RetryConfig, attempt: int) -> None:
        jitter = 0.25
        executor = RetryExecutor(config, jitter_factor=jitter)
        delay = executor.calculate_delay(attempt)
        upper_bound = config.max_delay_seconds * (1 + jitter)
        assert delay <= upper_bound + 1e-9  # epsilon for float imprecision

    @given(config=_valid_retry)
    def test_zero_jitter_is_deterministic(self, config: RetryConfig) -> None:
        executor = RetryExecutor(config, jitter_factor=0.0)
        d1 = executor.calculate_delay(0)
        d2 = executor.calculate_delay(0)
        assert d1 == d2

    @given(config=_valid_retry)
    @settings(max_examples=50)
    def test_zero_jitter_monotonically_non_decreasing(
        self, config: RetryConfig
    ) -> None:
        executor = RetryExecutor(config, jitter_factor=0.0)
        delays = [executor.calculate_delay(i) for i in range(10)]
        for i in range(1, len(delays)):
            assert delays[i] >= delays[i - 1]


# ---------------------------------------------------------------------------
# CircuitBreaker state machine
# ---------------------------------------------------------------------------


class TestCircuitBreakerStateMachine:
    @given(threshold=st.integers(min_value=1, max_value=10))
    def test_failures_open_circuit(self, threshold: int) -> None:
        cb = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=threshold),
            clock=lambda: 0.0,
        )
        for _ in range(threshold):
            cb.record_failure()
        assert cb.state is CircuitState.OPEN

    @given(threshold=st.integers(min_value=1, max_value=10))
    def test_fewer_failures_stay_closed(self, threshold: int) -> None:
        assume(threshold > 1)
        cb = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=threshold),
            clock=lambda: 0.0,
        )
        for _ in range(threshold - 1):
            cb.record_failure()
        assert cb.state is CircuitState.CLOSED

    @given(
        fail_thresh=st.integers(min_value=1, max_value=5),
        success_thresh=st.integers(min_value=1, max_value=5),
        timeout=st.floats(min_value=0.01, max_value=10.0),
    )
    def test_full_cycle_closed_open_half_closed(
        self, fail_thresh: int, success_thresh: int, timeout: float
    ) -> None:
        """CLOSED → OPEN → HALF_OPEN → CLOSED full cycle."""
        now = [0.0]

        def clock() -> float:
            return now[0]

        config = CircuitBreakerConfig(
            failure_threshold=fail_thresh,
            success_threshold=success_thresh,
            timeout_seconds=timeout,
        )
        cb = CircuitBreaker(config, clock=clock)

        # Trip to OPEN
        for _ in range(fail_thresh):
            cb.record_failure()
        state_after_failures: CircuitState = cb.state
        assert state_after_failures is CircuitState.OPEN

        # Advance past timeout → HALF_OPEN
        now[0] = timeout + 1.0
        state_after_timeout: CircuitState = cb.state
        assert state_after_timeout is CircuitState.HALF_OPEN

        # Record enough successes → CLOSED
        for _ in range(success_thresh):
            cb.record_success()
        state_after_recovery: CircuitState = cb.state
        assert state_after_recovery is CircuitState.CLOSED

    @given(threshold=st.integers(min_value=1, max_value=10))
    def test_success_resets_failure_count(self, threshold: int) -> None:
        cb = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=threshold),
            clock=lambda: 0.0,
        )
        # Accumulate failures just below threshold, then succeed
        for _ in range(threshold - 1):
            cb.record_failure()
        cb.record_success()
        assert cb.failure_count == 0
        assert cb.state is CircuitState.CLOSED


# ---------------------------------------------------------------------------
# Secret reference parsing
# ---------------------------------------------------------------------------


class TestSecretReferenceProperties:
    @given(
        provider=st.text(
            alphabet=string.ascii_lowercase + string.digits,
            min_size=1,
            max_size=10,
        ),
        key=st.text(
            alphabet=string.ascii_lowercase + string.digits + "/-_.",
            min_size=1,
            max_size=50,
        ),
    )
    def test_valid_references_roundtrip(self, provider: str, key: str) -> None:
        """secret://PROVIDER/KEY always parses back to provider and key."""
        assume("/" not in provider)
        ref_str = f"secret://{provider}/{key}"
        ref = parse_secret_reference(ref_str)
        assert ref is not None
        assert ref.provider == provider
        assert ref.key == key

    @given(
        text=st.text(min_size=0, max_size=100).filter(
            lambda s: not s.startswith("secret://")
        )
    )
    def test_non_secret_strings_return_none(self, text: str) -> None:
        assert parse_secret_reference(text) is None

    @given(provider=_identifier)
    def test_empty_key_returns_none(self, provider: str) -> None:
        assert parse_secret_reference(f"secret://{provider}/") is None


# ---------------------------------------------------------------------------
# SchemaField data type coercion
# ---------------------------------------------------------------------------


class TestSchemaFieldProperties:
    @given(dt=_data_types)
    def test_enum_value_string_coerces_to_enum(self, dt: DataType) -> None:
        field = SchemaField(name="col", data_type=dt.value)
        assert field.data_type is dt

    @given(dt=_data_types)
    def test_enum_stays_enum(self, dt: DataType) -> None:
        field = SchemaField(name="col", data_type=dt)
        assert field.data_type is dt

    @given(
        s=st.text(min_size=1, max_size=20).filter(
            lambda s: s not in {dt.value for dt in DataType}
        )
    )
    def test_unknown_string_stays_string(self, s: str) -> None:
        field = SchemaField(name="col", data_type=s)
        assert field.data_type == s
        assert isinstance(field.data_type, str)


# ---------------------------------------------------------------------------
# SchemaValidator — superset/subset properties
# ---------------------------------------------------------------------------


class TestSchemaValidatorProperties:
    @given(fields=st.lists(_schema_field, min_size=1, max_size=10))
    def test_identical_schemas_are_compatible(
        self, fields: list[SchemaField]
    ) -> None:
        # Deduplicate field names
        seen: set[str] = set()
        unique: list[SchemaField] = []
        for f in fields:
            if f.name not in seen:
                seen.add(f.name)
                unique.append(f)
        assume(len(unique) >= 1)

        schema = SchemaDefinition(fields=unique)
        result = SchemaValidator().validate(schema, schema, "src", "tgt")
        # Identical schemas should have no errors (may have warnings=0 too)
        assert result.valid

    @given(
        output_fields=st.lists(_schema_field, min_size=1, max_size=5),
        extra_fields=st.lists(_schema_field, min_size=1, max_size=3),
    )
    def test_superset_output_is_valid(
        self,
        output_fields: list[SchemaField],
        extra_fields: list[SchemaField],
    ) -> None:
        """Output having extra fields beyond input is valid (with warnings)."""
        # Deduplicate
        seen: set[str] = set()
        deduped_input: list[SchemaField] = []
        for f in output_fields:
            if f.name not in seen:
                seen.add(f.name)
                deduped_input.append(f)
        assume(len(deduped_input) >= 1)

        # Build output with extra fields that don't collide
        deduped_output = list(deduped_input)
        for f in extra_fields:
            if f.name not in seen:
                seen.add(f.name)
                deduped_output.append(f)

        input_schema = SchemaDefinition(fields=deduped_input)
        output_schema = SchemaDefinition(fields=deduped_output)
        result = SchemaValidator().validate(output_schema, input_schema, "src", "tgt")
        # Output is a superset of input — always valid (extras are just warnings)
        assert result.valid

    def test_none_schemas_valid_non_strict(self) -> None:
        result = SchemaValidator().validate(None, None, "src", "tgt")
        assert result.valid

    def test_none_schemas_invalid_strict(self) -> None:
        result = SchemaValidator().validate(None, None, "src", "tgt", strict=True)
        assert not result.valid


# ---------------------------------------------------------------------------
# Topological sort
# ---------------------------------------------------------------------------

_SPARK = SparkConfig(app_name="prop-test")


class TestTopologicalSortProperties:
    @given(n=st.integers(min_value=1, max_value=10))
    def test_linear_chain_preserves_order(self, n: int) -> None:
        """A → B → C → ... always produces alphabetical order."""
        names = [f"c{i:02d}" for i in range(n)]
        comps = []
        for i, name in enumerate(names):
            deps = [names[i - 1]] if i > 0 else []
            comps.append(
                ComponentConfig(
                    name=name,
                    component_type=ComponentType.TRANSFORMATION,
                    class_path="some.Module",
                    depends_on=deps,
                )
            )
        config = PipelineConfig(
            name="topo-test", version="1.0.0", spark=_SPARK, components=comps
        )
        order = config.get_execution_order()
        assert order == names

    @given(n=st.integers(min_value=1, max_value=8))
    def test_output_contains_all_components(self, n: int) -> None:
        """Independent components all appear in output."""
        comps = [
            ComponentConfig(
                name=f"c{i}",
                component_type=ComponentType.TRANSFORMATION,
                class_path="some.Module",
            )
            for i in range(n)
        ]
        config = PipelineConfig(
            name="topo-test", version="1.0.0", spark=_SPARK, components=comps
        )
        order = config.get_execution_order()
        assert set(order) == {f"c{i}" for i in range(n)}
        assert len(order) == n

    @given(n=st.integers(min_value=2, max_value=8))
    def test_dependencies_respected_in_output(self, n: int) -> None:
        """For every edge A→B, A appears before B."""
        # Star topology: c0 is root, all others depend on c0
        comps = [
            ComponentConfig(
                name="c0",
                component_type=ComponentType.TRANSFORMATION,
                class_path="some.Module",
            )
        ]
        for i in range(1, n):
            comps.append(
                ComponentConfig(
                    name=f"c{i}",
                    component_type=ComponentType.TRANSFORMATION,
                    class_path="some.Module",
                    depends_on=["c0"],
                )
            )
        config = PipelineConfig(
            name="topo-test", version="1.0.0", spark=_SPARK, components=comps
        )
        order = config.get_execution_order()
        c0_idx = order.index("c0")
        for i in range(1, n):
            assert order.index(f"c{i}") > c0_idx


# ---------------------------------------------------------------------------
# ResiliencePolicy on ComponentConfig
# ---------------------------------------------------------------------------


class TestResiliencePolicyProperties:
    @given(config=_valid_retry, cb=_valid_cb)
    def test_resilience_populates_both(
        self, config: RetryConfig, cb: CircuitBreakerConfig
    ) -> None:
        policy = ResiliencePolicy(retry=config, circuit_breaker=cb)
        comp = ComponentConfig(
            name="test",
            component_type=ComponentType.TRANSFORMATION,
            class_path="some.Module",
            resilience=policy,
        )
        assert comp.retry is config
        assert comp.circuit_breaker is cb

    @given(config=_valid_retry)
    def test_resilience_with_individual_retry_raises(
        self, config: RetryConfig
    ) -> None:
        with pytest.raises(ValueError, match="Cannot set both"):
            ComponentConfig(
                name="test",
                component_type=ComponentType.TRANSFORMATION,
                class_path="some.Module",
                resilience=ResiliencePolicies.DEFAULT,
                retry=config,
            )

    @given(cb=_valid_cb)
    def test_resilience_with_individual_cb_raises(
        self, cb: CircuitBreakerConfig
    ) -> None:
        with pytest.raises(ValueError, match="Cannot set both"):
            ComponentConfig(
                name="test",
                component_type=ComponentType.TRANSFORMATION,
                class_path="some.Module",
                resilience=ResiliencePolicies.DEFAULT,
                circuit_breaker=cb,
            )
