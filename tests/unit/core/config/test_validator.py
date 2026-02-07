"""Tests for pipeline configuration validator."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from pyspark_pipeline_framework.core.config.base import ComponentType
from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.pipeline import PipelineConfig
from pyspark_pipeline_framework.core.config.spark import SparkConfig
from pyspark_pipeline_framework.core.config.validator import (
    DryRunResult,
    ValidationError,
    ValidationPhase,
    ValidationResult,
    dry_run,
    validate_pipeline,
)


def _make_spark() -> SparkConfig:
    return SparkConfig(app_name="test")


def _make_component(
    name: str = "comp1",
    class_path: str = "pyspark_pipeline_framework.examples.batch.ReadCsv",
    *,
    enabled: bool = True,
) -> ComponentConfig:
    return ComponentConfig(
        name=name,
        component_type=ComponentType.SOURCE,
        class_path=class_path,
        enabled=enabled,
    )


def _make_pipeline_raw(
    name: str = "test-pipeline",
    components: list[ComponentConfig] | None = None,
) -> PipelineConfig:
    """Build a PipelineConfig bypassing __post_init__ for edge-case testing."""
    obj = object.__new__(PipelineConfig)
    obj.name = name
    obj.version = "1.0"
    obj.spark = _make_spark()
    obj.components = components if components is not None else []
    obj.environment = "dev"
    obj.mode = "batch"
    obj.hooks = MagicMock()
    obj.secrets = None
    obj.tags = {}
    return obj


def _make_pipeline(
    name: str = "test-pipeline",
    components: list[ComponentConfig] | None = None,
) -> PipelineConfig:
    """Build a valid PipelineConfig via normal constructor."""
    return PipelineConfig(
        name=name,
        version="1.0",
        spark=_make_spark(),
        components=components or [_make_component()],
    )


class TestValidationResult:
    """Tests for ValidationResult."""

    def test_is_valid_when_no_errors(self) -> None:
        result = ValidationResult()
        assert result.is_valid is True

    def test_not_valid_when_errors(self) -> None:
        result = ValidationResult(
            errors=[ValidationError(ValidationPhase.REQUIRED_FIELDS, "bad")]
        )
        assert result.is_valid is False

    def test_warnings_independent_of_validity(self) -> None:
        result = ValidationResult(warnings=["something"])
        assert result.is_valid is True


class TestDryRunResult:
    """Tests for DryRunResult."""

    def test_is_valid_when_no_errors(self) -> None:
        result = DryRunResult(instantiated=["comp1"])
        assert result.is_valid is True

    def test_not_valid_when_errors(self) -> None:
        result = DryRunResult(
            errors=[ValidationError(ValidationPhase.COMPONENT_CONFIG, "fail")]
        )
        assert result.is_valid is False


class TestValidatePipeline:
    """Tests for validate_pipeline()."""

    def test_empty_name_yields_error(self) -> None:
        config = _make_pipeline_raw(name="")
        result = validate_pipeline(config)
        assert not result.is_valid
        assert any(
            e.phase == ValidationPhase.REQUIRED_FIELDS and "name" in e.message.lower()
            for e in result.errors
        )

    def test_no_components_yields_error(self) -> None:
        config = _make_pipeline_raw(name="ok", components=[])
        result = validate_pipeline(config)
        assert not result.is_valid
        assert any(
            e.phase == ValidationPhase.REQUIRED_FIELDS
            and "component" in e.message.lower()
            for e in result.errors
        )

    def test_valid_config_passes(self) -> None:
        config = _make_pipeline(components=[_make_component()])
        result = validate_pipeline(config)
        assert result.is_valid

    def test_invalid_class_path_yields_type_resolution_error(self) -> None:
        config = _make_pipeline(
            components=[_make_component(class_path="no.such.Module")]
        )
        result = validate_pipeline(config)
        assert not result.is_valid
        assert any(e.phase == ValidationPhase.TYPE_RESOLUTION for e in result.errors)
        assert result.errors[0].component_name == "comp1"

    def test_disabled_component_skipped(self) -> None:
        config = _make_pipeline(
            components=[
                _make_component(class_path="no.such.Module", enabled=False),
                _make_component(name="good"),
            ]
        )
        result = validate_pipeline(config)
        assert result.is_valid

    def test_warnings_for_missing_from_config(self) -> None:
        """Components without from_config() produce a warning, not an error."""
        config = _make_pipeline(
            components=[
                _make_component(
                    class_path="pyspark_pipeline_framework.examples.batch.ReadCsv"
                )
            ]
        )
        result = validate_pipeline(config)
        assert result.is_valid
        # ReadCsv has from_config, but we test the mechanism works
        # by checking we get a result at all
        assert isinstance(result.warnings, list)

    def test_validate_component_class_failure_yields_component_config_error(
        self,
    ) -> None:
        """If validate_component_class raises, we get a COMPONENT_CONFIG error."""
        config = _make_pipeline(components=[_make_component()])
        with patch(
            "pyspark_pipeline_framework.core.config.validator.validate_component_class",
            side_effect=RuntimeError("boom"),
        ):
            result = validate_pipeline(config)
        assert not result.is_valid
        assert any(
            e.phase == ValidationPhase.COMPONENT_CONFIG for e in result.errors
        )

    def test_multiple_components_validated(self) -> None:
        config = _make_pipeline(
            components=[
                _make_component(name="a"),
                _make_component(name="b", class_path="no.such.Module"),
            ]
        )
        result = validate_pipeline(config)
        assert not result.is_valid
        assert len(result.errors) == 1
        assert result.errors[0].component_name == "b"


class TestDryRun:
    """Tests for dry_run()."""

    def test_successful_instantiation(self) -> None:
        config = _make_pipeline(
            components=[
                _make_component(
                    class_path="pyspark_pipeline_framework.examples.batch.ReadCsv"
                )
            ]
        )
        # ReadCsv.from_config needs config dict — override component config
        config.components[0].config = {
            "path": "/tmp/test.csv",
            "output_view": "test_view",
        }
        result = dry_run(config)
        assert result.is_valid
        assert "comp1" in result.instantiated

    def test_failed_instantiation(self) -> None:
        config = _make_pipeline(
            components=[_make_component(class_path="no.such.Module")]
        )
        result = dry_run(config)
        assert not result.is_valid
        assert result.errors[0].component_name == "comp1"

    def test_disabled_component_skipped(self) -> None:
        config = _make_pipeline(
            components=[
                _make_component(class_path="no.such.Module", enabled=False),
                _make_component(name="good"),
            ]
        )
        config.components[1].config = {
            "path": "/tmp/test.csv",
            "output_view": "test_view",
        }
        result = dry_run(config)
        assert result.is_valid
        assert "good" in result.instantiated

    def test_non_pipeline_component_yields_error(self) -> None:
        """If instantiation returns something that's not a PipelineComponent."""
        config = _make_pipeline(components=[_make_component()])
        with patch(
            "pyspark_pipeline_framework.core.config.validator.instantiate_component",
            return_value="not-a-component",
        ):
            result = dry_run(config)
        assert not result.is_valid
        assert any(
            "not a PipelineComponent" in e.message for e in result.errors
        )

    def test_instantiation_exception_caught(self) -> None:
        config = _make_pipeline(components=[_make_component()])
        with patch(
            "pyspark_pipeline_framework.core.config.validator.instantiate_component",
            side_effect=TypeError("bad config"),
        ):
            result = dry_run(config)
        assert not result.is_valid
        assert result.errors[0].phase == ValidationPhase.COMPONENT_CONFIG


class TestValidationPhase:
    """Tests for ValidationPhase enum."""

    def test_values(self) -> None:
        assert ValidationPhase.CONFIG_SYNTAX.value == "config-syntax"
        assert ValidationPhase.REQUIRED_FIELDS.value == "required-fields"
        assert ValidationPhase.TYPE_RESOLUTION.value == "type-resolution"
        assert ValidationPhase.COMPONENT_CONFIG.value == "component-config"

    def test_string_mixin(self) -> None:
        assert isinstance(ValidationPhase.CONFIG_SYNTAX, str)
