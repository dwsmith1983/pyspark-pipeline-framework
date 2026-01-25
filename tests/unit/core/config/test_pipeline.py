"""Tests for pipeline configuration models."""

import pytest

from pyspark_pipeline_framework.core.config.base import (
    ComponentType,
    Environment,
    PipelineMode,
)
from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.hooks import HooksConfig
from pyspark_pipeline_framework.core.config.pipeline import PipelineConfig
from pyspark_pipeline_framework.core.config.spark import SparkConfig


class TestPipelineConfig:
    """Tests for PipelineConfig."""

    def test_minimal_config(self) -> None:
        """Test minimal required configuration."""
        spark = SparkConfig(app_name="test-app")
        components = [
            ComponentConfig(
                name="source",
                component_type=ComponentType.SOURCE,
                class_path="myapp.Source",
            )
        ]

        config = PipelineConfig(name="test-pipeline", version="1.0.0", spark=spark, components=components)

        assert config.name == "test-pipeline"
        assert config.version == "1.0.0"
        assert config.environment == Environment.DEV
        assert config.mode == PipelineMode.BATCH
        assert config.spark == spark
        assert config.components == components
        assert isinstance(config.hooks, HooksConfig)
        assert config.secrets is None
        assert config.tags == {}

    def test_full_config(self) -> None:
        """Test full configuration with all options."""
        spark = SparkConfig(app_name="test-app")
        components = [
            ComponentConfig(
                name="source",
                component_type=ComponentType.SOURCE,
                class_path="myapp.Source",
            ),
            ComponentConfig(
                name="transform",
                component_type=ComponentType.TRANSFORMATION,
                class_path="myapp.Transform",
                depends_on=["source"],
            ),
        ]
        hooks = HooksConfig()

        config = PipelineConfig(
            name="test-pipeline",
            version="2.0.0",
            spark=spark,
            components=components,
            environment=Environment.PROD,
            mode=PipelineMode.STREAMING,
            hooks=hooks,
            tags={"team": "data", "project": "analytics"},
        )

        assert config.name == "test-pipeline"
        assert config.version == "2.0.0"
        assert config.environment == Environment.PROD
        assert config.mode == PipelineMode.STREAMING
        assert config.tags == {"team": "data", "project": "analytics"}

    def test_validation_name_required(self) -> None:
        """Test validation for required name."""
        with pytest.raises(ValueError, match="name is required"):
            PipelineConfig(
                name="",
                version="1.0.0",
                spark=SparkConfig(app_name="test"),
                components=[
                    ComponentConfig(
                        name="c1",
                        component_type=ComponentType.SOURCE,
                        class_path="myapp.C1",
                    )
                ],
            )

    def test_validation_version_required(self) -> None:
        """Test validation for required version."""
        with pytest.raises(ValueError, match="version is required"):
            PipelineConfig(
                name="test",
                version="",
                spark=SparkConfig(app_name="test"),
                components=[
                    ComponentConfig(
                        name="c1",
                        component_type=ComponentType.SOURCE,
                        class_path="myapp.C1",
                    )
                ],
            )

    def test_validation_components_required(self) -> None:
        """Test validation for at least one component."""
        with pytest.raises(ValueError, match="At least one component is required"):
            PipelineConfig(
                name="test",
                version="1.0.0",
                spark=SparkConfig(app_name="test"),
                components=[],
            )

    def test_validation_unique_component_names(self) -> None:
        """Test validation for unique component names."""
        with pytest.raises(ValueError, match="Component names must be unique"):
            PipelineConfig(
                name="test",
                version="1.0.0",
                spark=SparkConfig(app_name="test"),
                components=[
                    ComponentConfig(
                        name="duplicate",
                        component_type=ComponentType.SOURCE,
                        class_path="myapp.C1",
                    ),
                    ComponentConfig(
                        name="duplicate",
                        component_type=ComponentType.SINK,
                        class_path="myapp.C2",
                    ),
                ],
            )

    def test_validation_dependency_exists(self) -> None:
        """Test validation that dependencies reference existing components."""
        with pytest.raises(ValueError, match="depends on unknown component"):
            PipelineConfig(
                name="test",
                version="1.0.0",
                spark=SparkConfig(app_name="test"),
                components=[
                    ComponentConfig(
                        name="c1",
                        component_type=ComponentType.TRANSFORMATION,
                        class_path="myapp.C1",
                        depends_on=["nonexistent"],
                    )
                ],
            )

    def test_validation_circular_dependencies(self) -> None:
        """Test validation against circular dependencies."""
        with pytest.raises(ValueError, match="Circular dependency detected"):
            PipelineConfig(
                name="test",
                version="1.0.0",
                spark=SparkConfig(app_name="test"),
                components=[
                    ComponentConfig(
                        name="c1",
                        component_type=ComponentType.TRANSFORMATION,
                        class_path="myapp.C1",
                        depends_on=["c2"],
                    ),
                    ComponentConfig(
                        name="c2",
                        component_type=ComponentType.TRANSFORMATION,
                        class_path="myapp.C2",
                        depends_on=["c1"],
                    ),
                ],
            )

    def test_get_component_found(self) -> None:
        """Test getting a component by name."""
        components = [
            ComponentConfig(
                name="source",
                component_type=ComponentType.SOURCE,
                class_path="myapp.Source",
            ),
            ComponentConfig(
                name="sink",
                component_type=ComponentType.SINK,
                class_path="myapp.Sink",
            ),
        ]
        config = PipelineConfig(
            name="test",
            version="1.0.0",
            spark=SparkConfig(app_name="test"),
            components=components,
        )

        component = config.get_component("source")
        assert component is not None
        assert component.name == "source"

    def test_get_component_not_found(self) -> None:
        """Test getting a non-existent component."""
        components = [
            ComponentConfig(
                name="source",
                component_type=ComponentType.SOURCE,
                class_path="myapp.Source",
            )
        ]
        config = PipelineConfig(
            name="test",
            version="1.0.0",
            spark=SparkConfig(app_name="test"),
            components=components,
        )

        component = config.get_component("nonexistent")
        assert component is None

    def test_get_execution_order_simple(self) -> None:
        """Test execution order for simple linear dependencies."""
        components = [
            ComponentConfig(
                name="c1",
                component_type=ComponentType.SOURCE,
                class_path="myapp.C1",
            ),
            ComponentConfig(
                name="c2",
                component_type=ComponentType.TRANSFORMATION,
                class_path="myapp.C2",
                depends_on=["c1"],
            ),
            ComponentConfig(
                name="c3",
                component_type=ComponentType.SINK,
                class_path="myapp.C3",
                depends_on=["c2"],
            ),
        ]
        config = PipelineConfig(
            name="test",
            version="1.0.0",
            spark=SparkConfig(app_name="test"),
            components=components,
        )

        order = config.get_execution_order()
        assert order == ["c1", "c2", "c3"]

    def test_get_execution_order_parallel(self) -> None:
        """Test execution order with parallel components."""
        components = [
            ComponentConfig(
                name="source",
                component_type=ComponentType.SOURCE,
                class_path="myapp.Source",
            ),
            ComponentConfig(
                name="transform1",
                component_type=ComponentType.TRANSFORMATION,
                class_path="myapp.T1",
                depends_on=["source"],
            ),
            ComponentConfig(
                name="transform2",
                component_type=ComponentType.TRANSFORMATION,
                class_path="myapp.T2",
                depends_on=["source"],
            ),
            ComponentConfig(
                name="sink",
                component_type=ComponentType.SINK,
                class_path="myapp.Sink",
                depends_on=["transform1", "transform2"],
            ),
        ]
        config = PipelineConfig(
            name="test",
            version="1.0.0",
            spark=SparkConfig(app_name="test"),
            components=components,
        )

        order = config.get_execution_order()
        assert order[0] == "source"
        assert set(order[1:3]) == {"transform1", "transform2"}
        assert order[3] == "sink"

    def test_get_execution_order_no_dependencies(self) -> None:
        """Test execution order with no dependencies."""
        components = [
            ComponentConfig(
                name="c1",
                component_type=ComponentType.SOURCE,
                class_path="myapp.C1",
            ),
            ComponentConfig(
                name="c2",
                component_type=ComponentType.SOURCE,
                class_path="myapp.C2",
            ),
        ]
        config = PipelineConfig(
            name="test",
            version="1.0.0",
            spark=SparkConfig(app_name="test"),
            components=components,
        )

        order = config.get_execution_order()
        assert set(order) == {"c1", "c2"}
        assert len(order) == 2
