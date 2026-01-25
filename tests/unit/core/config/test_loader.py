"""Tests for HOCON configuration loader functions."""

from pathlib import Path

from pyspark_pipeline_framework.core.config import (
    PipelineConfig,
    SparkConfig,
    load_from_file,
    load_from_string,
)


class TestLoadFromFile:
    """Tests for load_from_file function."""

    def test_load_from_file_basic(self, tmp_path: Path) -> None:
        """Test loading configuration from HOCON file."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
            {
              name: "test-pipeline"
              version: "1.0.0"
              environment: dev
              mode: batch

              spark {
                app_name: "test-app"
                master: "local[*]"
              }

              components: [
                {
                  name: "source"
                  component_type: source
                  class_path: "com.example.Source"
                  config: {}
                }
              ]
            }
            """
        )

        config = load_from_file(str(config_file), PipelineConfig)

        assert config.name == "test-pipeline"
        assert config.version == "1.0.0"
        assert config.environment.value == "dev"
        assert config.mode.value == "batch"
        assert config.spark.app_name == "test-app"
        assert config.spark.master == "local[*]"
        assert len(config.components) == 1

    def test_load_from_file_with_components(self, tmp_path: Path) -> None:
        """Test loading configuration with multiple components."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
            {
              name: "test-pipeline"
              version: "1.0.0"
              environment: dev
              mode: batch

              spark {
                app_name: "test-app"
                master: "local[*]"
              }

              components: [
                {
                  name: "reader"
                  component_type: source
                  class_path: "com.example.Reader"
                  config: {}
                },
                {
                  name: "transformer"
                  component_type: transformation
                  class_path: "com.example.Transformer"
                  config: {}
                  depends_on: ["reader"]
                }
              ]
            }
            """
        )

        config = load_from_file(str(config_file), PipelineConfig)

        assert len(config.components) == 2
        assert config.components[0].name == "reader"
        assert config.components[1].name == "transformer"
        assert config.components[1].depends_on == ["reader"]

    def test_load_spark_config_from_file(self, tmp_path: Path) -> None:
        """Test loading SparkConfig from HOCON file."""
        config_file = tmp_path / "spark.conf"
        config_file.write_text(
            """
            {
              app_name: "test-app"
              master: "yarn"
              deploy_mode: cluster
              executor_cores: 4
            }
            """
        )

        config = load_from_file(str(config_file), SparkConfig)

        assert config.app_name == "test-app"
        assert config.master == "yarn"
        assert config.deploy_mode.value == "cluster"
        assert config.executor_cores == 4


class TestLoadFromString:
    """Tests for load_from_string function."""

    def test_load_from_string_basic(self) -> None:
        """Test loading configuration from HOCON string."""
        hocon_str = """
        {
          name: "test-pipeline"
          version: "1.0.0"
          environment: dev
          mode: batch

          spark {
            app_name: "test-app"
            master: "local[*]"
          }

          components: [
            {
              name: "source"
              component_type: source
              class_path: "com.example.Source"
              config: {}
            }
          ]
        }
        """

        config = load_from_string(hocon_str, PipelineConfig)

        assert config.name == "test-pipeline"
        assert config.version == "1.0.0"
        assert config.spark.app_name == "test-app"

    def test_load_spark_config_from_string(self) -> None:
        """Test loading SparkConfig from HOCON string."""
        hocon_str = """
        {
          app_name: "my-app"
          master: "local[4]"
          executor_memory: "8g"
        }
        """

        config = load_from_string(hocon_str, SparkConfig)

        assert config.app_name == "my-app"
        assert config.master == "local[4]"
        assert config.executor_memory == "8g"


class TestLoaderIntegration:
    """Integration tests for loader functions."""

    def test_file_and_string_equivalence(self, tmp_path: Path) -> None:
        """Test that file and string loading produce equivalent results."""
        hocon_content = """
        {
          name: "test-pipeline"
          version: "1.0.0"
          environment: dev
          mode: batch

          spark {
            app_name: "test-app"
            master: "local[*]"
          }

          components: [
            {
              name: "source"
              component_type: source
              class_path: "com.example.Source"
              config: {}
            }
          ]
        }
        """

        # Load from string
        config_from_string = load_from_string(hocon_content, PipelineConfig)

        # Load from file
        config_file = tmp_path / "test.conf"
        config_file.write_text(hocon_content)
        config_from_file = load_from_file(str(config_file), PipelineConfig)

        # Compare
        assert config_from_string.name == config_from_file.name
        assert config_from_string.version == config_from_file.version
        assert config_from_string.environment == config_from_file.environment
        assert config_from_string.spark.app_name == config_from_file.spark.app_name

    def test_loader_with_complete_config(self, tmp_path: Path) -> None:
        """Test loading complete configuration with all features."""
        config_file = tmp_path / "complete.conf"
        config_file.write_text(
            """
            {
              name: "complete-pipeline"
              version: "1.0.0"
              environment: prod
              mode: streaming
              tags: {
                team: "data-engineering"
                cost_center: "analytics"
              }

              spark {
                app_name: "complete-app"
                master: "yarn"
                deploy_mode: cluster
                executor_memory: "4g"
                executor_cores: 4
                num_executors: 10
                driver_memory: "2g"
                spark_conf: {
                  "spark.sql.adaptive.enabled": "true"
                  "spark.dynamicAllocation.enabled": "true"
                }
              }

              components: [
                {
                  name: "source"
                  component_type: source
                  class_path: "com.example.Source"
                  config: {
                    path: "/data/input"
                  }
                }
              ]

              hooks {
                logging {
                  level: INFO
                  format: json
                }
                metrics {
                  enabled: true
                  backend: prometheus
                }
              }
            }
            """
        )

        config = load_from_file(str(config_file), PipelineConfig)

        # Verify all sections loaded correctly
        assert config.name == "complete-pipeline"
        assert config.environment.value == "prod"
        assert config.mode.value == "streaming"
        assert len(config.tags) == 2
        assert config.spark.executor_cores == 4
        assert len(config.components) == 1
        assert config.hooks.logging.level.value == "INFO"
        assert config.hooks.metrics.enabled is True
