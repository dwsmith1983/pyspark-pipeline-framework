"""Example of loading HOCON configuration using dataconf."""

from pathlib import Path

from pyspark_pipeline_framework.core.config import PipelineConfig, load_from_file


def main() -> None:
    """Load and print pipeline configuration from HOCON file."""
    # Path to the HOCON configuration file
    config_path = str(Path(__file__).parent / "pipeline.conf")

    # Load configuration using the loader function
    config = load_from_file(config_path, PipelineConfig)

    # Print configuration details
    print(f"Pipeline: {config.name} v{config.version}")
    print(f"Environment: {config.environment.value}")
    print(f"Mode: {config.mode.value}")
    print(f"\nSpark App: {config.spark.app_name}")
    print(f"Master: {config.spark.master}")

    print(f"\nComponents ({len(config.components)}):")
    for component in config.components:
        print(f"  - {component.name} ({component.component_type.value})")
        if component.depends_on:
            print(f"    Depends on: {', '.join(component.depends_on)}")

    print(f"\nExecution order:")
    execution_order = config.get_execution_order()
    for i, component_name in enumerate(execution_order, 1):
        print(f"  {i}. {component_name}")

    # Convert Spark config to dictionary
    print(f"\nSpark Configuration:")
    spark_conf = config.spark.to_spark_conf_dict()
    for key, value in sorted(spark_conf.items()):
        print(f"  {key} = {value}")

    print(f"\nTags:")
    for key, value in config.tags.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
