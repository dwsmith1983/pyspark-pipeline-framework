# pyspark-pipeline-framework

[![CI](https://github.com/dwsmith1983/pyspark-pipeline-framework/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/dwsmith1983/pyspark-pipeline-framework/actions/workflows/ci.yml)
[![Documentation Status](https://readthedocs.org/projects/pyspark-pipeline-framework/badge/?version=latest)](https://pyspark-pipeline-framework.readthedocs.io/en/latest/)
[![PyPI version](https://img.shields.io/pypi/v/pyspark-pipeline-framework)](https://pypi.org/project/pyspark-pipeline-framework/)
[![Production Ready](https://img.shields.io/badge/status-production--ready-brightgreen)](https://github.com/dwsmith1983/pyspark-pipeline-framework)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Configuration-driven PySpark pipeline framework with HOCON configuration,
resilience patterns, lifecycle hooks, and streaming support.

> **Scala/JVM users** may also be interested in
> [spark-pipeline-framework](https://github.com/dwsmith1983/spark-pipeline-framework),
> the Scala implementation of this framework using PureConfig and Typesafe Config.
> You can find it on [GitHub](https://github.com/dwsmith1983/spark-pipeline-framework)
> and [Maven Central](https://central.sonatype.com/namespace/io.github.dwsmith1983).

## Installation

```bash
pip install pyspark-pipeline-framework
```

For development:

```bash
git clone https://github.com/dwsmith1983/pyspark-pipeline-framework.git
cd pyspark-pipeline-framework
pip install -e ".[dev]"
```

## Quick Start

```python
from pyspark_pipeline_framework.runner import SimplePipelineRunner

# Load pipeline from HOCON config and run
runner = SimplePipelineRunner.from_file("pipeline.conf")
result = runner.run()

print(result.status)            # PipelineResultStatus.SUCCESS
print(result.total_duration_ms) # 1234
```

## Batch Pipelines

### Creating Components

Extend ``DataFlow`` and implement ``name`` and ``run()``:

```python
from pyspark_pipeline_framework.runtime.dataflow.base import DataFlow


class MyTransform(DataFlow):
    def __init__(self, output_view: str) -> None:
        super().__init__()
        self._output_view = output_view

    @property
    def name(self) -> str:
        return "MyTransform"

    @classmethod
    def from_config(cls, config: dict) -> "MyTransform":
        return cls(**config)

    def run(self) -> None:
        df = self.spark.sql("SELECT id, UPPER(name) AS name FROM raw")
        df.createOrReplaceTempView(self._output_view)
```

The framework injects a ``SparkSession`` via ``set_spark_session()`` before
calling ``run()``. Access it through ``self.spark``.

### Built-in Example Components

Three reference components are included:

- ``ReadTable`` -- reads a table and registers a temp view
- ``SqlTransform`` -- executes SQL and registers the result
- ``WriteTable`` -- writes a temp view to a table

```python
from pyspark_pipeline_framework.examples.batch import (
    ReadTable, ReadTableConfig,
    SqlTransform, SqlTransformConfig,
    WriteTable, WriteTableConfig,
)
```

### HOCON Configuration

```hocon
{
  name: "customer-etl"
  version: "1.0.0"

  spark {
    app_name: "Customer ETL"
    master: "local[*]"
  }

  components: [
    {
      name: "read_raw"
      component_type: source
      class_path: "pyspark_pipeline_framework.examples.batch.ReadTable"
      config {
        table_name: "raw.customers"
        output_view: "raw_customers"
      }
    },
    {
      name: "transform"
      component_type: transformation
      class_path: "pyspark_pipeline_framework.examples.batch.SqlTransform"
      depends_on: ["read_raw"]
      config {
        sql: "SELECT id, UPPER(name) AS name FROM raw_customers"
        output_view: "cleaned"
      }
    },
    {
      name: "write"
      component_type: sink
      class_path: "pyspark_pipeline_framework.examples.batch.WriteTable"
      depends_on: ["transform"]
      config {
        input_view: "cleaned"
        output_table: "curated.customers"
      }
    }
  ]
}
```

## Streaming Pipelines

### Creating a Streaming Pipeline

Extend ``StreamingPipeline`` and provide a source, sink, and optional
transform:

```python
from pyspark_pipeline_framework.runtime.streaming.base import (
    StreamingPipeline, StreamingSource, StreamingSink,
    TriggerConfig, TriggerType,
)
from pyspark_pipeline_framework.runtime.streaming.sources import (
    KafkaStreamingSource,
)
from pyspark_pipeline_framework.runtime.streaming.sinks import (
    DeltaStreamingSink,
)


class EventIngestion(StreamingPipeline):
    def __init__(self) -> None:
        super().__init__()
        self._source = KafkaStreamingSource(
            bootstrap_servers="broker:9092", topics="events",
        )
        self._sink = DeltaStreamingSink(
            path="/data/delta/events",
            checkpoint_location="/checkpoints/events",
        )

    @property
    def name(self) -> str:
        return "EventIngestion"

    @property
    def source(self) -> StreamingSource:
        return self._source

    @property
    def sink(self) -> StreamingSink:
        return self._sink

    @property
    def trigger(self) -> TriggerConfig:
        return TriggerConfig(TriggerType.PROCESSING_TIME, "30 seconds")

    def transform(self, df):
        # Parse JSON value from Kafka
        return df.selectExpr("CAST(value AS STRING) AS raw_json")
```

### Built-in Streaming Components

**Sources:** ``KafkaStreamingSource``, ``FileStreamingSource``,
``DeltaStreamingSource``, ``IcebergStreamingSource``, ``RateStreamingSource``

**Sinks:** ``KafkaStreamingSink``, ``DeltaStreamingSink``,
``ConsoleStreamingSink``, ``IcebergStreamingSink``, ``FileStreamingSink``

### Example Pipelines

```python
from pyspark_pipeline_framework.examples.streaming import (
    FileToConsolePipeline,
    KafkaToDeltaPipeline,
)
```

### Running a Stream

```python
pipeline.set_spark_session(spark)

# Blocking -- runs until terminated
pipeline.run()

# Non-blocking -- returns StreamingQuery handle
query = pipeline.start_stream()
query.awaitTermination(timeout=60)
```

## Resilience

### Retry Policy

Configure per-component retries with exponential backoff:

```hocon
components: [
  {
    name: "flaky_source"
    component_type: source
    class_path: "my.module.FlakySource"
    retry {
      max_attempts: 3
      initial_delay_seconds: 1.0
      max_delay_seconds: 30.0
      backoff_multiplier: 2.0
    }
  }
]
```

### Circuit Breaker

Prevent repeated calls to failing components:

```hocon
components: [
  {
    name: "external_api"
    component_type: source
    class_path: "my.module.ApiSource"
    circuit_breaker {
      failure_threshold: 5
      timeout_seconds: 60.0
    }
  }
]
```

## Lifecycle Hooks

Hooks receive callbacks at pipeline and component lifecycle events.
Combine multiple hooks with ``CompositeHooks``:

```python
from pyspark_pipeline_framework.runner import (
    CompositeHooks, LoggingHooks, MetricsHooks,
    SimplePipelineRunner,
)

hooks = CompositeHooks(LoggingHooks(), MetricsHooks())
runner = SimplePipelineRunner(config, hooks=hooks)
result = runner.run()
```

### Available Hooks

- ``LoggingHooks`` -- logs lifecycle events
- ``MetricsHooks`` -- collects timing and retry counts
- ``DataQualityHooks`` -- runs data quality checks at lifecycle points
- ``AuditHooks`` -- emits audit events for compliance
- ``CheckpointHooks`` -- saves checkpoint state for resume

### Data Quality Checks

```python
from pyspark_pipeline_framework.core.quality import row_count_check, null_check
from pyspark_pipeline_framework.runner import DataQualityHooks, CompositeHooks

dq = DataQualityHooks(spark_wrapper)
dq.register(row_count_check("curated.customers", min_rows=100))
dq.register(null_check("curated.customers", "email", max_null_pct=5.0))

hooks = CompositeHooks(LoggingHooks(), dq)
runner = SimplePipelineRunner(config, hooks=hooks)
```

### Audit Trail

```python
from pyspark_pipeline_framework.core.audit import (
    LoggingAuditSink, FileAuditSink, CompositeAuditSink,
)
from pyspark_pipeline_framework.runner import AuditHooks, CompositeHooks

audit_sink = CompositeAuditSink(
    LoggingAuditSink(),
    FileAuditSink("/var/log/pipeline-audit.jsonl"),
)
hooks = CompositeHooks(LoggingHooks(), AuditHooks(audit_sink))
```

## Secrets Management

```python
from pyspark_pipeline_framework.core.secrets import (
    EnvSecretsProvider, SecretsResolver, SecretsCache, SecretsReference,
)

resolver = SecretsResolver()
resolver.register(EnvSecretsProvider())

cache = SecretsCache(resolver, ttl_seconds=300)

result = cache.resolve(SecretsReference(provider="env", key="DB_PASSWORD"))
if result.value:
    print("Secret resolved successfully")
```

Providers: ``EnvSecretsProvider``, ``AwsSecretsProvider``,
``VaultSecretsProvider``

## Checkpoint and Resume

Resume pipelines from the last successful component:

```python
from pyspark_pipeline_framework.runner import (
    LocalCheckpointStore, CheckpointHooks, CompositeHooks,
    compute_pipeline_fingerprint, load_checkpoint_for_resume,
)

store = LocalCheckpointStore(Path("/tmp/checkpoints"))
fingerprint = compute_pipeline_fingerprint(config)
checkpoint_hooks = CheckpointHooks(store, run_id="run-001", pipeline_fingerprint=fingerprint)

hooks = CompositeHooks(LoggingHooks(), checkpoint_hooks)
runner = SimplePipelineRunner(config, hooks=hooks)

# First run
result = runner.run()

# Resume after failure
completed = load_checkpoint_for_resume(store, "run-001", config)
result = runner.run(completed_components=completed)
```

## Testing Components

Use ``MagicMock`` for the ``SparkSession``:

```python
from unittest.mock import MagicMock
from my_project.components import MyTransform


def test_my_transform():
    spark = MagicMock()
    df = MagicMock()
    spark.sql.return_value = df

    comp = MyTransform(output_view="result")
    comp.set_spark_session(spark)
    comp.run()

    spark.sql.assert_called_once()
    df.createOrReplaceTempView.assert_called_once_with("result")
```

## Configuration Reference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| ``name`` | string | yes | Pipeline name |
| ``version`` | string | yes | Pipeline version |
| ``spark.app_name`` | string | yes | Spark application name |
| ``spark.master`` | string | no | Spark master URL |
| ``components[].name`` | string | yes | Unique component name |
| ``components[].component_type`` | enum | yes | ``source``, ``transformation``, ``sink`` |
| ``components[].class_path`` | string | yes | Python class to instantiate |
| ``components[].config`` | object | no | Component-specific configuration |
| ``components[].depends_on`` | list | no | Names of prerequisite components |
| ``components[].enabled`` | bool | no | Enable/disable (default: true) |
| ``components[].retry`` | object | no | Retry policy configuration |
| ``components[].circuit_breaker`` | object | no | Circuit breaker configuration |

## License

Apache License 2.0
