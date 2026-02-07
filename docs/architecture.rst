Architecture
============

This page provides a visual overview of pyspark-pipeline-framework's
architecture, showing component relationships and data flow during pipeline
execution.

Component Architecture
----------------------

pyspark-pipeline-framework uses a layered architecture separating configuration,
component abstractions, runtime execution, and orchestration:

.. code-block:: text

   ┌─────────────────────────────────────────────────────────────────────┐
   │                         User Application                            │
   │                  (Python code or HOCON config file)                 │
   └─────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │                       Runner / Orchestration                        │
   ├─────────────────────────────────────────────────────────────────────┤
   │  SimplePipelineRunner   │  PipelineHooks       │  CheckpointStore   │
   │  (execution engine)     │  (lifecycle events)  │  (resume state)    │
   └─────────────────────────────────────────────────────────────────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    ▼                ▼                ▼
   ┌──────────────────┐  ┌───────────────────┐ ┌──────────────────────────┐
   │   Batch Pipeline │  │ Streaming Pipeline│ │  Hooks Implementations   │
   │                  │  │                   │ │                          │
   │  DataFlow        │  │  StreamingSource  │ │  LoggingHooks            │
   │  SchemaAware     │  │  StreamingSink    │ │  MetricsHooks            │
   │  DataFlow        │  │  StreamingPipeline│ │  DataQualityHooks        │
   └──────────────────┘  └───────────────────┘ │  AuditHooks              │
                                               │  CheckpointHooks         │
                                               └──────────────────────────┘
                                     │
                                     ▼
   ┌───────────────────────────────────────────────────────────────────────┐
   │                         Core Abstractions                             │
   ├───────────────────────────────────────────────────────────────────────┤
   │  PipelineComponent     │  SchemaContract       │  ConfigurableInstance│
   │  (ABC)                 │  (Protocol)           │  (Protocol)          │
   └───────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
   ┌───────────────────────────────────────────────────────────────────────┐
   │                        Core Services                                  │
   ├────────────────┬──────────────┬──────────────┬────────────────────────┤
   │  Resilience    │  Quality     │  Audit       │  Secrets               │
   │  RetryExecutor │  DQ Checks   │  AuditSink   │  SecretsResolver       │
   │  CircuitBreaker│              │  ConfigFilter│  SecretsCache          │
   └────────────────┴──────────────┴──────────────┴────────────────────────┘
                                     │
                                     ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │                     Configuration Layer                             │
   ├─────────────────────────────────────────────────────────────────────┤
   │  PipelineConfig        │  ComponentConfig    │  SparkConfig         │
   │  (HOCON / dataconf)    │  RetryConfig        │  ConfigLoader        │
   └─────────────────────────────────────────────────────────────────────┘

Batch Pipeline Flow
-------------------

A batch pipeline follows this execution path:

.. code-block:: text

   ┌───────────┐     ┌──────────────┐     ┌────────────────┐
   │  HOCON    │────→│ ConfigLoader │────→│ PipelineConfig │
   │  File     │     │              │     │                │
   └───────────┘     └──────────────┘     └───────┬────────┘
                                                  │
                                                  ▼
                                          ┌────────────────┐
                                          │ Dynamic Loader │
                                          │ (importlib)    │
                                          └───────┬────────┘
                                                  │
                                                  ▼
                     ┌─────────────────────────────────────────────┐
                     │          SimplePipelineRunner               │
                     │                                             │
                     │  1. Resolve dependency order                │
                     │  2. For each component:                     │
                     │     a. on_component_start()                 │
                     │     b. Inject SparkSession                  │
                     │     c. Execute with retry / circuit breaker │
                     │     d. on_component_end()                   │
                     │  3. on_pipeline_end()                       │
                     └─────────────────────────────────────────────┘

Streaming Pipeline Flow
-----------------------

A streaming pipeline wires a source, transform, and sink:

.. code-block:: text

   ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
   │ StreamingSource │────→│   transform()   │────→│  StreamingSink  │
   │ (Kafka, File,   │     │  (user-defined) │     │ (Delta, Console,│
   │  Delta, Rate)   │     │                 │     │  Kafka, File)   │
   └─────────────────┘     └─────────────────┘     └─────────────────┘
           │                                                │
           └──────────── Spark Structured Streaming ────────┘

Package Layout
--------------

.. code-block:: text

   src/pyspark_pipeline_framework/
   ├── core/
   │   ├── config/         # HOCON config models, loaders, presets
   │   ├── component/      # PipelineComponent ABC, protocols, exceptions
   │   ├── schema/         # DataType enum, SchemaField, SchemaDefinition
   │   ├── resilience/     # RetryExecutor, CircuitBreaker
   │   ├── quality/        # Data quality check types and implementations
   │   ├── audit/          # Audit events, sinks, config filtering
   │   └── secrets/        # SecretsProvider ABC, Env/AWS/Vault, cache
   ├── runtime/
   │   ├── session/        # SparkSessionWrapper (lifecycle management)
   │   ├── dataflow/       # DataFlow ABC, SchemaAwareDataFlow
   │   ├── streaming/      # StreamingSource, StreamingSink, pipelines
   │   └── loader.py       # Dynamic component loading (importlib)
   ├── runner/
   │   ├── hooks.py        # PipelineHooks protocol, NoOpHooks
   │   ├── hooks_builtin.py# LoggingHooks, MetricsHooks, CompositeHooks
   │   ├── simple_runner.py# SimplePipelineRunner
   │   ├── result.py       # PipelineResult, ComponentResult
   │   ├── checkpoint.py   # CheckpointState, LocalCheckpointStore
   │   ├── quality_hooks.py# DataQualityHooks
   │   └── audit_hooks.py  # AuditHooks
   └── examples/
       ├── batch.py        # ReadTable, SqlTransform, WriteTable
       └── streaming.py    # FileToConsolePipeline, KafkaToDeltaPipeline
