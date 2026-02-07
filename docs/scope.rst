Scope & Design
==============

This page describes the scope, design philosophy, and architectural decisions
behind pyspark-pipeline-framework. Use it to decide whether this framework is
a good fit for your project.

Design Philosophy
-----------------

pyspark-pipeline-framework is built on three principles:

**Configuration-driven.**
Pipelines are declared in HOCON configuration files. Components, ordering,
retry policies, and circuit breakers are all specified outside of application
code. The same component code runs in development, staging, and production
with different configuration files.

**Composable.**
Every pipeline is a sequence of components that conform to the
``PipelineComponent`` abstract base class. Components are loaded dynamically
from class paths, instantiated from configuration dictionaries, and executed
in dependency order. New components can be added without modifying the runner.

**Observable.**
The ``PipelineHooks`` protocol provides lifecycle callbacks at every execution
point: pipeline start/end, component start/end, retries, and failures. Built-in
hooks cover logging, metrics, data quality checks, audit trails, and checkpoint
management. Compose them freely via ``CompositeHooks``.


What This Framework IS
----------------------

Simple Sequential Pipelines
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The framework loads a HOCON configuration file, dynamically instantiates
components via ``importlib``, resolves ``depends_on`` ordering, and executes
components sequentially through ``SimplePipelineRunner``:

.. code-block:: text

   HOCON Config --> ConfigLoader --> PipelineConfig
                                         |
                                         v
                                    Dynamic Loader
                                    (importlib)
                                         |
                                         v
                                SimplePipelineRunner
                                  (sequential execution)

Each component receives a ``SparkSession``, runs its logic, and passes data
downstream via temporary views or shared state. This model is deliberately
simple: a pipeline is a list of steps that run in order.

Configuration-Driven Architecture
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All pipeline definitions use `HOCON <https://github.com/lightbend/config/blob/master/HOCON.md>`_
(Human-Optimized Config Object Notation), parsed by the ``dataconf`` library
into type-safe Python dataclasses:

.. code-block:: javascript

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
         class_path: "my_project.components.ReadCustomers"
         config {
           table_name: "raw.customers"
           output_view: "raw"
         }
       }
     ]
   }

HOCON provides features that JSON lacks: comments, includes, variable
substitution (``${?ENV_VAR}``), and multi-file composition. These features make
it straightforward to manage environment-specific overrides.

Production-Ready Observability
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The framework provides five built-in hook implementations that compose via
``CompositeHooks``:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Hook
     - Purpose
   * - ``LoggingHooks``
     - Structured logging of lifecycle events via ``structlog``
   * - ``MetricsHooks``
     - Timing, counters, and retry counts for monitoring systems
   * - ``DataQualityHooks``
     - Run data quality checks before/after components
   * - ``AuditHooks``
     - Emit audit events with config filtering for compliance
   * - ``CheckpointHooks``
     - Persist checkpoint state for pipeline resume

All hooks follow the ``PipelineHooks`` protocol, so custom implementations
(Slack notifications, PagerDuty alerts, custom dashboards) plug in without
any framework changes.

Flexible Error Handling
~~~~~~~~~~~~~~~~~~~~~~~

Components can be configured with per-component resilience policies:

- **Retry with exponential backoff** -- ``RetryExecutor`` with configurable
  ``max_attempts``, ``initial_delay_seconds``, ``max_delay_seconds``, and
  ``backoff_multiplier``.
- **Circuit breaker** -- ``CircuitBreaker`` with ``failure_threshold`` and
  ``timeout_seconds``. Prevents cascading failures by rejecting calls to
  repeatedly failing components.
- **Fail-fast vs. continue** -- The runner supports both strategies.
  Components can be marked ``enabled: false`` to skip them entirely.

.. code-block:: javascript

   {
     name: "flaky_api"
     component_type: source
     class_path: "my_project.components.ApiSource"
     retry {
       max_attempts: 3
       initial_delay_seconds: 1.0
       max_delay_seconds: 30.0
       backoff_multiplier: 2.0
     }
     circuit_breaker {
       failure_threshold: 5
       timeout_seconds: 60.0
     }
   }


What This Framework is NOT
--------------------------

Not a DAG Executor
~~~~~~~~~~~~~~~~~~

Pipelines execute components **sequentially** in dependency order. There is no
parallel task scheduling, no DAG visualization, and no fan-out/fan-in
execution. If you need complex directed acyclic graph execution with parallel
branches, use a workflow orchestrator like Airflow or Dagster and call this
framework from individual tasks.

The ``depends_on`` field controls ordering, not parallel execution:

.. code-block:: javascript

   components: [
     { name: "a", ... },
     { name: "b", depends_on: ["a"], ... },
     { name: "c", depends_on: ["a"], ... },
     { name: "d", depends_on: ["b", "c"], ... }
   ]

In this example, ``b`` and ``c`` both depend on ``a`` but they still execute
sequentially (``a`` then ``b`` then ``c`` then ``d``), not in parallel.

Optional Schema Contracts
~~~~~~~~~~~~~~~~~~~~~~~~~

Schema validation is **opt-in**. Components can implement the
``SchemaContract`` protocol and extend ``SchemaAwareDataFlow`` to declare input
and output schemas, but this is not enforced by default. The framework does not
require every component to declare schemas.

Not a Workflow Orchestrator
~~~~~~~~~~~~~~~~~~~~~~~~~~~

This framework runs **a single pipeline**. It does not manage multi-pipeline
DAGs, scheduling, alerting dashboards, or cross-pipeline dependencies. For
multi-pipeline orchestration, use Airflow, Dagster, Prefect, or a similar tool
and invoke ``SimplePipelineRunner`` from within each task.


When to Use This Framework
--------------------------

Good Fit
~~~~~~~~

- **Batch ETL jobs** -- Read from tables/files, transform with SQL or
  DataFrame logic, write to tables. The source-transform-sink pattern maps
  directly to the component model.
- **Data transformations** -- Chains of SQL or DataFrame transformations
  composed as reusable components.
- **Streaming ingestion** -- Kafka, file, or Delta Lake sources piped through
  transforms to Delta, Iceberg, or Kafka sinks using Structured Streaming.
- **Configuration-driven batch processing** -- Multiple environments (dev,
  staging, prod) with shared component code and different HOCON configs.
- **Audited data pipelines** -- Compliance requirements met with
  ``AuditHooks`` and ``ConfigFilter`` for redacting secrets from audit logs.

Not a Good Fit
~~~~~~~~~~~~~~

- **Complex DAG dependencies** -- Pipelines with parallel branches, conditional
  execution, or fan-out/fan-in patterns. Use Airflow or Dagster.
- **Non-Spark workloads** -- The framework is purpose-built for PySpark.
  ``core/`` has no Spark dependency at import time, but ``runtime/`` and
  ``runner/`` assume a ``SparkSession``.
- **Real-time serving** -- For sub-second latency APIs or event-driven
  microservices, use dedicated serving frameworks (FastAPI, Flink, etc.).
- **Ad-hoc notebook exploration** -- The framework adds structure that is
  unnecessary for one-off interactive analysis. Use plain PySpark in notebooks.


Architecture Decisions
----------------------

Why Sequential Execution?
~~~~~~~~~~~~~~~~~~~~~~~~~

Most PySpark ETL jobs are inherently sequential: read data, transform it,
write the result. Parallel execution within a single Spark application adds
complexity (thread safety, resource contention, debugging difficulty) with
limited benefit because Spark itself parallelizes work across executors.

Sequential execution provides:

- **Predictable ordering** -- Components always run in the same order.
- **Simple debugging** -- Stack traces point to a single component.
- **Safe Spark usage** -- No concurrent ``SparkSession`` access concerns.
- **Checkpoint/resume** -- Failed pipelines resume from the last completed step.

Why HOCON Configuration?
~~~~~~~~~~~~~~~~~~~~~~~~

HOCON was chosen over YAML, TOML, or JSON because:

- **Comments** -- HOCON supports ``//`` and ``#`` comments. YAML does too, but
  JSON and TOML (for complex nested structures) do not.
- **Includes** -- ``include "base.conf"`` enables environment-specific
  overrides layered on shared defaults.
- **Substitution** -- ``${?DB_PASSWORD}`` reads environment variables with
  optional fallback. Avoids separate templating tools.
- **Compatibility** -- HOCON is a superset of JSON. Existing JSON configs work
  without changes.
- **Consistency** -- The Scala ``spark-pipeline-framework`` uses Typesafe
  Config (HOCON). Using the same format enables teams to share configuration
  patterns across Scala and Python projects.

The ``dataconf`` library parses HOCON into Python dataclasses with type
validation, providing the same type safety that PureConfig provides in Scala.

Why Reflection-Based Instantiation?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Components are loaded dynamically via ``importlib.import_module`` and
``getattr``:

.. code-block:: python

   from pyspark_pipeline_framework.runtime.loader import load_component_class

   cls = load_component_class("my_project.components.ReadCustomers")
   instance = cls.from_config({"table_name": "raw.customers"})

This design provides:

- **No framework coupling** -- Components do not need to register themselves.
  Any class with ``from_config()`` and ``run()`` can be loaded.
- **Configuration-driven composition** -- New components are added to the
  pipeline by editing a HOCON file, not by modifying Python code.
- **Validation** -- ``validate_pipeline()`` dry-runs class loading and
  configuration parsing without executing any Spark operations.


Comparison with Alternatives
----------------------------

.. list-table::
   :header-rows: 1
   :widths: 20 16 16 16 16 16

   * - Feature
     - **This Framework**
     - Raw PySpark
     - Airflow
     - Dagster
     - Custom Framework
   * - Config-driven pipelines
     - HOCON + dataconf
     - Manual
     - Airflow Variables
     - Dagster config
     - Varies
   * - Component reuse
     - ``DataFlow`` ABC
     - Copy/paste
     - Operators
     - Assets/Ops
     - Varies
   * - Dynamic loading
     - ``importlib`` + class path
     - N/A
     - Plugin system
     - Resource system
     - Often manual
   * - Execution model
     - Sequential
     - Sequential
     - DAG (parallel)
     - DAG (parallel)
     - Varies
   * - Retry / circuit breaker
     - Built-in, per-component
     - Manual
     - Task-level retry
     - Op-level retry
     - Often missing
   * - Lifecycle hooks
     - ``PipelineHooks`` protocol
     - None
     - Callbacks
     - Sensors/hooks
     - Varies
   * - Data quality
     - ``DataQualityHooks``
     - Manual / Great Expectations
     - Separate DAG
     - Asset checks
     - Varies
   * - Audit trail
     - ``AuditHooks`` + sinks
     - Manual
     - Audit logs
     - Event log
     - Varies
   * - Secrets management
     - ``SecretsResolver`` + providers
     - Manual
     - Connections
     - Resources
     - Varies
   * - Checkpoint / resume
     - ``CheckpointStore``
     - Manual
     - Built-in
     - Built-in
     - Often missing
   * - Streaming support
     - ``StreamingPipeline``
     - Manual
     - Limited
     - Limited
     - Varies
   * - Learning curve
     - Low
     - Lowest
     - Medium
     - Medium-High
     - Varies
   * - Deployment
     - ``spark-submit`` / Databricks
     - Same
     - Managed scheduler
     - Managed scheduler
     - Varies

**Summary:** Use this framework when you want configuration-driven PySpark
pipelines with production features (resilience, observability, audit) but
without the operational overhead of a full workflow orchestrator. Use Airflow or
Dagster when you need DAG scheduling, cross-pipeline dependencies, or a
managed execution environment.


See Also
--------

- :doc:`/getting-started` -- Installation and quick example
- :doc:`/architecture` -- Component architecture diagrams
- :doc:`/user-guide/components` -- Creating pipeline components
