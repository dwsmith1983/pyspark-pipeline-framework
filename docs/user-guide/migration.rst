Scala Migration Guide
=====================

This guide helps users of the Scala
`spark-pipeline-framework <https://github.com/dwsmith1983/spark-pipeline-framework>`_
migrate to the Python version. The two frameworks share the same architecture
and concepts, but configuration field names and some API conventions differ.

HOCON Field Mapping
-------------------

The table below maps Scala HOCON field names to their Python equivalents.

Pipeline-Level Fields
^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 35 35 30

   * - Scala Field
     - Python Field
     - Notes
   * - ``pipeline-name``
     - ``name``
     - Shortened
   * - ``fail-fast``
     - *(constructor arg)*
     - Passed to ``SimplePipelineRunner(fail_fast=True)``
   * - ``schema-validation``
     - *(not in config)*
     - Schema validation is automatic when components implement ``SchemaContract``
   * - ``checkpoint``
     - *(not in config)*
     - Use ``CheckpointHooks`` at runtime (see :doc:`/user-guide/checkpoint`)
   * - ``retry-policy``
     - *(per-component)*
     - Set ``retry`` on each ``ComponentConfig`` or use ``resilience`` presets
   * - ``circuit-breaker``
     - *(per-component)*
     - Set ``circuit_breaker`` on each ``ComponentConfig``
   * - ``pipeline-components``
     - ``components``
     - Shortened

Component-Level Fields
^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 35 35 30

   * - Scala Field
     - Python Field
     - Notes
   * - ``instance-type``
     - ``class_path``
     - Fully-qualified class path (e.g. ``"my.module.MyComponent"``)
   * - ``instance-name``
     - ``name``
     - Unique identifier within the pipeline
   * - ``instance-config``
     - ``config``
     - Dict of component-specific settings
   * - ``retry-policy.max-retries``
     - ``retry.max_attempts``
     - Note: Python counts total attempts, not retries (``max_attempts = max_retries + 1``)
   * - ``retry-policy.initial-delay-ms``
     - ``retry.initial_delay_seconds``
     - Seconds instead of milliseconds
   * - ``retry-policy.max-delay-ms``
     - ``retry.max_delay_seconds``
     - Seconds instead of milliseconds
   * - ``retry-policy.backoff-multiplier``
     - ``retry.backoff_multiplier``
     - Same semantics
   * - ``retry-policy.jitter-factor``
     - *(constructor arg)*
     - ``RetryExecutor(jitter_factor=0.25)``
   * - ``retry-policy.retryable-exceptions``
     - ``retry.retry_on_exceptions``
     - Same semantics (list of class names)

Spark Configuration
^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 35 35 30

   * - Scala Field
     - Python Field
     - Notes
   * - ``spark.master``
     - ``spark.master``
     - Same
   * - ``spark.app-name``
     - ``spark.app_name``
     - Underscores instead of hyphens
   * - ``spark.config``
     - ``spark.spark_conf``
     - Additional ``spark.*`` properties

Secrets Configuration
^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 35 35 30

   * - Scala Syntax
     - Python Syntax
     - Notes
   * - ``${secret:env://VAR_NAME}``
     - ``secret://env/VAR_NAME``
     - Simpler URI scheme
   * - ``${secret:aws://secret-name}``
     - ``secret://aws/secret-name``
     - Same provider
   * - ``${secret:vault://path/to/secret#field}``
     - ``secret://vault/path/to/secret:field``
     - Colon separator instead of hash

Example: Full Config Migration
------------------------------

**Scala HOCON:**

.. code-block:: javascript

   {
     spark {
       master = "yarn"
       app-name = "Customer ETL"
       config {
         "spark.sql.shuffle.partitions" = "200"
       }
     }

     pipeline {
       pipeline-name = "customer-etl"
       fail-fast = true

       retry-policy {
         max-retries = 2
         initial-delay-ms = 1000
         max-delay-ms = 30000
         backoff-multiplier = 2.0
       }

       pipeline-components = [
         {
           instance-type = "com.example.ReadCustomers"
           instance-name = "read-customers"
           instance-config {
             table = "raw.customers"
             output-view = "raw_customers"
           }
         },
         {
           instance-type = "com.example.TransformCustomers"
           instance-name = "transform-customers"
           instance-config {
             input-view = "raw_customers"
             output-view = "clean_customers"
           }
         }
       ]
     }
   }

**Python HOCON:**

.. code-block:: javascript

   {
     name: "customer-etl"
     version: "1.0.0"

     spark {
       app_name: "Customer ETL"
       master: "yarn"
       spark_conf {
         "spark.sql.shuffle.partitions": "200"
       }
     }

     components: [
       {
         name: "read-customers"
         component_type: source
         class_path: "my_package.ReadCustomers"
         config {
           table: "raw.customers"
           output_view: "raw_customers"
         }
         retry {
           max_attempts: 3
           initial_delay_seconds: 1.0
           max_delay_seconds: 30.0
           backoff_multiplier: 2.0
         }
       },
       {
         name: "transform-customers"
         component_type: transformation
         class_path: "my_package.TransformCustomers"
         depends_on: ["read-customers"]
         config {
           input_view: "raw_customers"
           output_view: "clean_customers"
         }
       }
     ]
   }

API Migration
-------------

Component Creation
^^^^^^^^^^^^^^^^^^

**Scala:**

.. code-block:: scala

   class MyTransform extends DataFlow {
     override def name: String = "MyTransform"
     override def run(): Unit = {
       val df = spark.sql("SELECT * FROM raw")
       df.createOrReplaceTempView("transformed")
     }
   }

   object MyTransform extends ConfigurableInstance {
     override def createFromConfig(conf: Config): MyTransform = {
       // parse config
       new MyTransform(...)
     }
   }

**Python:**

.. code-block:: python

   class MyTransform(DataFlow):
       @property
       def name(self) -> str:
           return "MyTransform"

       @classmethod
       def from_config(cls, config: dict) -> "MyTransform":
           return cls(**config)

       def run(self) -> None:
           df = self.spark.sql("SELECT * FROM raw")
           df.createOrReplaceTempView("transformed")

Key differences:

- ``name`` is a property instead of a method override
- ``from_config`` is a classmethod on the class itself (no companion object)
- ``from_config`` receives a ``dict`` instead of a Typesafe ``Config`` object

Pipeline Execution
^^^^^^^^^^^^^^^^^^

**Scala:**

.. code-block:: scala

   SimplePipelineRunner.run(config, hooks = LoggingHooks)

**Python:**

.. code-block:: python

   runner = SimplePipelineRunner.from_file(
       "pipeline.conf",
       hooks=CompositeHooks(LoggingHooks()),
   )
   result = runner.run()

Key differences:

- Python runner is instance-based (Scala uses a singleton object)
- Python ``run()`` returns a ``PipelineResult``; Scala returns ``Unit``
  and throws on failure
- Python hooks are composed explicitly with ``CompositeHooks``; Scala uses
  ``PipelineHooks.compose()``

Key Conceptual Differences
--------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Concept
     - Scala
     - Python
   * - Config parsing
     - PureConfig (``loadOrThrow``)
     - dataconf (``load_from_file``)
   * - Type safety
     - Compile-time (Scala types)
     - Runtime (mypy strict + dataclass validation)
   * - Protocols
     - Traits (nominal typing)
     - ``Protocol`` (structural typing)
   * - Metrics
     - Micrometer
     - ``MeterRegistry`` protocol (pluggable)
   * - Logging
     - Log4j2
     - Python ``logging`` module
   * - Build
     - SBT + cross-compilation
     - Hatchling + pip
   * - Dependency injection
     - Constructor + companion objects
     - Constructor + ``from_config`` classmethod
   * - Error handling
     - Exceptions + ``Try``
     - Exceptions with ``__cause__`` chains

See Also
--------

- :doc:`/user-guide/configuration` -- Python configuration reference
- :doc:`/user-guide/components` -- Creating Python components
- :doc:`/user-guide/resilience` -- Retry and circuit breaker configuration
- `Scala framework docs <https://dwsmith1983.github.io/spark-pipeline-framework/>`_
